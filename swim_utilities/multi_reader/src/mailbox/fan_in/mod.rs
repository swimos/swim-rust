// mod read;
mod send;
#[cfg(test)]
mod tests;

use crate::mailbox::core::{queue, Node, QueueConsumer, QueueProducer, QueueResult, WriteTask};
use bytes::{Buf, BytesMut};
use futures::future::Shared;
use futures::task::AtomicWaker;
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

pub fn mailbox_channel(capacity: NonZeroUsize) -> (ChannelRegistrar, ChannelReceiver) {
    let (queue_tx, queue_rx) = queue();
    let shared = Arc::new(SharedInner::new(capacity, queue_tx));
    let registrar = ChannelRegistrar {
        shared: shared.clone(),
    };
    let receiver = ChannelReceiver {
        shared,
        queue: queue_rx,
    };
    (registrar, receiver)
}

pub struct ChannelRegistrar {
    shared: Arc<SharedInner>,
}

impl ChannelRegistrar {
    pub fn register(&self) -> Result<RawChannelSender, ()> {
        if self.shared.closed.load(Ordering::Relaxed) {
            return Err((()));
        }

        Ok(RawChannelSender {
            shared: self.shared.clone(),
        })
    }
}

pub struct RawChannelSender {
    shared: Arc<SharedInner>,
}

impl RawChannelSender {
    // collides with the Send trait
    pub fn send<'r, I>(&self, item: I) -> send::Send<'_, I>
    where
        I: AsRef<[u8]>,
    {
        send::Send::new(self, item)
    }
}

struct SharedInner {
    write_queue: QueueProducer<WriteTask>,
    receiver: AtomicWaker,
    capacity: usize,
    mailbox: Mutex<BytesMut>,
    closed: AtomicBool,
}

unsafe impl Send for SharedInner {}
unsafe impl Sync for SharedInner {}

impl SharedInner {
    fn new(capacity: NonZeroUsize, write_queue: QueueProducer<WriteTask>) -> SharedInner {
        SharedInner {
            capacity: capacity.get(),
            mailbox: Mutex::new(BytesMut::default()),
            write_queue,
            receiver: AtomicWaker::default(),
            closed: AtomicBool::new(false),
        }
    }
}

pub struct ChannelReceiver {
    shared: Arc<SharedInner>,
    queue: QueueConsumer<WriteTask>,
}

impl Drop for ChannelReceiver {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Relaxed);
    }
}

impl AsyncRead for ChannelReceiver {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        read_buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let queue = &self.queue;
        let SharedInner {
            mailbox,
            receiver,
            closed,
            ..
        } = &self.shared.as_ref();
        receiver.register(cx.waker());

        loop {
            match queue.head_ref() {
                QueueResult::Data(task) => {
                    if task.is_pending() {
                        task.wake();
                        task.register(cx.waker());

                        break Poll::Pending;
                    } else {
                        let data = &mut *mailbox.lock();
                        break write(task, data, read_buf, queue, cx);
                    }
                }
                QueueResult::Inconsistent => {
                    std::thread::yield_now();
                }
                QueueResult::Empty => {
                    // If it is closed, then no more writers will be inserted and we want to drain
                    // the queue of all of its data before returning that it is closed.
                    if closed.load(Ordering::Relaxed) {
                        break Poll::Ready(Err(ErrorKind::BrokenPipe.into()));
                    } else {
                        break Poll::Pending;
                    }
                }
            }
        }
    }
}

#[inline]
fn write(
    task: &WriteTask,
    from: &mut BytesMut,
    into: &mut ReadBuf<'_>,
    queue: &QueueConsumer<WriteTask>,
    cx: &mut Context<'_>,
) -> Poll<std::io::Result<()>> {
    let remaining = into.remaining();

    if from.has_remaining() && remaining > 0 {
        let count = from.remaining().min(remaining);

        into.put_slice(&from[..count]);
        from.advance(count);

        if task.is_complete() {
            queue.pop();

            if queue.has_next() {
                // Entries inserted into the queue won't wake us up if there is another
                // entry in the queue. Instead, we register ourselves to be woken up
                // once we know we are done with the head and that there is another
                // entry available.
                cx.waker().wake_by_ref();
            }
        }

        Poll::Ready(Ok(()))
    } else {
        Poll::Pending
    }
}
