// mod read;
mod send;
#[cfg(test)]
mod tests;

use crate::mailbox::core::{Node, Queue, WriteTask};
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
    let shared = Arc::new(SharedInner::new(capacity));
    let registrar = ChannelRegistrar {
        shared: shared.clone(),
    };
    let receiver = ChannelReceiver { shared };
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
    current: UnsafeCell<Option<WriteTask>>,
    write_queue: Queue<WriteTask>,
    receiver: AtomicWaker,
    capacity: usize,
    mailbox: Mutex<BytesMut>,
    closed: AtomicBool,
}

unsafe impl Send for SharedInner {}
unsafe impl Sync for SharedInner {}

impl SharedInner {
    fn new(capacity: NonZeroUsize) -> SharedInner {
        let empty = Node::new(None);

        SharedInner {
            capacity: capacity.get(),
            mailbox: Mutex::new(BytesMut::default()),
            current: UnsafeCell::default(),
            write_queue: Queue::new(AtomicPtr::new(empty), UnsafeCell::new(empty)),
            receiver: AtomicWaker::default(),
            closed: AtomicBool::new(false),
        }
    }
}

pub struct ChannelReceiver {
    shared: Arc<SharedInner>,
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
        let SharedInner {
            mailbox,
            current,
            write_queue,
            receiver,
            closed,
            ..
        } = &self.shared.as_ref();

        receiver.register(cx.waker());

        match unsafe { &*current.get() } {
            Some(task) => {
                let data = &mut *mailbox.lock();
                let remaining = read_buf.remaining();

                if data.has_remaining() && remaining > 0 {
                    let count = data.remaining().min(remaining);

                    read_buf.put_slice(&data[..count]);
                    data.advance(count);

                    if task.is_complete() {
                        unsafe {
                            // this is safe as the writer no longer has a reference to the buffer
                            *current.get() = None;
                        }
                    }

                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            None => match write_queue.pop() {
                Some(mut task) => {
                    task.wake();
                    task.register(cx.waker());

                    unsafe {
                        *current.get() = Some(task);
                    }

                    Poll::Pending
                }
                None => {
                    // If it is closed, then no more writers will be inserted and we want to drain
                    // the queue of all of its data before returning that it is closed.
                    if closed.load(Ordering::Relaxed) {
                        return Poll::Ready(Err(ErrorKind::BrokenPipe.into()));
                    } else {
                        Poll::Pending
                    }
                }
            },
        }
    }
}
