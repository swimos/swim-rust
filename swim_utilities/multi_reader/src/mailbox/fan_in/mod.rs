// Copyright 2015-2021 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A fan-in MPSC read selector for byte channels.
//!
//! # Overview:
//! The channel is comprised of three parts: a registrar that produces RawChannelSender instances if
//! the channel is still open, RawChannelSender for sending bytes to a receiver that reads them out.
//! If a sender requests to send bytes then it is placed into a queue that is popped by the
//! receiver. The receiver pops tasks from a queue and grants a sender exclusive access to an
//! inner `Bytes` instance and the data is transferred across like in the `byte_channel` structure.
//! Once the sender has finished writing its data it marks itself as complete and the receiver pops
//! the next task from the queue.
//!
//! # Improvements/to dos:
//! - Internally, the producers and consumers are backed by an unbounded atomic wait queue that
//! could possibly be improved upon and allocated upfront. Once an entry is unused then it could be
//! marked as stale and just reset the item and register a new waker. I have experimented with
//! different data structures to replace the queue (Arc<Mutex<Deque>>, Arc<Mutex<LinkedList>>,
//! ConcurrentQueue) but the queue performed the best under high contention.
//! - There might be a race condition on the WriteTask's usage between 'wake' and 'register' calls.
//! It may be better to take the registered waker from the task, set a new one then register the
//! one that was taken. Is there something else that can be used instead of an AtomicWaker too? As
//! it doesn't provider `Waker::will_wake` to check if the registered waker needs to be replaced.
//! - The WriteTask's state can be condensed to a single AtomicU8.
//! - Shared `capacity` variable needs to be moved to the `RawChannelSender`.  
//! - `RawChannelSender`'s future implementation should be pulled out to a function so that it can
//! implement `AsyncWrite` for usage with encoders.
//! - `RawChannelRegistrar`/`RawChannelSender` Drop not implemented. Once every sender **and** the
//! registrar have been dropped then the channel should be marked as closed.

#[cfg(test)]
mod tests;

mod send;

use crate::mailbox::core::{queue, Node, QueueConsumer, QueueProducer, QueueResult, WriteTask};
use bytes::{Buf, BytesMut};
use futures::future::Shared;
use futures::task::AtomicWaker;
use parking_lot::Mutex;
use parking_lot_core::SpinWait;
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

pub fn mailbox_channel(capacity: NonZeroUsize) -> (RawChannelRegistrar, ChannelReceiver) {
    let (queue_tx, queue_rx) = queue();
    let shared = Arc::new(SharedInner::new(capacity, queue_tx));
    let registrar = RawChannelRegistrar {
        shared: shared.clone(),
    };
    let receiver = ChannelReceiver {
        shared,
        queue: queue_rx,
    };
    (registrar, receiver)
}

pub struct RawChannelRegistrar {
    shared: Arc<SharedInner>,
}

impl RawChannelRegistrar {
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

        let mut backoff = SpinWait::new();

        loop {
            match queue.head_ref() {
                QueueResult::Data(task) => {
                    if task.is_pending() {
                        task.wake();
                        task.register(cx.waker());

                        break Poll::Pending;
                    } else {
                        backoff.reset();
                        let data = &mut *mailbox.lock();
                        break write(task, data, read_buf, queue, cx, backoff);
                    }
                }
                QueueResult::Inconsistent => {
                    backoff.spin();
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
    mut backoff: SpinWait,
) -> Poll<std::io::Result<()>> {
    let remaining = into.remaining();

    if from.has_remaining() && remaining > 0 {
        let count = from.remaining().min(remaining);

        into.put_slice(&from[..count]);
        from.advance(count);

        if task.is_complete() {
            loop {
                match queue.pop() {
                    QueueResult::Inconsistent => {
                        backoff.spin();
                    }
                    _ => break,
                }
            }

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
