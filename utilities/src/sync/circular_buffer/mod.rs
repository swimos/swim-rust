// Copyright 2015-2020 SWIM.AI inc.
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

use crossbeam_queue::{ArrayQueue, SegQueue};
use futures::task::{AtomicWaker, Context, Poll};
use futures::Stream;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Send end of a circular buffer channel. If the receive end is dropped it the sender will
/// reject any additional attempts to send values.
#[derive(Debug)]
pub struct Sender<T>(Arc<Inner<T>>);

/// Receive end of a circular buffer channel. If the send end is dropped, the receive end will
/// receive all data remaining in the buffer and will then terminate.
#[derive(Debug)]
pub struct Receiver<T>(Arc<Inner<T>>);

const LARGE_BOUNDARY: usize = 32;

/// A single producer, single consumer circular buffer channel. The receiver will receiver all records
/// pushed into the channel until the buffer fills, after which point records will be dropped in
/// order of age. The representation of the buffer varies with its capacity. For a capacity of 1
/// the buffer is a single value, guarded by a lock. For 'small' buffers (up to `LARGE_BOUNDARY`
/// entries) the representation is a fixed size, pre-allocated array. For larger buffers the
/// representation will grow, as required, as entries are pushed into it.
pub fn channel<T: Send + Sync>(capacity: NonZeroUsize) -> (Sender<T>, Receiver<T>) {
    let queue = match capacity.get() {
        1 => InnerQueue::One(QueueChannel::new(OneItemQueue::new(), 1)),
        n if n < LARGE_BOUNDARY => InnerQueue::Small(QueueChannel::new(ArrayQueue::new(n), n)),
        n => InnerQueue::Large(QueueChannel::new(SegQueue::new(), n)),
    };
    let inner = Arc::new(Inner {
        queue,
        sender_active: AtomicBool::new(true),
    });
    (Sender(inner.clone()), Receiver(inner))
}

pub fn watch_channel<T: Send + Sync>() -> (Sender<T>, Receiver<T>) {
    channel(NonZeroUsize::new(1).unwrap())
}

pub mod error {

    /// Error type returning the pushed value if the receive end of the channel is dropped.
    #[derive(Debug, PartialEq, Eq)]
    pub struct SendError<T>(pub T);

    /// Error indicating that the send end of the channel was dropped and no further values will
    /// be received.
    #[derive(Debug, Default, PartialEq, Eq)]
    pub struct RecvError;
}

#[derive(Debug)]
struct QueueChannel<Q> {
    queue: Q,
    capacity: usize,
    permits: AtomicUsize,
    waker: AtomicWaker,
}

impl<Q> QueueChannel<Q> {
    fn new(queue: Q, capacity: usize) -> Self {
        QueueChannel {
            queue,
            capacity,
            permits: AtomicUsize::new(capacity),
            waker: AtomicWaker::new(),
        }
    }
}

#[derive(Debug)]
// `Small` is disproportionately large, however it is the typical case so it is undesirable to box it.
#[allow(clippy::large_enum_variant)]
enum InnerQueue<T> {
    One(QueueChannel<OneItemQueue<T>>),
    Small(QueueChannel<ArrayQueue<T>>),
    Large(QueueChannel<SegQueue<T>>),
}

impl<T> InnerQueue<T> {
    fn waker(&self) -> &AtomicWaker {
        match self {
            InnerQueue::One(chan_queue) => &chan_queue.waker,
            InnerQueue::Small(chan_queue) => &chan_queue.waker,
            InnerQueue::Large(chan_queue) => &chan_queue.waker,
        }
    }
}

#[derive(Debug)]
struct Inner<T> {
    queue: InnerQueue<T>,
    sender_active: AtomicBool,
}

fn poll_consume<T: Send + Sync>(arc_inner: &Arc<Inner<T>>, cx: &mut Context) -> Poll<Option<T>> {
    let Inner {
        queue,
        sender_active,
    } = &**arc_inner;
    match queue {
        InnerQueue::One(chan_queue) => poll_consume_queue(sender_active, chan_queue, cx),
        InnerQueue::Small(chan_queue) => poll_consume_queue(sender_active, chan_queue, cx),
        InnerQueue::Large(chan_queue) => poll_consume_queue(sender_active, chan_queue, cx),
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Sender(inner) = self;
        inner.sender_active.store(false, Ordering::Release);
        inner.queue.waker().wake();
    }
}

impl<T: Send + Sync> Sender<T> {
    /// Attempt to send a value into the channel (this will fail if the receiver has been dropped).
    pub fn try_send(&mut self, value: T) -> Result<(), error::SendError<T>> {
        let Sender(inner) = self;
        if Arc::strong_count(inner) < 2 {
            Err(error::SendError(value))
        } else {
            match &inner.queue {
                InnerQueue::One(chan_queue) => send(chan_queue, value),
                InnerQueue::Small(chan_queue) => send(chan_queue, value),
                InnerQueue::Large(chan_queue) => send(chan_queue, value),
            }
            Ok(())
        }
    }
}

fn send<T, Q>(chan_queue: &QueueChannel<Q>, value: T)
where
    T: Send + Sync,
    Q: InternalQueue<T>,
{
    let QueueChannel {
        queue,
        capacity,
        permits,
        waker,
    } = chan_queue;
    loop {
        let available = permits.load(Ordering::Relaxed);
        if available > 0 {
            if permits.compare_and_swap(available, available - 1, Ordering::Acquire) == available {
                queue.push_value(value).ok().expect("Inconsistent queue.");
                if available == *capacity {
                    waker.wake();
                }
                break;
            }
        } else {
            // The popped value must remain in scope until we have released the permit in case
            // it has a destructor that panics.
            let top = queue.pop_value();
            if top.is_some() {
                permits.fetch_add(1, Ordering::Release);
            }
        }
    }
}

#[derive(Debug)]
pub struct Recv<'a, T>(&'a mut Receiver<T>);

fn poll_consume_queue<T, Q>(
    sender_active: &AtomicBool,
    queue: &QueueChannel<Q>,
    cx: &mut Context<'_>,
) -> Poll<Option<T>>
where
    T: Send + Sync,
    Q: InternalQueue<T>,
{
    let QueueChannel {
        queue,
        capacity,
        permits,
        waker,
        ..
    } = queue;
    loop {
        if let Some(value) = queue.pop_value() {
            permits.fetch_add(1, Ordering::Release);
            return Poll::Ready(Some(value));
        } else {
            let available = permits.load(Ordering::Relaxed);
            if available == *capacity {
                if sender_active.load(Ordering::Acquire) {
                    waker.register(cx.waker());
                    let check = permits.load(Ordering::Acquire);
                    if check == *capacity {
                        return Poll::Pending;
                    }
                } else {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl<'a, T: Send + Sync> Future for Recv<'a, T> {
    type Output = Result<T, error::RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Receiver(inner) = &self.0;
        poll_consume(inner, cx).map(|maybe| maybe.ok_or(error::RecvError))
    }
}

impl<T: Send + Sync> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        poll_consume(&self.0, cx)
    }
}

impl<T: Send + Sync> Receiver<T> {
    /// Wait for a value to be available in the channel and receive it. This will will complete with
    /// an error if the sender is dropped and the buffer has been exhausted.
    pub fn recv(&mut self) -> Recv<T> {
        Recv(self)
    }
}

trait InternalQueue<T> {
    fn push_value(&self, value: T) -> Result<(), T>;

    fn pop_value(&self) -> Option<T>;
}

impl<T: Send + Sync> InternalQueue<T> for SegQueue<T> {
    fn push_value(&self, value: T) -> Result<(), T> {
        self.push(value);
        Ok(())
    }

    fn pop_value(&self) -> Option<T> {
        self.pop()
    }
}

impl<T: Send + Sync> InternalQueue<T> for ArrayQueue<T> {
    fn push_value(&self, value: T) -> Result<(), T> {
        self.push(value)
    }

    fn pop_value(&self) -> Option<T> {
        self.pop()
    }
}

#[derive(Debug)]
struct OneItemQueue<T>(parking_lot::Mutex<Option<T>>);

impl<T> OneItemQueue<T> {
    fn new() -> Self {
        OneItemQueue(parking_lot::Mutex::new(None))
    }
}

impl<T: Send + Sync> InternalQueue<T> for OneItemQueue<T> {
    fn push_value(&self, value: T) -> Result<(), T> {
        let mut lock = self.0.lock();
        if lock.is_some() {
            Err(value)
        } else {
            *lock = Some(value);
            Ok(())
        }
    }

    fn pop_value(&self) -> Option<T> {
        let mut lock = self.0.lock();
        lock.take()
    }
}
