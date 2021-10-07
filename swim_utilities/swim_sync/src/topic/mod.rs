// Copyright 2015-2021 SWIM.AI inc.
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

#[cfg(test)]
mod tests;

use std::cell::UnsafeCell;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::task::AtomicWaker;
use futures::Stream;
use pin_project::pin_project;
use std::num::NonZeroUsize;

use swim_future::item_sink::ItemSink;

use crate::waiters::ReadWaiters;

/// Create a single producer, multiple observer channel. The channel consists of a circular buffer
/// into which each of the entries is written. All observers will observe a reference to an entry
/// in the buffer for each call to [`Receiver::recv`] or [`Receiver::try_recv`] and a slot will only
/// be released to be written again after all observers have observed it.
///
/// # Arguments
/// * `n` - The capacity of the internal buffer.
pub fn channel<T>(n: NonZeroUsize) -> (Sender<T>, Receiver<T>) {
    let n = n.get();
    let inner = Arc::new(Inner::new(n));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver {
            inner,
            read_offset: n,
        },
    )
}

/// The send end of the channel.
#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// There are no longer any active receivers.
    NoReceivers(T),
    /// There is no free capacity in the buffer.
    NoCapacity(T),
}

/// Indicates that there are no longer any active receivers.
#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

pub enum TryRecvError {
    /// The send end of the channel has been dropped.
    Closed,
    /// No new values are available in the buffer.
    NoValue,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SubscribeError;

impl<T> Sender<T> {
    /// Send a value into the channel, waiting for free capacity if the buffer is full.
    pub fn send(&mut self, value: T) -> TopicSend<T> {
        let Sender { inner } = self;
        TopicSend {
            inner: &*inner,
            value: Some(value),
            fail_on_no_rx: true,
        }
    }

    /// Send a value into the channel. If there are no receivers registered the value is discarded.
    pub fn discarding_send(&mut self, value: T) -> TopicSend<T> {
        let Sender { inner } = self;
        TopicSend {
            inner: &*inner,
            value: Some(value),
            fail_on_no_rx: false,
        }
    }

    /// Attempt to send a a value into the channel, returning an error immediately if the buffer
    /// is full.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        //&mut self is required for the safety guarantee below.
        let Inner {
            capacity,
            entries,
            guarded,
            read_floor,
            read_waiters,
            ..
        } = &*self.inner;
        let read = read_floor.load(Ordering::Acquire);
        let lock = guarded.read();
        if lock.num_rx == 0 {
            return Err(TrySendError::NoReceivers(value));
        }
        let target = lock.write_offset.load(Ordering::Acquire);
        if target == read {
            Err(TrySendError::NoCapacity(value))
        } else {
            let next = next_slot(target, *capacity);
            lock.write_offset.store(next, Ordering::Release);
            let expected_readers = lock.num_rx;
            let entry = &entries[target];
            entry.pending.store(expected_readers, Ordering::Relaxed);
            // Safety: We know we are between the write offset and a previously stored version
            // of the reader floor. The reader floor cannot advance past the current write offset
            // and there is only one writer so we can guarantee that we have exclusive access
            // to this entry.
            let data_ref = unsafe { &mut *entry.data.get() };
            let _prev = std::mem::replace(data_ref, Some(value));
            drop(lock);
            read_waiters.lock().wake_all();
            Ok(())
        }
    }

    pub fn subscriber(&self) -> Subscriber<T> {
        self.inner.subscriber()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.active.store(false, Ordering::Release);
        self.inner.writer_waiter.take();
        self.inner.read_waiters.lock().wake_all();
    }
}

/// Equivalent to a [`Receiver`] that does not observe the values in the buffer. It can only be
/// used to create new [`Receiver`]s.
#[derive(Debug)]
pub struct Subscriber<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for Subscriber<T> {
    fn clone(&self) -> Self {
        Subscriber {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Subscriber<T> {
    /// Attempt to create a new [`Receiver`]. This will fail if the sender has been dropped.
    pub fn subscribe(&self) -> Result<Receiver<T>, SubscribeError> {
        let Subscriber { inner, .. } = self;
        let mut lock = inner.guarded.write();
        if !inner.active.load(Ordering::Relaxed) {
            Err(SubscribeError)
        } else {
            Ok(add_receiver(inner, &mut *lock))
        }
    }
}

/// The receiver end of the channel
#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    read_offset: usize,
}

impl<T> Receiver<T> {
    /// Observe the next value from the channel, waiting until one becomes available.
    pub fn recv(&mut self) -> TopicReceive<T> {
        TopicReceive {
            receiver: self,
            slot_and_epoch: None,
        }
    }

    /// Attempt to observe the next value from the channel, returning an error immediately if none
    /// are available.
    pub fn try_recv(&mut self) -> Result<EntryGuard<T>, TryRecvError> {
        try_advance_reader(&mut &mut *self, None)
    }

    /// Create a [`Subscriber`] from this [`Receiver] that can be used to create more receivers
    /// without being required to observe new values in the channel.
    pub fn subscriber(&self) -> Subscriber<T> {
        self.inner.subscriber()
    }
}

impl<T: Clone> Receiver<T> {
    /// Convert the receiver into a stream where the observed values can be cloned.
    pub fn into_stream(self) -> ReceiverStream<T>
    where
        T: Clone,
    {
        ReceiverStream {
            receiver: self,
            slot_and_epoch: None,
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let Receiver { inner, .. } = self;
        let mut lock = inner.guarded.write();
        add_receiver(inner, &mut *lock)
    }
}

#[derive(Debug)]
struct Entry<T> {
    data: UnsafeCell<Option<T>>,
    pending: AtomicUsize,
}

impl<T> Default for Entry<T> {
    fn default() -> Self {
        Entry {
            data: UnsafeCell::new(None),
            pending: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug)]
struct Guarded {
    write_offset: AtomicUsize,
    num_rx: usize,
}

#[derive(Debug)]
struct Inner<T> {
    capacity: usize,
    entries: Box<[Entry<T>]>,
    guarded: parking_lot::RwLock<Guarded>,
    read_floor: AtomicUsize,
    active: AtomicBool,
    read_waiters: parking_lot::Mutex<ReadWaiters>,
    writer_waiter: AtomicWaker,
    num_sub: AtomicUsize,
}

impl<T> Inner<T> {
    fn new(n: usize) -> Self {
        Inner {
            capacity: n,
            entries: (0..(n + 1)).map(|_| Entry::default()).collect(),
            guarded: parking_lot::RwLock::new(Guarded {
                write_offset: AtomicUsize::new(0),
                num_rx: 1,
            }),
            read_floor: AtomicUsize::new(n),
            active: AtomicBool::new(true),
            read_waiters: Default::default(),
            writer_waiter: Default::default(),
            num_sub: AtomicUsize::new(1),
        }
    }

    fn subscriber(self: &Arc<Self>) -> Subscriber<T> {
        self.num_sub.fetch_add(1, Ordering::Release);
        Subscriber {
            inner: self.clone(),
        }
    }
}

fn add_receiver<T>(inner: &Arc<Inner<T>>, lock: &mut Guarded) -> Receiver<T> {
    let new_inner = inner.clone();
    let offset = prev_slot(lock.write_offset.load(Ordering::Acquire), inner.capacity);
    lock.num_rx = lock.num_rx.checked_add(1).expect("Receiver overflow.");
    inner.num_sub.fetch_add(1, Ordering::Release);
    // The write offset cannot advance while we hold a write lock on the guarded section of
    // the internal state. This means that it is impossible for an entry to be added that
    // expects to be read by the new receiver. Therefore it is consistent to use the write
    // offset that we just read as the starting offset of the new receiver.
    Receiver {
        inner: new_inner,
        read_offset: offset,
    }
}

unsafe impl<T: Send + Sync> Send for Inner<T> {}
unsafe impl<T: Send + Sync> Sync for Inner<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct EntryGuard<'a, T> {
    value: &'a T,
}

impl<'a, T> Deref for EntryGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Receiver { inner, read_offset } = self;
        let Inner {
            capacity,
            entries,
            guarded,
            read_floor,
            writer_waiter,
            num_sub,
            ..
        } = &**inner;
        let mut lock = guarded.write();
        lock.num_rx -= 1;
        num_sub.fetch_sub(1, Ordering::Release);
        let write_offset = lock.write_offset.load(Ordering::Relaxed);
        let mut i = next_slot(*read_offset, *capacity);
        let mut new_floor = None;
        // We must decrement the counter on all entries that were expecting to be read by this
        // receiver. As we hold an exclusive lock on the write offset, no new entries can be
        // added while this is done.
        while i != write_offset {
            let entry = &entries[i];
            let prev = entry.pending.fetch_sub(1, Ordering::Release);
            if prev == 1 {
                new_floor = Some(i);
            }
            i = next_slot(i, *capacity);
        }
        if let Some(floor) = new_floor {
            read_floor.store(floor, Ordering::Release);
            drop(lock);
            writer_waiter.wake();
        }
    }
}

impl<T> Drop for Subscriber<T> {
    fn drop(&mut self) {
        let Subscriber { inner } = self;
        inner.num_sub.fetch_sub(1, Ordering::Release);
    }
}

#[pin_project]
pub struct TopicSend<'a, T> {
    #[pin]
    inner: &'a Inner<T>,
    value: Option<T>,
    fail_on_no_rx: bool,
}

pub struct TopicReceive<'a, T> {
    receiver: &'a mut Receiver<T>,
    slot_and_epoch: Option<(usize, u64)>,
}

impl<'a, T> Drop for TopicReceive<'a, T> {
    fn drop(&mut self) {
        if let Some((slot, epoch)) = self.slot_and_epoch.take() {
            self.receiver.inner.read_waiters.lock().remove(slot, epoch)
        }
    }
}

fn next_slot(i: usize, capacity: usize) -> usize {
    (i + 1) % (capacity + 1)
}

fn prev_slot(i: usize, capacity: usize) -> usize {
    (i + capacity) % (capacity + 1)
}

impl<'a, T> Future for TopicSend<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if this.inner.num_sub.load(Ordering::Acquire) == 0 {
            return Poll::Ready(Err(SendError(
                this.value.take().expect("Send future polled twice."),
            )));
        }
        let read = this.inner.read_floor.load(Ordering::Acquire);
        let lock = this.inner.guarded.read();
        if lock.num_rx == 0 {
            let v = this.value.take().expect("Send future polled twice.");
            return if *this.fail_on_no_rx {
                Poll::Ready(Err(SendError(v)))
            } else {
                Poll::Ready(Ok(()))
            };
        }
        let target = lock.write_offset.load(Ordering::Acquire);
        if target == read {
            this.inner.writer_waiter.register(cx.waker());
            let new_read = this.inner.read_floor.load(Ordering::Acquire);
            if target == new_read {
                return Poll::Pending;
            }
        }
        let next = next_slot(target, this.inner.capacity);
        let expected_readers = lock.num_rx;
        let entry = &this.inner.entries[target];
        entry.pending.store(expected_readers, Ordering::Relaxed);
        // Safety: We know we are between the write offset and a previously stored version
        // of the reader floor. The reader floor cannot advance past the current write offset
        // and there is only one writer so we can guarantee that we have exclusive access
        // to this entry.
        let data_ref = unsafe { &mut *entry.data.get() };
        let _prev = std::mem::replace(
            data_ref,
            Some(this.value.take().expect("Send future polled twice.")),
        );
        lock.write_offset.store(next, Ordering::Release);
        drop(lock);
        this.inner.read_waiters.lock().wake_all();
        Poll::Ready(Ok(()))
    }
}

struct FutureContext<'b, 'c> {
    slot_and_epoch: &'b mut Option<(usize, u64)>,
    cx: &'b mut Context<'c>,
}

fn try_advance_reader<'a, 'b, T>(
    receiver: &'b mut &'a mut Receiver<T>,
    context: Option<FutureContext<'b, '_>>,
) -> Result<EntryGuard<'a, T>, TryRecvError> {
    let Receiver { inner, read_offset } = *receiver;
    let Inner {
        capacity,
        entries,
        guarded,
        read_floor,
        active,
        read_waiters,
        writer_waiter,
        ..
    } = &**inner;
    let lock = guarded.read();
    let target = next_slot(*read_offset, *capacity);
    let next_write = lock.write_offset.load(Ordering::Acquire);
    if target == next_write {
        if !active.load(Ordering::Acquire) {
            return Err(TryRecvError::Closed);
        } else if let Some(FutureContext { slot_and_epoch, cx }) = context {
            let mut waiters = read_waiters.lock();
            *slot_and_epoch = Some(if let Some((slot, epoch)) = slot_and_epoch.take() {
                waiters.update(cx.waker(), slot, epoch)
            } else {
                waiters.insert(cx.waker().clone())
            });
            let fresh_next_write = lock.write_offset.load(Ordering::Acquire);
            if target == fresh_next_write {
                return Err(TryRecvError::NoValue);
            }
        } else {
            return Err(TryRecvError::NoValue);
        }
    }
    drop(lock);
    *read_offset = target;
    let entry = &entries[target];
    let prev = entry.pending.fetch_sub(1, Ordering::Release);
    debug_assert!(prev != 0);
    if prev == 1 {
        read_floor.store(target, Ordering::Release);
        writer_waiter.wake();
    }
    // Safety: We know we are between the read floor and a previously stored version
    // of the write offset. The write offset cannot advance past the current read floor
    // and only the unique writer can modify entries so it is safe to read this entry.
    let value = unsafe { &*entry.data.get() }
        .as_ref()
        .expect("Inconsistent entries.");
    Ok(EntryGuard { value })
}

impl<'a, T> Future for TopicReceive<'a, T> {
    type Output = Option<EntryGuard<'a, T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let TopicReceive {
            receiver,
            slot_and_epoch,
        } = self.get_mut();
        match try_advance_reader(receiver, Some(FutureContext { slot_and_epoch, cx })) {
            Ok(value) => Poll::Ready(Some(value)),
            Err(TryRecvError::Closed) => Poll::Ready(None),
            _ => Poll::Pending,
        }
    }
}

/// Wraps a [`Receiver`] as a stream that will clone the values it observes.
pub struct ReceiverStream<T> {
    receiver: Receiver<T>,
    slot_and_epoch: Option<(usize, u64)>,
}

impl<T: Clone> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let ReceiverStream {
            receiver,
            slot_and_epoch,
        } = self.get_mut();
        match try_advance_reader(
            &mut &mut *receiver,
            Some(FutureContext { slot_and_epoch, cx }),
        ) {
            Ok(value) => Poll::Ready(Some((*value).clone())),
            Err(TryRecvError::Closed) => Poll::Ready(None),
            _ => Poll::Pending,
        }
    }
}

impl<T> Drop for ReceiverStream<T> {
    fn drop(&mut self) {
        if let Some((slot, epoch)) = self.slot_and_epoch.take() {
            self.receiver.inner.read_waiters.lock().remove(slot, epoch)
        }
    }
}

impl<'a, T> ItemSink<'a, T> for Sender<T>
where
    T: Send + Sync + 'a,
{
    type Error = SendError<T>;
    type SendFuture = TopicSend<'a, T>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        self.send(value)
    }
}

pub mod discarding {

    use swim_future::item_sink::ItemSink;

    /// Wraps a [`topic::Sender`] for a sink implementation that uses the discarding send function.
    pub struct Discarding<T>(pub super::Sender<T>);

    impl<'a, T> ItemSink<'a, T> for Discarding<T>
    where
        T: Send + Sync + 'a,
    {
        type Error = super::SendError<T>;
        type SendFuture = super::TopicSend<'a, T>;

        fn send_item(&'a mut self, value: T) -> Self::SendFuture {
            let Discarding(sender) = self;
            sender.discarding_send(value)
        }
    }
}
