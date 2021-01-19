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

#[cfg(test)]
mod tests;

use std::cell::UnsafeCell;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use futures_util::task::AtomicWaker;
use pin_project::pin_project;
use pin_utils::core_reexport::num::NonZeroUsize;

use crate::sync::ReadWaiters;

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
}

unsafe impl<T> Send for Inner<T> {}
unsafe impl<T> Sync for Inner<T> {}

pub fn channel<T>(n: NonZeroUsize) -> (Sender<T>, Receiver<T>) {
    let n = n.get();
    let inner = Arc::new(Inner {
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
    });
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

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    NoReceivers(T),
    NoCapacity(T),
}

#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

pub enum TryRecvError {
    Closed,
    NoValue,
}

impl<T> Sender<T> {
    pub fn send(&mut self, value: T) -> TopicSend<T> {
        let Sender { inner } = self;
        TopicSend {
            inner: &*inner,
            value: Some(value),
        }
    }

    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
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
            let data_ref = unsafe { &mut *entry.data.get() };
            *data_ref = Some(value);
            drop(lock);
            read_waiters.lock().wake_all();
            Ok(())
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.active.store(false, Ordering::Release);
        self.inner.writer_waiter.take();
        self.inner.read_waiters.lock().wake_all();
    }
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    read_offset: usize,
}

impl<T> Receiver<T> {
    pub fn recv(&mut self) -> TopicReceive<T> {
        TopicReceive {
            receiver: self,
            slot_and_epoch: None,
        }
    }

    pub fn try_recv(&mut self) -> Result<EntryGuard<T>, TryRecvError> {
        try_advance_reader(&mut &mut *self, None)
    }
}

impl<T: Clone> Receiver<T> {
    pub fn into_stream(self) -> ReceiverStream<T> {
        ReceiverStream {
            receiver: self,
            slot_and_epoch: None,
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let Receiver { inner, .. } = self;
        let new_inner = inner.clone();
        let Inner {
            capacity, guarded, ..
        } = &**inner;
        let mut lock = guarded.write();
        let offset = prev_slot(lock.write_offset.load(Ordering::Acquire), *capacity);
        lock.num_rx = lock.num_rx.checked_add(1).expect("Receiver overflow.");
        drop(lock);
        Receiver {
            inner: new_inner,
            read_offset: offset,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct EntryGuard<'a, T> {
    value: &'a T,
}

impl<'a, T> Deref for EntryGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
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
            ..
        } = &**inner;
        let mut lock = guarded.write();
        lock.num_rx -= 1;
        let write_offset = lock.write_offset.load(Ordering::Relaxed);
        let mut i = next_slot(*read_offset, *capacity);
        let mut new_floor = None;
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

#[pin_project]
pub struct TopicSend<'a, T> {
    #[pin]
    inner: &'a Inner<T>,
    value: Option<T>,
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
        let read = this.inner.read_floor.load(Ordering::Acquire);
        let lock = this.inner.guarded.read();
        if lock.num_rx == 0 {
            return Poll::Ready(Err(SendError(
                this.value.take().expect("Send future polled twice."),
            )));
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
        let data_ref = unsafe { &mut *entry.data.get() };
        *data_ref = Some(this.value.take().expect("Send future polled twice."));
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
