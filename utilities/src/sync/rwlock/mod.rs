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

use parking_lot::Mutex;
use parking_lot_core::SpinWait;
use slab::Slab;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[cfg(test)]
mod tests;

/// Node type for [`WriterQueue`].
#[derive(Debug)]
struct OrderedWaker {
    prev: Option<usize>,
    next: Option<usize>,
    waker: Waker,
}

impl OrderedWaker {
    fn new(waker: Waker, prev: Option<usize>) -> Self {
        OrderedWaker {
            prev,
            next: None,
            waker,
        }
    }
}

/// Intrusive linked list of tasks waiting for a write lock.
#[derive(Default, Debug)]
struct WriterQueue {
    first: Option<usize>,
    last: Option<usize>,
    wakers: Slab<OrderedWaker>,
}

impl WriterQueue {
    /// Adds a waker for another tasks to the queue. If the slot is specified, it will attempt
    /// to replace the waker in that slot, if occupied.
    fn add_waker(&mut self, waker: Waker, slot: Option<usize>) -> usize {
        let WriterQueue {
            first,
            last,
            wakers,
        } = self;

        if let Some((i, existing)) =
            slot.and_then(|i| wakers.get_mut(i).map(|ord_waker| (i, &mut ord_waker.waker)))
        {
            *existing = waker;
            i
        } else {
            let new_entry = OrderedWaker::new(waker, *last);
            let i = wakers.insert(new_entry);
            if let Some(prev) = last.and_then(|j| wakers.get_mut(j)) {
                prev.next = Some(i);
            }
            *last = Some(i);
            if wakers.len() == 1 {
                *first = Some(i);
            }
            i
        }
    }

    /// Remove the waker in the specified slot, if it exists.
    fn remove(&mut self, index: usize) {
        let WriterQueue {
            first,
            last,
            wakers,
        } = self;
        if wakers.contains(index) {
            let OrderedWaker { prev, next, .. } = wakers.remove(index);
            match (prev, next) {
                (Some(i), Some(j)) => {
                    wakers[i].next = Some(j);
                    wakers[j].prev = Some(i);
                }
                (Some(i), _) => {
                    wakers[i].next = None;
                    *last = Some(i);
                }
                (_, Some(j)) => {
                    wakers[j].prev = None;
                    *first = Some(j);
                }
                _ => {
                    *first = None;
                    *last = None;
                }
            }
        }
    }

    /// Take the waker at the head of the queue.
    fn poll(&mut self) -> Option<Waker> {
        if let Some(first) = self.first.take() {
            let OrderedWaker { next, waker, .. } = self.wakers.remove(first);
            self.first = next;
            if self.wakers.len() <= 1 {
                self.last = next;
            }
            Some(waker)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct RwLockInner<T> {
    state: AtomicIsize,
    contents: UnsafeCell<T>,
    read_waiters: Mutex<Slab<Waker>>,
    write_queue: Mutex<WriterQueue>,
}

impl<T> RwLockInner<T> {
    //If this returns true, a write lock has been taken and the caller should immediately create
    //a write guard.
    fn try_write_inner(self: &Arc<Self>, slot: Option<usize>) -> bool {
        if self
            .state
            .compare_exchange(0, -1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            if let Some(i) = slot {
                self.write_queue.lock().remove(i);
            }
            true
        } else {
            false
        }
    }

    //If this returns true, a read lock has been taken and the caller should immediately create
    //a read guard.
    fn try_read_inner(self: &Arc<Self>) -> bool {
        let mut spinner = SpinWait::new();
        loop {
            let current = self.state.load(Ordering::Relaxed);
            if current < 0 {
                debug_assert!(current == -1);
                return false;
            } else {
                let new_read_count = current.checked_add(1).expect("Reader overflow.");
                if self
                    .state
                    .compare_exchange_weak(
                        current,
                        new_read_count,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return true;
                } else {
                    spinner.spin_no_yield();
                }
            }
        }
    }

    /// Try to immediately take a read lock, if possible.
    fn try_read(self: Arc<Self>) -> Result<ReadGuard<T>, Arc<Self>> {
        if self.try_read_inner() {
            Ok(ReadGuard(self))
        } else {
            Err(self)
        }
    }

    /// Try to immediately take a read lock, if possible, by reference.
    fn try_read_ref(self: &Arc<Self>) -> Option<ReadGuard<T>> {
        if self.try_read_inner() {
            Some(ReadGuard(self.clone()))
        } else {
            None
        }
    }

    /// Try to immediately take a write lock, if possible.
    fn try_write(self: Arc<Self>, slot: Option<usize>) -> Result<WriteGuard<T>, Arc<Self>> {
        if self.try_write_inner(slot) {
            Ok(WriteGuard(self))
        } else {
            Err(self)
        }
    }

    /// Try to immediately take a write lock, if possible, by reference.
    fn try_write_ref(self: &Arc<Self>) -> Option<WriteGuard<T>> {
        if self.try_write_inner(None) {
            Some(WriteGuard(self.clone()))
        } else {
            None
        }
    }

    /// Register a task waker, waiting to take a write lock.
    fn add_write_waker(&self, waker: Waker, slot: Option<usize>) -> usize {
        self.write_queue.lock().add_waker(waker, slot)
    }

    /// Register a task waker, waiting to take a read lock.
    fn add_read_waker(&self, waker: Waker, slot: Option<usize>) -> usize {
        let mut read_wakers = self.read_waiters.lock();
        if let Some((existing, i)) =
            slot.and_then(|i| read_wakers.get_mut(i).map(|existing| (existing, i)))
        {
            *existing = waker;
            i
        } else {
            read_wakers.insert(waker)
        }
    }

    /// True if any read or write locks are held.
    fn is_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }

    /// True if a write lock is held.
    fn is_write_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) < 0
    }
}

/// A read favouring, asynchronous read/write lock that can be shared between threads.
#[derive(Debug)]
pub struct RwLock<T>(Arc<RwLockInner<T>>);

impl<T: Send + Sync> RwLock<T> {
    /// Create a new read/write lock with the specified initial value.
    pub fn new(init: T) -> Self {
        RwLock(Arc::new(RwLockInner {
            state: AtomicIsize::new(0),
            contents: UnsafeCell::new(init),
            read_waiters: Default::default(),
            write_queue: Default::default(),
        }))
    }

    /// Create a future that will take a read lock.
    pub fn read(&self) -> ReadFuture<T> {
        let RwLock(inner) = self;
        ReadFuture {
            inner: Some(inner.clone()),
            slot: None,
        }
    }

    /// Create a future that will take a write lock.
    pub fn write(&self) -> WriteFuture<T> {
        let RwLock(inner) = self;
        WriteFuture {
            inner: Some(inner.clone()),
            slot: None,
        }
    }

    /// Attempt to take a read lock immediately, if possible.
    pub fn try_read(&self) -> Option<ReadGuard<T>> {
        let RwLock(inner) = self;
        inner.try_read_ref()
    }

    /// Attempt to take a write lock immediately, if possible.
    pub fn try_write(&self) -> Option<WriteGuard<T>> {
        let RwLock(inner) = self;
        inner.try_write_ref()
    }
}

impl<T: Default + Send + Sync> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

unsafe impl<T> Send for RwLockInner<T> {}
unsafe impl<T> Sync for RwLockInner<T> {}

impl<T> Clone for RwLock<T> {
    fn clone(&self) -> Self {
        let RwLock(inner) = self;
        RwLock(inner.clone())
    }
}

/// RAII handle representing a read lock.
#[derive(Debug)]
pub struct ReadGuard<T>(Arc<RwLockInner<T>>);

/// RAII handle representing a write lock.
#[derive(Debug)]
pub struct WriteGuard<T>(Arc<RwLockInner<T>>);

/// Future that will result in a [`ReadGuard`].
#[derive(Debug)]
pub struct ReadFuture<T> {
    inner: Option<Arc<RwLockInner<T>>>,
    slot: Option<usize>,
}

/// Future that will result in a [`WriteGuard`].
#[derive(Debug)]
pub struct WriteFuture<T> {
    inner: Option<Arc<RwLockInner<T>>>,
    slot: Option<usize>,
}

impl<T> Deref for ReadGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let ReadGuard(inner) = self;
        unsafe { &*inner.contents.get() }
    }
}

impl<T> Deref for WriteGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let WriteGuard(inner) = self;
        unsafe { &*inner.contents.get() }
    }
}

impl<T> Drop for ReadGuard<T> {
    fn drop(&mut self) {
        let ReadGuard(inner) = self;
        let prev_read_count = inner.state.fetch_sub(1, Ordering::Release);
        debug_assert!(prev_read_count > 0);

        if prev_read_count == 1 {
            if let Some(waker) = inner.write_queue.lock().poll() {
                waker.wake();
            }
        }
    }
}

impl<T> DerefMut for WriteGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let WriteGuard(inner) = self;
        unsafe { &mut *inner.contents.get() }
    }
}

impl<T> Drop for WriteGuard<T> {
    fn drop(&mut self) {
        let WriteGuard(inner) = self;
        inner.state.store(0, Ordering::Release);
        let mut waiting_readers = inner.read_waiters.lock();
        if waiting_readers.is_empty() {
            if let Some(waker) = inner.write_queue.lock().poll() {
                waker.wake();
            }
        } else {
            waiting_readers.drain().for_each(|waker| waker.wake());
        }
    }
}

impl<T: Send + Sync> Future for ReadFuture<T> {
    type Output = ReadGuard<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ReadFuture { inner, slot } = self.get_mut();

        let mut lck_inner = inner.take().expect("Read future used twice.");

        let mut spinner = SpinWait::new();

        loop {
            match lck_inner.try_read() {
                Ok(guard) => {
                    return Poll::Ready(guard);
                }
                Err(blocked) => {
                    lck_inner = blocked;
                    *slot = Some(lck_inner.add_read_waker(cx.waker().clone(), *slot));
                    if lck_inner.is_write_locked() {
                        *inner = Some(lck_inner);
                        return Poll::Pending;
                    } else {
                        spinner.spin_no_yield();
                    }
                }
            }
        }
    }
}

impl<T: Send + Sync> Future for WriteFuture<T> {
    type Output = WriteGuard<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let WriteFuture { inner, slot } = self.get_mut();

        let mut lck_inner = inner.take().expect("Write future used twice.");

        let mut spinner = SpinWait::new();

        loop {
            match lck_inner.try_write(*slot) {
                Ok(guard) => {
                    return Poll::Ready(guard);
                }
                Err(blocked) => {
                    lck_inner = blocked;
                    *slot = Some(lck_inner.add_write_waker(cx.waker().clone(), *slot));
                    if lck_inner.is_locked() {
                        *inner = Some(lck_inner);
                        return Poll::Pending;
                    } else {
                        spinner.spin_no_yield();
                    }
                }
            }
        }
    }
}

impl<T> Drop for ReadFuture<T> {
    fn drop(&mut self) {
        let ReadFuture { inner, slot, .. } = self;
        if let Some(inner) = inner.take() {
            if let Some(i) = slot.take() {
                let mut lock = inner.read_waiters.lock();
                if lock.contains(i) {
                    lock.remove(i);
                }
            }
        }
    }
}

impl<T> Drop for WriteFuture<T> {
    fn drop(&mut self) {
        let WriteFuture { inner, slot } = self;
        if let Some(inner) = inner.take() {
            if let Some(i) = slot.take() {
                inner.write_queue.lock().remove(i);
            }
        }
    }
}
