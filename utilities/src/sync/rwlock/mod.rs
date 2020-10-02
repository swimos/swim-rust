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
use std::fmt::{Debug, Formatter};
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
    waker: Option<Waker>,
}

impl OrderedWaker {
    fn new(waker: Waker, prev: Option<usize>) -> Self {
        OrderedWaker {
            prev,
            next: None,
            waker: Some(waker),
        }
    }
}

/// Intrusive linked list of tasks waiting for a write lock.
#[derive(Default)]
struct WriterQueue {
    first: Option<usize>,
    last: Option<usize>,
    wakers: Slab<OrderedWaker>,
    len: usize,
}

struct Wakers<'a>(&'a Slab<OrderedWaker>);

impl<'a> Debug for Wakers<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.0.iter()).finish()
    }
}

impl Debug for WriterQueue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterQueue")
            .field("first", &self.first)
            .field("wakers", &Wakers(&self.wakers))
            .finish()
    }
}

impl WriterQueue {
    /// Adds a waker for another tasks to the queue. If the slot is specified, it will attempt
    /// to replace the waker in that slot, if occupied.
    fn add_waker(&mut self, waker: Waker, slot: Option<usize>) -> usize {
        let WriterQueue {
            first,
            last,
            wakers,
            len,
        } = self;

        let maybe_existing = slot.and_then(|i| wakers.get_mut(i).map(|ord_waker| (i, ord_waker)));

        let (i, unlinked) = if let Some((
            i,
            OrderedWaker {
                prev,
                next,
                waker: existing,
            },
        )) = maybe_existing
        {
            let unlinked = existing.is_none();
            *existing = Some(waker);
            if unlinked {
                *prev = *last;
                *next = None;
            }
            (i, unlinked)
        } else {
            let new_entry = OrderedWaker::new(waker, *last);
            let i = wakers.insert(new_entry);
            (i, true)
        };

        if unlinked {
            *len += 1;
            if let Some(prev) = last.and_then(|j| wakers.get_mut(j)) {
                prev.next = Some(i);
            }
            *last = Some(i);
            if *len == 1 {
                *first = Some(i);
            }
        }
        i
    }

    /// Remove the waker in the specified slot, if it exists.
    fn remove(&mut self, index: usize) {
        let WriterQueue {
            first,
            last,
            wakers,
            len,
        } = self;
        if wakers.contains(index) {
            let OrderedWaker { prev, next, waker } = wakers.remove(index);
            if waker.is_some() {
                *len -= 1;
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
    }

    /// Take the waker at the head of the queue. The head node is unlinked but left in the slab
    /// until the owning future removes it (to avoid double allocation of slots).
    fn poll(&mut self) -> Option<Waker> {
        let WriterQueue {
            first,
            last,
            wakers,
            len,
        } = self;
        if let Some(OrderedWaker { next, waker, .. }) =
            first.and_then(|first| wakers.get_mut(first))
        {
            let waker = waker.take();
            debug_assert!(waker.is_some());
            let new_head = *next;
            *next = None;
            *len -= 1;
            *first = new_head;
            if *len <= 1 {
                *last = new_head;
            }
            if let Some(next_rec) = new_head.and_then(|i| wakers.get_mut(i)) {
                next_rec.prev = None;
            }
            waker
        } else {
            None
        }
    }
}

/// Maintains a bag of readers waiting to obtain the lock.
#[derive(Debug, Default)]
struct ReadWaiters {
    waiters: Slab<Waker>,
    epoch: u64,
}

impl ReadWaiters {

    /// Insert a new waker return the assigned slot and the current epoch.
    fn insert(&mut self, waker: Waker) -> (usize, u64) {
        let ReadWaiters { waiters, epoch } = self;
        (waiters.insert(waker), *epoch)
    }

    /// If the epoch has changed, does the same as `insert`, otherwise updates the waker in
    /// the specified slot.
    fn update(&mut self, waker: Waker, slot: usize, expected_epoch: u64) -> (usize, u64) {
        let ReadWaiters { waiters, epoch } = self;
        if *epoch == expected_epoch {
            if let Some(existing) = waiters.get_mut(slot) {
                *existing = waker;
                (slot, expected_epoch)
            } else {
                (waiters.insert(waker), *epoch)
            }
        } else {
            (waiters.insert(waker), *epoch)
        }
    }

    /// Remove the waker at the provided slot if the epoch has not changed.
    fn remove(&mut self, slot: usize, expected_epoch: u64) {
        let ReadWaiters { waiters, epoch } = self;
        if *epoch == expected_epoch {
            waiters.remove(slot);
        }
    }

    /// Wake all wakers and advance the epoch.
    fn wake_all(&mut self) {
        let ReadWaiters { waiters, epoch } = self;
        *epoch = epoch.wrapping_add(1);
        waiters.drain().for_each(|w| w.wake());
    }

    fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }
}

#[derive(Debug)]
struct RwLockInner<T> {
    state: AtomicIsize,
    contents: UnsafeCell<T>,
    read_waiters: Mutex<ReadWaiters>,
    write_queue: Mutex<WriterQueue>,
}

impl<T> RwLockInner<T> {
    //If this returns true, a write lock has been taken and the caller should immediately create
    //a write guard.
    fn try_write(self: &Arc<Self>) -> bool {
        self.state
            .compare_exchange(0, -1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    //If this returns true, a read lock has been taken and the caller should immediately create
    //a read guard.
    fn try_read_inner(self: &Arc<Self>, slot_and_epoch: &mut Option<(usize, u64)>) -> bool {
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
                    if let Some((slot, expected_epoch)) = slot_and_epoch.take() {
                        self.read_waiters.lock().remove(slot, expected_epoch);
                    }
                    return true;
                } else {
                    spinner.spin_no_yield();
                }
            }
        }
    }

    /// Try to immediately take a read lock, if possible.
    fn try_read(
        self: Arc<Self>,
        slot_and_epoch: &mut Option<(usize, u64)>,
    ) -> Result<ReadGuard<T>, Arc<Self>> {
        if self.try_read_inner(slot_and_epoch) {
            Ok(ReadGuard(self))
        } else {
            Err(self)
        }
    }

    /// Try to immediately take a read lock, if possible, by reference.
    fn try_read_ref(
        self: &Arc<Self>,
        slot_and_epoch: &mut Option<(usize, u64)>,
    ) -> Option<ReadGuard<T>> {
        if self.try_read_inner(slot_and_epoch) {
            Some(ReadGuard(self.clone()))
        } else {
            None
        }
    }

    /// Try to immediately take a write lock, if possible.
    fn try_write_fut(
        self: Arc<Self>,
        slot: &mut Option<usize>,
    ) -> Result<WriteGuard<T>, Arc<Self>> {
        if self.try_write() {
            if let Some(i) = slot.take() {
                self.write_queue.lock().remove(i);
            }
            Ok(WriteGuard(self))
        } else {
            Err(self)
        }
    }

    /// Try to immediately take a write lock, if possible, by reference.
    fn try_write_ref(self: &Arc<Self>) -> Option<WriteGuard<T>> {
        if self.try_write() {
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
    fn add_read_waker(&self, waker: Waker, slot_and_epoch: Option<(usize, u64)>) -> (usize, u64) {
        let mut read_wakers = self.read_waiters.lock();
        if let Some((slot, expected_epoch)) = slot_and_epoch {
            read_wakers.update(waker, slot, expected_epoch)
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RwLockId(usize);

impl<T: Send + Sync> RwLock<T> {
    /// Get an opaque address for the contents of the lock.
    pub fn addr(&self) -> *const u8 {
        self.0.as_ref() as *const RwLockInner<T> as *const u8
    }

    /// Determine if two handles are for the same lock.
    pub fn same_lock(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.0, &other.0)
    }
}

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
            slot_and_epoch: None,
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
        inner.try_read_ref(&mut None)
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
    slot_and_epoch: Option<(usize, u64)>,
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
        let mut waiting_writers = inner.write_queue.lock();
        let prev_read_count = inner.state.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(prev_read_count > 0);

        if prev_read_count == 1 {
            if let Some(waker) = waiting_writers.poll() {
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
        let mut waiting_readers = inner.read_waiters.lock();
        if waiting_readers.is_empty() {
            let mut waiting_writers = inner.write_queue.lock();
            inner.state.store(0, Ordering::SeqCst);
            if let Some(waker) = waiting_writers.poll() {
                waker.wake();
            }
        } else {
            inner.state.store(0, Ordering::SeqCst);
            waiting_readers.wake_all();
        }
    }
}

impl<T: Send + Sync> Future for ReadFuture<T> {
    type Output = ReadGuard<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ReadFuture {
            inner,
            slot_and_epoch,
        } = self.get_mut();

        let mut lck_inner = inner.take().expect("Read future used twice.");

        let mut spinner = SpinWait::new();

        loop {
            match lck_inner.try_read(slot_and_epoch) {
                Ok(guard) => {
                    return Poll::Ready(guard);
                }
                Err(blocked) => {
                    lck_inner = blocked;
                    *slot_and_epoch =
                        Some(lck_inner.add_read_waker(cx.waker().clone(), *slot_and_epoch));
                    if lck_inner.is_write_locked() {
                        return match lck_inner.try_read(slot_and_epoch) {
                            Ok(guard) => Poll::Ready(guard),
                            Err(blocked) => {
                                *inner = Some(blocked);
                                Poll::Pending
                            }
                        };
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
            match lck_inner.try_write_fut(slot) {
                Ok(guard) => {
                    return Poll::Ready(guard);
                }
                Err(blocked) => {
                    lck_inner = blocked;
                    *slot = Some(lck_inner.add_write_waker(cx.waker().clone(), *slot));
                    if lck_inner.is_locked() {
                        return match lck_inner.try_write_fut(slot) {
                            Ok(guard) => Poll::Ready(guard),
                            Err(blocked) => {
                                *inner = Some(blocked);
                                Poll::Pending
                            }
                        };
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
        let ReadFuture {
            inner,
            slot_and_epoch,
            ..
        } = self;
        if let Some(inner) = inner.take() {
            if let Some((slot, epoch)) = slot_and_epoch.take() {
                let mut lock = inner.read_waiters.lock();
                lock.remove(slot, epoch);
                let mut waiting_writers = inner.write_queue.lock();
                if let Some(waker) = waiting_writers.poll() {
                    waker.wake();
                }
            }
        }
    }
}

impl<T> Drop for WriteFuture<T> {
    fn drop(&mut self) {
        let WriteFuture { inner, slot, .. } = self;
        if let Some(inner) = inner.take() {
            if let Some(i) = slot.take() {
                let mut waiting_writers = inner.write_queue.lock();
                waiting_writers.remove(i);
                let mut waiting_readers = inner.read_waiters.lock();
                if waiting_readers.is_empty() {
                    if let Some(waker) = waiting_writers.poll() {
                        waker.wake();
                    }
                } else {
                    waiting_readers.wake_all();
                }
            }
        }
    }
}
