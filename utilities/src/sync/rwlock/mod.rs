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

use futures_util::task::Context;
use parking_lot::Mutex;
use parking_lot_core::SpinWait;
use slab::Slab;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::task::Waker;
use tokio::macros::support::{Pin, Poll};

#[cfg(test)]
mod tests;

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

#[derive(Default, Debug)]
struct WriterQueue {
    first: Option<usize>,
    last: Option<usize>,
    wakers: Slab<OrderedWaker>,
}

impl WriterQueue {
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
    read_queue: Mutex<Slab<Waker>>,
    write_queue: Mutex<WriterQueue>,
}

impl<T> RwLockInner<T> {
    fn try_write(self: Arc<Self>, slot: Option<usize>) -> Result<WriteGuard<T>, Arc<Self>> {
        if self
            .state
            .compare_exchange(0, -1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            if let Some(i) = slot {
                self.write_queue.lock().remove(i);
            }
            Ok(WriteGuard(self))
        } else {
            Err(self)
        }
    }

    fn try_read(self: Arc<Self>) -> Result<ReadGuard<T>, Arc<Self>> {
        let mut spinner = SpinWait::new();
        loop {
            let current = self.state.load(Ordering::Relaxed);
            if current < 0 {
                debug_assert!(current == -1);
                return Err(self);
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
                    return Ok(ReadGuard(self));
                } else {
                    spinner.spin_no_yield();
                }
            }
        }
    }

    fn add_write_waker(&self, waker: Waker, slot: Option<usize>) -> usize {
        self.write_queue.lock().add_waker(waker, slot)
    }

    fn add_read_waker(&self, waker: Waker, slot: Option<usize>) -> usize {
        let mut read_wakers = self.read_queue.lock();
        if let Some((existing, i)) =
            slot.and_then(|i| read_wakers.get_mut(i).map(|existing| (existing, i)))
        {
            *existing = waker;
            i
        } else {
            read_wakers.insert(waker)
        }
    }

    fn is_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }

    fn is_write_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) < 0
    }
}

#[derive(Debug)]
pub struct RwLock<T>(Arc<RwLockInner<T>>);

impl<T: Send + Sync> RwLock<T> {
    pub fn new(init: T) -> Self {
        RwLock(Arc::new(RwLockInner {
            state: AtomicIsize::new(0),
            contents: UnsafeCell::new(init),
            read_queue: Default::default(),
            write_queue: Default::default(),
        }))
    }

    pub fn read(&self) -> ReadFuture<T> {
        let RwLock(inner) = self;
        ReadFuture {
            inner: Some(inner.clone()),
            slot: None,
        }
    }

    pub fn write(&self) -> WriteFuture<T> {
        let RwLock(inner) = self;
        WriteFuture {
            inner: Some(inner.clone()),
            slot: None,
        }
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

#[derive(Debug)]
pub struct ReadGuard<T>(Arc<RwLockInner<T>>);

#[derive(Debug)]
pub struct WriteGuard<T>(Arc<RwLockInner<T>>);

#[derive(Debug)]
pub struct ReadFuture<T> {
    inner: Option<Arc<RwLockInner<T>>>,
    slot: Option<usize>,
}

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
        let mut waiting_readers = inner.read_queue.lock();
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
                let lock = inner.read_queue.lock();
                if lock.contains(i) {
                    inner.read_queue.lock().remove(i);
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
