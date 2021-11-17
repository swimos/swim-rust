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

#[cfg(test)]
pub(crate) mod tests;

use crate::ptr::Addressed;
use crate::var::observer::Observer;
use futures::future::FutureExt;
use futures::ready;
use parking_lot::Mutex;
use slab::Slab;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use swim_utilities::ptr::data_ptr_eq;
use swim_utilities::sync::rwlock::{ReadFuture, ReadGuard, RwLock, WriteGuard};
use swim_utilities::sync::topic;

pub mod observer;

// The type of the contents of a transactional variable.
// TODO: It would be better if the contents were allocated within the variable itself.
pub(crate) type Contents = Arc<dyn Any + Send + Sync>;

pub(crate) struct TVarGuarded {
    content: Contents,
    observer: Option<topic::Sender<Contents>>,
    wakers: Mutex<Slab<Waker>>,
}

impl TVarGuarded {
    /// Notify the observer and any wakers, if present.
    async fn notify(&mut self) {
        if let Some(observer) = &mut self.observer {
            if observer
                .discarding_send(self.content.clone())
                .await
                .is_err()
            {
                self.observer = None;
            }
        }
        self.wakers.lock().drain().for_each(|w| w.wake());
    }
}

// Type erased contents of a transactional cell.
#[derive(Clone)]
pub(in crate) struct TVarInner {
    guarded: RwLock<TVarGuarded>,
}

impl Addressed for TVarInner {
    type Referent = u8;

    fn addr(&self) -> *const Self::Referent {
        self.guarded.addr()
    }
}

impl TVarInner {
    /// Erase the type of a value to store it inside a transactional variable.
    pub fn new<T>(value: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self::from_arc(Arc::new(value))
    }

    /// Erase the type of a value, stored in an Arc, to store it inside a transactional variable.
    pub fn from_arc<T>(value: Arc<T>) -> Self
    where
        T: Any + Send + Sync,
    {
        TVarInner {
            guarded: RwLock::new(TVarGuarded {
                content: value,
                observer: None,
                wakers: Default::default(),
            }),
        }
    }

    /// Create a transactional variable with an observer than will be notified
    /// each time the value changes.
    ///
    /// # Arguments
    /// * `value` - The initial value of the variable.
    /// * `buffer_size` - The size of the circular buffer used by the observer.
    pub fn from_arc_with_observer<T>(
        value: Arc<T>,
        buffer_size: NonZeroUsize,
    ) -> (Self, Observer<T>)
    where
        T: Any + Send + Sync,
    {
        let (tx, rx) = topic::channel(buffer_size);
        let observer = Observer::new(rx);
        (
            TVarInner {
                guarded: RwLock::new(TVarGuarded {
                    content: value,
                    observer: Some(tx),
                    wakers: Default::default(),
                }),
            },
            observer,
        )
    }

    /// Create a transactional variable with an observer than will be notified
    /// each time the value changes.
    /// # Arguments
    /// * `value` - The initial value of the variable.
    /// * `buffer_size` - The size of the circular buffer used by the observer.
    pub fn new_with_observer<T>(value: T, buffer_size: NonZeroUsize) -> (Self, Observer<T>)
    where
        T: Any + Send + Sync,
    {
        Self::from_arc_with_observer(Arc::new(value), buffer_size)
    }

    /// Read the contents of the variable.
    pub fn read(&self) -> ReadContentsFuture {
        ReadContentsFuture {
            inner: self.guarded.read(),
        }
    }

    /// Determine if the contents of the variable have changed as compared to a previous value.
    pub fn has_changed(&self, ptr: &Contents) -> bool {
        if let Some(guard) = self.guarded.try_read() {
            !data_ptr_eq(guard.deref().content.as_ref(), ptr.as_ref())
        } else {
            true
        }
    }

    /// Determine if the contents of the variable have changed and register a waker if they
    /// have not.
    pub fn add_waker(&self, ptr: &Contents, slot: &mut Option<usize>, waker: &Waker) -> bool {
        if let Some(guard) = self.guarded.read().now_or_never() {
            if data_ptr_eq(guard.deref().content.as_ref(), ptr.as_ref()) {
                let mut lock = guard.wakers.lock();
                match slot.as_ref().and_then(|i| lock.get_mut(*i)) {
                    Some(existing) => {
                        *existing = waker.clone();
                    }
                    _ => *slot = Some(lock.insert(waker.clone())),
                };
                false
            } else {
                true
            }
        } else {
            true
        }
    }

    /// Determine if the contents of the variable have changed as compared to a previous value and,
    /// if not, take the read lock on the variable.
    pub(crate) async fn validate_read(
        &self,
        expected: Contents,
    ) -> Result<ReadGuard<TVarGuarded>, ()> {
        let guard = self.guarded.read().await;
        if data_ptr_eq(guard.deref().content.as_ref(), expected.as_ref()) {
            Ok(guard)
        } else {
            Err(())
        }
    }

    /// Determine if the contents of the variable have changed as compared to a previous value and,
    /// if not, take the write lock on the variable.
    pub async fn prepare_write(
        &self,
        expected: Option<Contents>,
        value: Contents,
    ) -> Result<ApplyWrite, ()> {
        let guard = self.guarded.write().await;
        match expected {
            Some(expected) if !data_ptr_eq(guard.deref().content.as_ref(), expected.as_ref()) => {
                Err(())
            }
            _ => Ok(ApplyWrite { guard, value }),
        }
    }
}

/// A transactional variable that can be read and written by [`crate::stm::Stm`] transactions.
pub struct TVar<T>(TVarInner, PhantomData<Arc<T>>);

impl<T> Clone for TVar<T> {
    fn clone(&self) -> Self {
        let TVar(inner, _) = self;
        TVar(inner.clone(), PhantomData)
    }
}

impl<T> Debug for TVar<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TVar<{}>", std::any::type_name::<T>())
    }
}

impl<T: Default + Send + Sync + 'static> Default for TVar<T> {
    fn default() -> Self {
        TVar::new(Default::default())
    }
}

/// Representation of a transactional read from a variable.
pub struct TVarRead<T>(TVarInner, PhantomData<fn() -> Arc<T>>);

impl<T> Clone for TVarRead<T> {
    fn clone(&self) -> Self {
        TVarRead(self.0.clone(), PhantomData)
    }
}

impl<T> TVarRead<T> {
    pub(crate) fn inner(&self) -> &TVarInner {
        &self.0
    }
}

impl<T> Debug for TVarRead<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Read[TVar<{}>]", std::any::type_name::<T>())
    }
}

/// Future for reading the current value of the [`TVar`].
pub(crate) struct ReadContentsFuture {
    inner: ReadFuture<TVarGuarded>,
}

impl Future for ReadContentsFuture {
    type Output = Contents;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let guarded = ready!(self.inner.poll_unpin(cx));
        Poll::Ready(guarded.content.clone())
    }
}

/// Representation of a transactional write to a variable.
pub struct TVarWrite<T> {
    pub(crate) inner: TVarInner,
    pub(crate) value: Arc<T>,
    _op_t_: PhantomData<fn(Arc<T>) -> ()>,
}

impl<T: Debug> Debug for TVarWrite<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Write[TVar<{}> << {:?}]",
            std::any::type_name::<T>(),
            &self.value
        )
    }
}

impl<T: Any + Send + Sync> TVar<T> {
    pub fn new(initial: T) -> Self {
        TVar(TVarInner::new(initial), PhantomData)
    }

    pub fn from_arc(initial: Arc<T>) -> Self {
        TVar(TVarInner::from_arc(initial), PhantomData)
    }

    pub fn new_with_observer(initial: T, buffer_size: NonZeroUsize) -> (Self, Observer<T>) {
        let (inner, observer) = TVarInner::new_with_observer(initial, buffer_size);
        (TVar(inner, PhantomData), observer)
    }

    pub fn from_arc_with_observer(
        initial: Arc<T>,
        buffer_size: NonZeroUsize,
    ) -> (Self, Observer<T>) {
        let (inner, observer) = TVarInner::from_arc_with_observer(initial, buffer_size);
        (TVar(inner, PhantomData), observer)
    }
}

impl<T> TVar<T> {
    /// Read from the variable as part of a transaction.
    pub fn get(&self) -> TVarRead<T> {
        let TVar(inner, ..) = self;
        TVarRead(inner.clone(), PhantomData)
    }

    /// Write to the variable as part of a transaction.
    pub fn put(&self, value: T) -> TVarWrite<T> {
        let TVar(inner, ..) = self;
        TVarWrite {
            inner: inner.clone(),
            value: Arc::new(value),
            _op_t_: PhantomData,
        }
    }

    /// Write to the variable as part of a transaction.
    pub fn put_arc(&self, value: Arc<T>) -> TVarWrite<T> {
        let TVar(inner, ..) = self;
        TVarWrite {
            inner: inner.clone(),
            value,
            _op_t_: PhantomData,
        }
    }

    /// Determine whether two variables are the same.
    pub fn same_var(this: &Self, other: &Self) -> bool {
        RwLock::same_lock(&this.0.guarded, &other.0.guarded)
    }

    /// Lock the variable so no reads, writes or transactions can make progress.
    pub async fn lock(&self) -> TVarLock {
        TVarLock(self.0.guarded.write().await)
    }
}

pub struct TVarLock(WriteGuard<TVarGuarded>);

impl<T: Any + Send + Sync> TVar<T> {
    /// Load the value of the variable outside of a transaction.
    pub async fn load(&self) -> Arc<T> {
        let TVar(inner, ..) = self;
        let lock = inner.guarded.read().await;
        let content_ref: Arc<T> = if let Ok(content) = lock.deref().content.clone().downcast() {
            content
        } else {
            unreachable!()
        };
        content_ref
    }

    async fn store_arc(&self, value: Arc<T>) {
        let TVar(inner, ..) = self;
        let mut lock = inner.guarded.write().await;
        (*lock).content = value;
        lock.notify().await;
    }

    /// Store a value in the variable outside of a transaction.
    pub async fn store<U: Into<Arc<T>>>(&self, value: U) {
        self.store_arc(value.into()).await
    }
}

impl<T: Any + Clone> TVar<T> {
    /// Clone the contents of the variable outside of a transaction.
    pub async fn snapshot(&self) -> T {
        let TVar(inner, ..) = self;
        let lock = inner.guarded.read().await;
        let content_ref: &T = if let Some(content) = lock.deref().content.downcast_ref() {
            content
        } else {
            unreachable!()
        };
        content_ref.clone()
    }
}

/// Holds the write lock on the variable and a value to be written allowing the write to be
/// applied later.
pub(crate) struct ApplyWrite {
    guard: WriteGuard<TVarGuarded>,
    value: Contents,
}

impl ApplyWrite {
    /// Apply the pending write and release the lock.
    pub async fn apply(self) {
        let ApplyWrite { mut guard, value } = self;

        (*guard).content = value;
        guard.notify().await;
    }
}
