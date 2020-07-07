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
pub(crate) mod tests;

use futures::future::FutureExt;

use crate::var::observer::{DynObserver, RawWrapper, StaticObserver};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use utilities::ptr::data_ptr_eq;

pub mod observer;

// The type of the contents of a transactional variable.
// TODO: It would be better if the contents were allocated within the variable itself.
pub(crate) type Contents = Arc<dyn Any + Send + Sync>;

pub(crate) struct TVarGuarded {
    content: Contents,
    observer: Option<DynObserver>,
    wakers: Mutex<Vec<Waker>>,
}

impl TVarGuarded {
    /// Notify the observer and any wakers, if present.
    async fn notify(&mut self) {
        if let Some(observer) = &mut self.observer {
            observer.notify_raw(self.content.clone()).await;
        }
        self.wakers
            .lock()
            .expect("Locked twice by the same thread.")
            .drain(..)
            .for_each(|w| w.wake());
    }
}

// Type erased contents of a transactional cell.
pub(in crate) struct TVarInner {
    guarded: RwLock<TVarGuarded>,
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
                wakers: Mutex::new(vec![]),
            }),
        }
    }

    /// Create a transactional variable with an observer than will be notified
    /// each time the value changes.
    pub fn from_arc_with_observer<T, Obs>(value: Arc<T>, observer: Obs) -> Self
    where
        T: Any + Send + Sync,
        Obs: StaticObserver<Arc<T>> + Send + Sync + 'static,
    {
        TVarInner {
            guarded: RwLock::new(TVarGuarded {
                content: value,
                observer: Some(Box::new(RawWrapper::new(observer))),
                wakers: Mutex::new(vec![]),
            }),
        }
    }

    /// Create a transactional variable with an observer than will be notified
    /// each time the value changes.
    pub fn new_with_observer<T, Obs>(value: T, observer: Obs) -> Self
    where
        T: Any + Send + Sync,
        Obs: StaticObserver<Arc<T>> + Send + Sync + 'static,
    {
        Self::from_arc_with_observer(Arc::new(value), observer)
    }

    /// Read the contents of the variable.
    pub async fn read(&self) -> Contents {
        let lock = self.guarded.read().await;
        lock.content.clone()
    }

    /// Determine if the contents of the variable have changed as compared to a previous value.
    pub fn has_changed(&self, ptr: &Contents) -> bool {
        if let Some(guard) = self.guarded.read().now_or_never() {
            !data_ptr_eq(guard.deref().content.as_ref(), ptr.as_ref())
        } else {
            true
        }
    }

    /// Determine if the contents of the variable have changed and register a waker if they
    /// have not.
    pub fn add_waker(&self, ptr: &Contents, waker: &Waker) -> bool {
        if let Some(guard) = self.guarded.read().now_or_never() {
            if data_ptr_eq(guard.deref().content.as_ref(), ptr.as_ref()) {
                let mut lock = guard.wakers.lock().unwrap();
                lock.push(waker.clone());
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
    ) -> Option<RwLockReadGuard<'_, TVarGuarded>> {
        let guard = self.guarded.read().await;
        if data_ptr_eq(guard.deref().content.as_ref(), expected.as_ref()) {
            Some(guard)
        } else {
            None
        }
    }

    /// Determine if the contents of the variable have changed as compared to a previous value and,
    /// if not, take the write lock on the variable.
    pub async fn prepare_write(
        &self,
        expected: Option<Contents>,
        value: Contents,
    ) -> Option<ApplyWrite<'_>> {
        let guard = self.guarded.write().await;
        match expected {
            Some(expected) if !data_ptr_eq(guard.deref().content.as_ref(), expected.as_ref()) => {
                None
            }
            _ => Some(ApplyWrite { guard, value }),
        }
    }
}

/// A transactional variable that can be read and written by [`crate::stm::Stm`] transactions.
pub struct TVar<T>(Arc<TVarInner>, PhantomData<Arc<T>>);

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
pub struct TVarRead<T>(Arc<TVarInner>, PhantomData<fn() -> Arc<T>>);

impl<T> Clone for TVarRead<T> {
    fn clone(&self) -> Self {
        TVarRead(self.0.clone(), PhantomData)
    }
}

impl<T> TVarRead<T> {
    pub(crate) fn inner(&self) -> &Arc<TVarInner> {
        &self.0
    }
}

impl<T> Debug for TVarRead<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Read[TVar<{}>]", std::any::type_name::<T>())
    }
}

/// Representation of a transactional write to a variable.
pub struct TVarWrite<T> {
    pub(crate) inner: Arc<TVarInner>,
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
        TVar(Arc::new(TVarInner::new(initial)), PhantomData)
    }

    pub fn from_arc(initial: Arc<T>) -> Self {
        TVar(Arc::new(TVarInner::from_arc(initial)), PhantomData)
    }

    pub fn new_with_observer<Obs>(initial: T, observer: Obs) -> Self
    where
        Obs: StaticObserver<Arc<T>> + Send + Sync + 'static,
    {
        TVar(
            Arc::new(TVarInner::new_with_observer(initial, observer)),
            PhantomData,
        )
    }

    pub fn from_arc_with_observer<Obs>(initial: Arc<T>, observer: Obs) -> Self
    where
        Obs: StaticObserver<Arc<T>> + Send + Sync + 'static,
    {
        TVar(
            Arc::new(TVarInner::from_arc_with_observer(initial, observer)),
            PhantomData,
        )
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
        Arc::ptr_eq(&this.0, &other.0)
    }
}

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
pub(crate) struct ApplyWrite<'a> {
    guard: RwLockWriteGuard<'a, TVarGuarded>,
    value: Contents,
}

impl<'a> ApplyWrite<'a> {
    /// Apply the pending write and release the lock.
    pub async fn apply(self) {
        let ApplyWrite { mut guard, value } = self;

        (*guard).content = value;
        guard.notify().await;
    }
}
