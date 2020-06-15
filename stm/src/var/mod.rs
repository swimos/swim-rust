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

pub mod observer;

// The type of the contents of a transactional variable.
// TODO: It would be better if the contents were allocated within the variable itself.
pub(crate) type Contents = Arc<dyn Any + Send + Sync>;

// Type erased contents of a transactional cell.
pub(in crate) struct TVarInner {
    content: RwLock<Contents>,
    wakers: Mutex<Vec<Waker>>,
    observer: Option<DynObserver>,
}

impl TVarInner {
    /// Erase the type of a value to store it inside a transactional variable.
    pub fn new<T>(value: T) -> Self
    where
        T: Any + Send + Sync,
    {
        TVarInner {
            content: RwLock::new(Arc::new(value)),
            wakers: Mutex::new(vec![]),
            observer: None,
        }
    }

    /// Create a transactional variable with an observer than will be notified
    /// each time the value changes.
    pub fn new_with_observer<T, Obs>(value: T, observer: Obs) -> Self
    where
        T: Any + Send + Sync,
        Obs: StaticObserver<Arc<T>> + Send + Sync + 'static,
    {
        TVarInner {
            content: RwLock::new(Arc::new(value)),
            wakers: Mutex::new(vec![]),
            observer: Some(Box::new(RawWrapper::new(observer))),
        }
    }

    /// Read the contents of the variable.
    pub async fn read(&self) -> Contents {
        let lock = self.content.read().await;
        lock.clone()
    }

    fn notify_takes_value(&self) -> bool {
        self.observer.is_some()
    }

    /// Notify any futures waiting for the variable to change.
    pub fn notify(&self) {
        self.wakers
            .lock()
            .expect("Locked twice by the same thread.")
            .drain(..)
            .for_each(|w| w.wake());
    }

    /// Notify any futures waiting for the variable to change.
    async fn notify_with(&self, contents: Contents) {
        self.notify();
        if let Some(observer) = &self.observer {
            observer.notify_raw(contents).await;
        }
    }

    /// Determine if the contents of the variable have changed as compared to a previous value.
    pub fn has_changed(&self, ptr: &Contents) -> bool {
        if let Some(guard) = self.content.read().now_or_never() {
            !Arc::ptr_eq(guard.deref(), ptr)
        } else {
            false
        }
    }

    /// Register an interest in the next change to the variable.
    pub fn subscribe(&self, waker: Waker) {
        self.wakers
            .lock()
            .expect("Locked twice by the same thread.")
            .push(waker);
    }

    /// Determine if the contents of the variable have changed as compared to a previous value and,
    /// if not, take the read lock on the variable.
    pub async fn validate_read(&self, expected: Contents) -> Option<RwLockReadGuard<'_, Contents>> {
        let guard = self.content.read().await;
        if Arc::ptr_eq(guard.deref(), &expected) {
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
        let guard = self.content.write().await;
        match expected {
            Some(expected) if !Arc::ptr_eq(guard.deref(), &expected) => None,
            _ => Some(ApplyWrite {
                var: self,
                guard,
                value,
            }),
        }
    }
}

/// A transactional variable that can be read and written by [`crate::stm::Stm`] transactions.
#[derive(Clone)]
pub struct TVar<T>(Arc<TVarInner>, PhantomData<Arc<T>>);

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

    pub fn new_with_observer<Obs>(initial: T, observer: Obs) -> Self
    where
        Obs: StaticObserver<Arc<T>> + Send + Sync + 'static,
    {
        TVar(
            Arc::new(TVarInner::new_with_observer(initial, observer)),
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

    /// Determine whether two variables are the same.
    pub fn same_var(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl<T: Any + Send + Sync> TVar<T> {
    /// Load the value of the variable outside of a transaction.
    pub async fn load(&self) -> Arc<T> {
        let TVar(inner, ..) = self;
        let lock = inner.content.read().await;
        let content_ref: Arc<T> = if let Ok(content) = lock.deref().clone().downcast() {
            content
        } else {
            unreachable!()
        };
        content_ref
    }

    async fn store_arc(&self, value: Arc<T>) {
        let TVar(inner, ..) = self;
        let mut lock = inner.content.write().await;
        let duplicate = if inner.notify_takes_value() {
            Some(value.clone())
        } else {
            None
        };
        *lock = value;
        if let Some(value) = duplicate {
            inner.notify_with(value).await;
        } else {
            inner.notify();
        }
        drop(lock);
        self.0.notify();
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
        let lock = inner.content.read().await;
        let content_ref: &T = if let Some(content) = lock.deref().downcast_ref() {
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
    var: &'a TVarInner,
    guard: RwLockWriteGuard<'a, Contents>,
    value: Contents,
}

impl<'a> ApplyWrite<'a> {
    /// Apply the pending write and release the lock.
    pub async fn apply(self) {
        let ApplyWrite {
            var,
            mut guard,
            value,
        } = self;
        let duplicate = if var.notify_takes_value() {
            Some(value.clone())
        } else {
            None
        };
        *guard = value;
        if let Some(value) = duplicate {
            var.notify_with(value).await;
        } else {
            var.notify();
        }
        drop(guard);
    }
}
