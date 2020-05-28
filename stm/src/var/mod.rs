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

use futures::future::FutureExt;

use tokio::sync::{RwLock, RwLockWriteGuard, RwLockReadGuard};
use std::sync::{Arc, Mutex};
use std::fmt::{Debug, Formatter};
use std::any::Any;
use std::ops::Deref;
use std::marker::PhantomData;
use std::task::Waker;

pub(crate) type Contents = Arc<dyn Any + Send + Sync>;

pub(in crate) struct TVarInner {
    content: RwLock<Contents>,
    wakers: Mutex<Vec<Waker>>,
}

impl TVarInner {

    pub fn new<T: Send + Sync + 'static>(value: T) -> Self {
        TVarInner {
            content: RwLock::new(Arc::new(value)),
            wakers: Mutex::new(vec![]),
        }
    }

    pub async fn read(&self) -> Contents {
        let lock = self.content.read().await;
        lock.clone()
    }

    pub fn notify(&self) {
        self.wakers.lock().unwrap().drain(..).for_each(|w| w.wake());
    }

    pub fn has_changed(&self, ptr: &Contents) -> bool {
        if let Some(guard) = self.content.read().now_or_never() {
            !Arc::ptr_eq(guard.deref(), ptr)
        } else {
            false
        }
    }

    pub fn subscribe(&self, waker: Waker) {
        self.wakers.lock().unwrap().push(waker);
    }

    pub async fn validate_read(&self,
                                      expected: Contents) -> Option<RwLockReadGuard<'_, Contents>> {
        let guard = self.content.read().await;
        if Arc::ptr_eq(guard.deref(), &expected) {
            Some(guard)
        } else {
            None
        }
    }

    pub async fn prepare_write(&self,
                                      expected: Option<Contents>,
                                      value: Contents) -> Option<ApplyWrite<'_>> {
        let guard = self.content.write().await;
        match expected {
            Some(expected) if !Arc::ptr_eq(guard.deref(), &expected) => None,
            _ => {
                Some(ApplyWrite {
                    var: self,
                    guard,
                    value,
                })
            }
        }
    }

}



#[derive(Clone)]
pub struct TVar<T>(Arc<TVarInner>, PhantomData<Arc<T>>,);

impl<T> Debug for TVar<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TVar<{}>", std::any::type_name::<T>())
    }
}

pub struct TVarRead<T>(Arc<TVarInner>, PhantomData<fn() -> Arc<T>>);

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

pub struct TVarWrite<T> {
    pub(crate) inner: Arc<TVarInner>,
    pub(crate) value: Arc<T>,
    _op_t_: PhantomData<fn(Arc<T>) -> ()>
}

impl<T: Debug> Debug for TVarWrite<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Write[TVar<{}> << {:?}]", std::any::type_name::<T>(), &self.value)
    }
}


impl<T: Send + Sync + 'static> TVar<T> {

    pub fn new(initial: T) -> Self {
        TVar(Arc::new(TVarInner::new(initial)), PhantomData)
    }

}

impl<T> TVar<T> {

    pub fn get(&self) -> TVarRead<T> {
        let TVar(inner, ..) = self;
        TVarRead(inner.clone(), PhantomData)
    }

    pub fn put(&self, value: T) -> TVarWrite<T> {
        let TVar(inner, ..) = self;
        TVarWrite {
            inner: inner.clone(),
            value: Arc::new(value),
            _op_t_: PhantomData
        }
    }

    pub fn same_var(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

}

impl<T: Any + Send + Sync> TVar<T> {

    pub async fn load(&self) -> Arc<T> {
        let TVar(inner, ..) = self;
        let lock = inner.content.read().await;
        let content_ref: Arc<T> = lock.deref().clone().downcast().unwrap();
        content_ref
    }

    pub async fn store_arc(&self, value: Arc<T>) {
        let TVar(inner, ..) = self;
        let mut lock = inner.content.write().await;
        *lock = value;
        drop(lock);
        self.0.notify();
    }

    pub async fn store(&self, value: T) {
        self.store_arc(Arc::new(value)).await
    }
}

impl<T: Any + Clone> TVar<T> {

    pub async fn snapshot(&self) -> T {
        let TVar(inner, ..) = self;
        let lock = inner.content.read().await;
        let content_ref: &T = lock.deref().downcast_ref().unwrap();
        content_ref.clone()
    }

}


pub(crate) struct ApplyWrite<'a> {
    var: &'a TVarInner,
    guard: RwLockWriteGuard<'a, Contents>,
    value: Contents,
}

impl<'a> ApplyWrite<'a> {

    pub fn apply(self) {
        let ApplyWrite { var, mut guard, value } = self;
        *guard = value;
        drop(guard);
        var.notify();
    }

}

