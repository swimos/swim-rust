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

use futures::task::AtomicWaker;
use futures::future::FutureExt;

use tokio::sync::RwLock;
use std::sync::{Arc, Mutex};
use std::fmt::{Debug, Formatter};
use std::any::Any;
use std::ops::Deref;

pub(crate) trait VarRef {

    fn has_changed(&self, ptr: Arc<dyn Any + Send + Sync>) -> bool;

    fn subscribe(&self, waker: Arc<AtomicWaker>);

}

pub(crate) struct TVarInner<T> {
    content: RwLock<Arc<T>>,
    wakers: Mutex<Vec<Arc<AtomicWaker>>>,
}

impl<T: Any + Send + Sync> VarRef for TVarInner<T> {
    fn has_changed(&self, ptr: Arc<dyn Any + Send + Sync>) -> bool {
        if let Ok(as_t) = ptr.downcast::<T>() {
            if let Some(guard) = self.content.read().now_or_never() {
                !Arc::ptr_eq(guard.deref(), &as_t)
            } else {
                false
            }
        } else {
            panic!("Incompatible pointers.")
        }
    }

    fn subscribe(&self, waker: Arc<AtomicWaker>) {
        self.wakers.lock().unwrap().push(waker);
    }
}

impl<T: Send + Sync + 'static> TVarInner<T> {

    pub(crate) fn new(value: T) -> Self {
        TVarInner {
            content: RwLock::new(Arc::new(value)),
            wakers: Mutex::new(vec![])
        }
    }

    pub(crate) async fn read(&self) -> Arc<T> {
        let lock = self.content.read().await;
        lock.clone()
    }

    pub(crate) fn notify(&self) {
        self.wakers.lock().unwrap().drain(..).for_each(|w| w.wake());
    }

}



#[derive(Clone)]
pub struct TVar<T>(Arc<TVarInner<T>>);

impl<T> Debug for TVar<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TVar<{}>", std::any::type_name::<T>())
    }
}

pub struct TVarRead<T>(pub(crate) Arc<TVarInner<T>>);

impl<T> Debug for TVarRead<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Read[TVar<{}>]", std::any::type_name::<T>())
    }
}

pub struct TVarWrite<T> {
    pub(crate) inner: Arc<TVarInner<T>>,
    pub(crate) value: Arc<T>,
}

impl<T: Debug> Debug for TVarWrite<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Write[TVar<{}> << {:?}]", std::any::type_name::<T>(), &self.value)
    }
}


impl<T: Send + Sync + 'static> TVar<T> {

    pub fn new(initial: T) -> Self {
        TVar(Arc::new(TVarInner::new(initial)))
    }

}

impl<T> TVar<T> {

    pub fn get(&self) -> TVarRead<T> {
        let TVar(inner) = self;
        TVarRead(inner.clone())
    }

    pub fn put(&self, value: T) -> TVarWrite<T> {
        let TVar(inner) = self;
        TVarWrite {
            inner: inner.clone(),
            value: Arc::new(value),
        }
    }

    pub fn same_var(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

}
