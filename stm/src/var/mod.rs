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
use tokio::sync::RwLock;
use std::sync::Arc;
use std::fmt::{Debug, Formatter};

pub(crate) trait VarRef {}

pub(crate) struct TVarInner<T> {
    content: RwLock<Arc<T>>,
    _waker: AtomicWaker,
}

impl<T> VarRef for TVarInner<T> {}

impl<T: Send + Sync + 'static> TVarInner<T> {

    pub(crate) fn new(value: T) -> Self {
        TVarInner {
            content: RwLock::new(Arc::new(value)),
            _waker: AtomicWaker::new()
        }
    }

    pub(crate) async fn read(&self) -> Arc<T> {
        let lock = self.content.read().await;
        lock.clone()
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
