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

use crate::engines::mem::stm::Stm;
use crate::engines::mem::transaction::{atomically, RetryManager};
use crate::engines::mem::var::TVar;
use crate::stores::lane::observer::StoreObserver;
use crate::StoreError;
use futures::future::{ready, Ready};
use futures::stream::{empty, Empty};
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Debug)]
pub struct ValueDataMemStore<V> {
    // todo add retry manager field
    var: TVar<V>,
}

impl<V> PartialEq for ValueDataMemStore<V> {
    fn eq(&self, other: &Self) -> bool {
        TVar::same_var(&self.var, &other.var)
    }
}

impl<V> ValueDataMemStore<V>
where
    V: Send + Sync + 'static,
{
    pub fn new(default: V) -> ValueDataMemStore<V> {
        ValueDataMemStore {
            var: TVar::new(default),
        }
    }

    pub fn observable(
        default: V,
        buffer_size: NonZeroUsize,
    ) -> (ValueDataMemStore<V>, StoreObserver<V>) {
        let (var, observer) = TVar::new_with_observer(default, buffer_size);
        (ValueDataMemStore { var }, StoreObserver::Mem(observer))
    }

    pub async fn atomically<O>(&self, op: impl Stm<Result = O>) -> Result<O, StoreError> {
        atomically(&op, ExactlyOnce)
            .await
            .map_err(|e| StoreError::Delegate(Box::new(e)))
    }
}

impl<V> Clone for ValueDataMemStore<V> {
    fn clone(&self) -> Self {
        ValueDataMemStore {
            var: self.var.clone(),
        }
    }
}

impl<V> ValueDataMemStore<V>
where
    V: Send + Sync + 'static,
{
    pub async fn store(&self, value: V) {
        self.var.store(Arc::new(value)).await;
    }

    pub async fn load(&self) -> Arc<V> {
        self.var.load().await
    }

    pub async fn get(&self) -> Result<Arc<V>, StoreError> {
        self.atomically(&self.var.get()).await
    }

    pub async fn set(&self, value: V) -> Result<(), StoreError> {
        self.atomically(&self.var.put(value)).await
    }

    pub async fn get_for_update(&self, op: impl Fn(Arc<V>) -> V + Sync) -> Result<(), StoreError> {
        self.atomically(
            &self
                .var
                .get()
                .and_then(|v| self.var.put_arc(Arc::new(op(v)))),
        )
        .await
    }

    pub async fn lock(&self) -> crate::engines::mem::var::TVarLock {
        self.var.lock().await
    }
}

#[derive(Clone, Debug)]
pub struct ExactlyOnce;

impl RetryManager for ExactlyOnce {
    type ContentionManager = Empty<()>;
    type RetryFut = Ready<bool>;

    fn contention_manager(&self) -> Self::ContentionManager {
        empty()
    }

    fn retry(&mut self) -> Self::RetryFut {
        ready(false)
    }
}
