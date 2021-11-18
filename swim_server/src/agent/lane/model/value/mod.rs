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

use crate::agent::lane::LaneModel;
use std::any::Any;
use std::num::NonZeroUsize;
use std::sync::Arc;
use stm::stm::Stm;
use stm::var::observer::Observer;
use stm::var::TVar;

#[cfg(test)]
mod tests;

use serde::de::DeserializeOwned;
use serde::Serialize;
use server_store::agent::lane::value::{ValueDataModel, ValueLaneStoreIo};
use server_store::agent::lane::StoreIo;
use server_store::agent::NodeStore;

/// A lane containing a single value.
#[derive(Debug)]
pub struct ValueLane<T> {
    value: TVar<T>,
}

impl<T> Clone for ValueLane<T> {
    fn clone(&self) -> Self {
        ValueLane {
            value: self.value.clone(),
        }
    }
}

impl<T: Any + Send + Sync> ValueLane<T> {
    pub fn new(init: T) -> Self {
        ValueLane {
            value: TVar::new(init),
        }
    }

    pub fn observable(init: T, buffer_size: NonZeroUsize) -> (Self, Observer<T>) {
        let (var, observer) = TVar::new_with_observer(init, buffer_size);
        (ValueLane { value: var }, observer)
    }
}

impl<T> ValueLane<T>
where
    T: Any + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    pub fn store_observable<Store: NodeStore>(
        model: &ValueDataModel<Store, T>,
        buffer_size: NonZeroUsize,
        default: T,
    ) -> (Self, Observer<T>) {
        match model.load().expect("Failed to load value lane state") {
            Some(value) => Self::observable(value, buffer_size),
            None => Self::observable(default, buffer_size),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ValueLaneEvent<T> {
    pub previous: Option<Arc<T>>,
    pub current: Arc<T>,
}

impl<T> LaneModel for ValueLane<T>
where
    T: Send + Sync + 'static,
{
    type Event = ValueLaneEvent<T>;

    fn same_lane(this: &Self, other: &Self) -> bool {
        TVar::same_var(&this.value, &other.value)
    }
}

impl<T: Any + Send + Sync> ValueLane<T> {
    /// Get the current value.
    pub fn get(&self) -> impl Stm<Result = Arc<T>> {
        self.value.get()
    }

    /// Update the current value.
    pub fn set(&self, value: T) -> impl Stm<Result = ()> {
        self.value.put(value)
    }

    /// Get the current value, outside of a transaction.
    pub async fn load(&self) -> Arc<T> {
        self.value.load().await
    }

    /// Store a value to the lane, outside of a transaction.
    pub async fn store(&self, value: T) {
        self.value.store(value).await;
    }

    /// Locks the variable, preventing it from being read from or written to. This is
    /// required to force the ordering of events in some unit tests.
    #[cfg(test)]
    pub async fn lock(&self) -> stm::var::TVarLock {
        self.value.lock().await
    }
}

pub fn streamed_value_lane<T, Store>(
    name: impl Into<String>,
    buffer_size: NonZeroUsize,
    transient: bool,
    store: Store,
) -> (ValueLane<T>, Observer<T>, Option<Box<dyn StoreIo>>)
where
    Store: NodeStore,
    T: Default + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    let lane_id = store
        .lane_id_of(&name.into())
        .expect("Failed to fetch lane id");
    let model = ValueDataModel::new(store, lane_id);

    let (lane, observer) = ValueLane::store_observable(&model, buffer_size, Default::default());

    let store_io: Option<Box<dyn StoreIo>> = if transient {
        None
    } else {
        Some(Box::new(ValueLaneStoreIo::new(
            observer.clone().into_stream(),
            model,
        )))
    };

    (lane, observer, store_io)
}
