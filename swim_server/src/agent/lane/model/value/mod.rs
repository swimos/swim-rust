// Copyright 2015-2021 SWIM.AI inc.
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
use crate::StoreError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::Any;
use std::sync::Arc;
use store::stores::lane::value::ValueDataModel;

#[cfg(test)]
mod tests;

/// A lane containing a single value.
#[derive(Debug)]
pub struct ValueLane<T> {
    data_model: ValueDataModel<T>,
}

impl<T> Clone for ValueLane<T> {
    fn clone(&self) -> Self {
        ValueLane {
            data_model: self.data_model.clone(),
        }
    }
}

impl<T: Any + Send + Sync> ValueLane<T> {
    pub fn new(model: ValueDataModel<T>) -> Self {
        ValueLane { data_model: model }
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
        this.data_model.eq(&other.data_model)
    }
}

impl<T> ValueLane<T>
where
    T: Any + Send + Sync + Serialize + DeserializeOwned + Default,
{
    /// Get the current value.
    pub async fn get(&self) -> Result<Arc<T>, StoreError> {
        self.data_model.get().await
    }

    /// Update the current value.
    pub async fn set(&self, value: T) -> Result<(), StoreError> {
        self.data_model.set(value).await
    }

    /// Get the current value, outside of a transaction.
    pub async fn load(&self) -> Result<Arc<T>, StoreError> {
        self.data_model.load().await
    }

    /// Store a value to the lane, outside of a transaction.
    pub async fn store(&self, value: T) -> Result<(), StoreError> {
        self.data_model.store(value).await
    }

    pub async fn get_for_update(&self, op: impl Fn(Arc<T>) -> T + Sync) -> Result<(), StoreError> {
        self.data_model.get_for_update(op).await
    }

    /// Locks the variable, preventing it from being read from or written to. This is
    /// required to force the ordering of events in some unit tests.
    #[cfg(test)]
    pub(crate) async fn lock(&self) -> Option<crate::engines::mem::var::TVarLock> {
        self.data_model.lock().await
    }
}
