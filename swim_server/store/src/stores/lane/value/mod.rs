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

use std::num::NonZeroUsize;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::stores::lane::observer::StoreObserver;
use crate::stores::lane::value::db::ValueDataDbStore;
use crate::stores::lane::value::mem::ValueDataMemStore;
use crate::stores::node::SwimNodeStore;
use crate::StoreError;

pub mod db;
pub mod mem;

/// A value lane data model.
#[derive(Debug)]
pub enum ValueDataModel<T> {
    Mem(ValueDataMemStore<T>),
    Db(ValueDataDbStore<T>),
}

impl<V> PartialEq for ValueDataModel<V> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueDataModel::Mem(l_store), ValueDataModel::Mem(r_store)) => l_store.eq(&r_store),
            (ValueDataModel::Db(l_store), ValueDataModel::Db(r_store)) => l_store.eq(&r_store),
            _ => false,
        }
    }
}

impl<V> Clone for ValueDataModel<V> {
    fn clone(&self) -> Self {
        match self {
            ValueDataModel::Mem(store) => ValueDataModel::Mem(store.clone()),
            ValueDataModel::Db(store) => ValueDataModel::Db(store.clone()),
        }
    }
}

impl<V> ValueDataModel<V>
where
    V: Send + Sync + 'static,
{
    /// Constructs a new value data model.
    ///
    /// # Arguments
    /// `delegate`: if this data model is *not* transient, then delegate operations to this store.
    /// `lane_uri`: the lane URI that this store represents.
    /// `transient`: whether this store should be an in-memory model.
    /// `default_value`: the value to set the store to if its empty.
    pub fn new(
        delegate: SwimNodeStore,
        lane_uri: String,
        transient: bool,
        default_value: V,
    ) -> Self {
        if transient {
            ValueDataModel::Mem(ValueDataMemStore::new(default_value))
        } else {
            ValueDataModel::Db(ValueDataDbStore::new(delegate, lane_uri, default_value))
        }
    }

    /// Constructs a new, observable, value data model.
    ///
    /// # Arguments
    /// `delegate`: if this data model is *not* transient, then delegate operations to this store.
    /// `lane_uri`: the lane URI that this store represents.
    /// `transient`: whether this store should be an in-memory model.
    /// `buffer_size`: the capacity of the observation buffer.
    /// `default_value`: the value to set the store to if its empty.
    pub fn observable(
        delegate: SwimNodeStore,
        lane_uri: String,
        transient: bool,
        buffer_size: NonZeroUsize,
        default_value: V,
    ) -> (Self, StoreObserver<V>) {
        if transient {
            let (store, observer) = ValueDataMemStore::observable(default_value, buffer_size);
            (ValueDataModel::Mem(store), observer)
        } else {
            let (store, observer) =
                ValueDataDbStore::observable(delegate, lane_uri, buffer_size, default_value);
            (ValueDataModel::Db(store), observer)
        }
    }
}

impl<V> ValueDataModel<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Serializes and stores `value` outside of a transaction.
    pub async fn store(&self, value: V) -> Result<(), StoreError> {
        match self {
            ValueDataModel::Mem(store) => Ok(store.store(value).await),
            ValueDataModel::Db(store) => store.store(value).await,
        }
    }

    /// Load and deserialize the value outside of a transaction.
    pub async fn load(&self) -> Result<Arc<V>, StoreError> {
        match self {
            ValueDataModel::Mem(store) => Ok(store.load().await),
            ValueDataModel::Db(store) => store.load(),
        }
    }

    /// Load and deserialize the value inside of a transaction if the delegate supports them.
    pub async fn get(&self) -> Result<Arc<V>, StoreError> {
        match self {
            ValueDataModel::Mem(store) => store.get().await,
            ValueDataModel::Db(store) => store.load(),
        }
    }

    /// Serialize and stores `value` inside of a transaction if the delegate supports them.
    pub async fn set(&self, value: V) -> Result<(), StoreError> {
        match self {
            ValueDataModel::Mem(store) => store.set(value).await,
            ValueDataModel::Db(store) => store.store(value).await,
        }
    }

    /// Loads the current value inside of a transaction if the delegate supports them and stores
    /// the result of `op`.
    pub async fn get_for_update(&self, op: impl Fn(Arc<V>) -> V + Sync) -> Result<(), StoreError> {
        match self {
            ValueDataModel::Mem(store) => store.get_for_update(op).await,
            _ => unimplemented!(),
        }
    }

    /// Locks the `TVar` if this is a transient store.
    pub async fn lock(&self) -> Option<crate::engines::mem::var::TVarLock> {
        match self {
            ValueDataModel::Mem(store) => Some(store.lock().await),
            ValueDataModel::Db(_) => None,
        }
    }
}
