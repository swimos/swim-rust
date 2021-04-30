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

use crate::engines::KeyedSnapshot;
use crate::stores::lane::map::MapDataModel;
use crate::stores::lane::value::ValueDataModel;
use crate::stores::StoreKey;
use crate::{PlaneStore, StoreError, StoreInfo};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::Serialize;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use swim_common::model::text::Text;

/// A trait for defining store engines which open stores for nodes.
///
/// Node stores are responsible for ensuring that the data models that they open for their lanes
/// correctly map their keys to their delegate store engines.
///
/// # Data models
/// Data models may be either persistent or transient. Persistent data models are backed by a store
/// in which operations on persistent data models are mapped to the correct node and delegated to
/// the store; providing that the top-level server store is also persistent.
///
/// Transient data models will live in memory for the duration that a handle to the model exists.
pub trait NodeStore: Send + Sync + Clone + Debug + 'static {
    type Delegate: PlaneStore;

    /// Open a new map data model.
    ///
    /// # Arguments
    /// `lane_uri`: the URI of the lane.
    /// `transient`: whether the lane is a transient lane. If this is `true`, then the data model
    /// returned should be an in-memory store.
    fn map_lane_store<I, K, V>(
        &self,
        lane_uri: I,
    ) -> BoxFuture<'static, MapDataModel<Self::Delegate, K, V>>
    where
        I: ToString + Send + 'static,
        K: Serialize + Send + 'static,
        V: Serialize + Send + 'static,
        Self: Sized;

    /// Open a new value data model.
    ///
    /// # Arguments
    /// `lane_uri`: the URI of the lane.
    /// `transient`: whether the lane is a transient lane. If this is `true`, then the data model
    /// returned should be an in-memory store.
    fn value_lane_store<I, V>(
        &self,
        lane_uri: I,
    ) -> BoxFuture<'static, ValueDataModel<Self::Delegate>>
    where
        I: ToString + Send + 'static,
        V: Serialize + Send + 'static,
        Self: Sized;

    /// Put a key-value pair into the delegate store.
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError>;

    /// Get a value keyed by a lane key from the delegate store.
    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a key-value pair by its lane key from the delegate store.
    fn delete(&self, key: StoreKey) -> Result<(), StoreError>;

    /// Returns information about the delegate store
    fn store_info(&self) -> StoreInfo;
}

/// A node store which is used to open value and map lane data models.
pub struct SwimNodeStore<D> {
    /// The plane store that value and map data models will delegate their store engine operations
    /// to.
    delegate: Arc<D>,
    /// The node URI that this store represents.
    node_uri: Text,
}

impl<D> Debug for SwimNodeStore<D> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("SwimNodeStore")
            .field("node_uri", &self.node_uri)
            .finish()
    }
}

impl<D> Clone for SwimNodeStore<D> {
    fn clone(&self) -> Self {
        SwimNodeStore {
            delegate: self.delegate.clone(),
            node_uri: self.node_uri.clone(),
        }
    }
}

impl<D: PlaneStore> SwimNodeStore<D> {
    /// Create a new Swim node store which will delegate its engine operations to `delegate` and
    /// represents a node at `node_uri`.
    pub fn new<I: Into<Text>>(delegate: D, node_uri: I) -> SwimNodeStore<D> {
        SwimNodeStore {
            delegate: Arc::new(delegate),
            node_uri: node_uri.into(),
        }
    }

    /// Executes a ranged snapshot read prefixed by a lane key and deserialize each key-value pair
    /// using `map_fn`.
    ///
    /// Returns an optional snapshot iterator if entries were found that will yield deserialized
    /// key-value pairs.
    pub(crate) fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: StoreKey,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        self.delegate.load_ranged_snapshot(prefix, map_fn)
    }
}

impl<D: PlaneStore> NodeStore for SwimNodeStore<D> {
    type Delegate = D;

    fn map_lane_store<I, K, V>(&self, lane: I) -> BoxFuture<'static, MapDataModel<D, K, V>>
    where
        I: ToString + Send + 'static,
        K: Serialize + Send + 'static,
        V: Serialize + Send + 'static,
    {
        let node_store = self.clone();
        let key = format!("{}/{}", self.node_uri, lane.to_string());

        async move {
            let lane_id = node_store.delegate.id_for(key).await;
            MapDataModel::new(node_store, lane_id)
        }
        .boxed()
    }

    fn value_lane_store<I, V>(&self, lane: I) -> BoxFuture<'static, ValueDataModel<D>>
    where
        I: ToString + Send + 'static,
        V: Serialize + Send + 'static,
    {
        let node_store = self.clone();
        let key = format!("{}/{}", self.node_uri, lane.to_string());
        async move {
            let lane_id = node_store.delegate.id_for(key).await;
            ValueDataModel::new(node_store, lane_id)
        }
        .boxed()
    }

    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        self.delegate.put(key, value)
    }

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        self.delegate.get(key)
    }

    fn delete(&self, key: StoreKey) -> Result<(), StoreError> {
        self.delegate.delete(key)
    }

    fn store_info(&self) -> StoreInfo {
        self.delegate.store_info()
    }
}
