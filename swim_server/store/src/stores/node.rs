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

use crate::stores::lane::map::MapDataModel;
use crate::stores::lane::value::ValueDataModel;
use crate::stores::lane::LaneKey;
use crate::stores::plane::SwimPlaneStore;
use crate::stores::{MapStorageKey, StoreKey, ValueStorageKey};
use crate::{KeyedSnapshot, RangedSnapshot, StoreEngine, StoreError};
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;

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
pub trait NodeStore: for<'a> StoreEngine<'a> + Send + Sync + Clone + Debug {
    /// Open a new map data model.
    ///
    /// # Arguments
    /// `lane_uri`: the URI of the lane.
    /// `transient`: whether the lane is a transient lane. If this is `true`, then the data model
    /// returned should be an in-memory store.
    fn map_lane_store<I, K, V>(&self, lane_uri: I, transient: bool) -> MapDataModel<K, V>
    where
        I: ToString,
        K: Serialize,
        V: Serialize,
        Self: Sized;

    /// Open a new value data model.
    ///
    /// # Arguments
    /// `lane_uri`: the URI of the lane.
    /// `transient`: whether the lane is a transient lane. If this is `true`, then the data model
    /// returned should be an in-memory store.
    fn value_lane_store<I, V>(&self, lane_uri: I, transient: bool) -> ValueDataModel
    where
        I: ToString,
        V: Serialize,
        Self: Sized;
}

/// A node store which is used to open value and map lane data models.
#[derive(Debug)]
pub struct SwimNodeStore {
    /// The plane store that value and map data models will delegate their store engine operations
    /// to.
    delegate: Arc<SwimPlaneStore>,
    /// The node URI that this store represents.
    node_uri: Arc<String>,
}

impl RangedSnapshot for SwimNodeStore {
    type Prefix = LaneKey;

    fn ranged_snapshot<F, K, V>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let prefix = match prefix {
            LaneKey::Map { lane_uri, .. } => StoreKey::Map(MapStorageKey {
                node_uri: self.node_uri.clone(),
                lane_uri,
                key: None,
            }),
            LaneKey::Value { lane_uri } => StoreKey::Value(ValueStorageKey {
                node_uri: self.node_uri.clone(),
                lane_uri,
            }),
        };

        self.delegate.ranged_snapshot(prefix, map_fn)
    }
}

impl Clone for SwimNodeStore {
    fn clone(&self) -> Self {
        SwimNodeStore {
            delegate: self.delegate.clone(),
            node_uri: self.node_uri.clone(),
        }
    }
}

impl SwimNodeStore {
    /// Create a new Swim node store which will delegate its engine operations to `delegate` and
    /// represents a node at `node_uri`.
    pub fn new(delegate: SwimPlaneStore, node_uri: String) -> SwimNodeStore {
        SwimNodeStore {
            delegate: Arc::new(delegate),
            node_uri: Arc::new(node_uri),
        }
    }
}

impl NodeStore for SwimNodeStore {
    fn map_lane_store<I, K, V>(&self, lane: I, transient: bool) -> MapDataModel<K, V>
    where
        I: ToString,
        K: Serialize,
        V: Serialize,
    {
        MapDataModel::new(self.clone(), lane.to_string(), transient)
    }

    fn value_lane_store<I, V>(&self, lane: I, transient: bool) -> ValueDataModel
    where
        I: ToString,
        V: Serialize,
    {
        ValueDataModel::new(self.clone(), lane.to_string(), transient)
    }
}

fn map_key(lane_key: LaneKey, node_uri: Arc<String>) -> StoreKey {
    match lane_key {
        LaneKey::Map { lane_uri, key } => StoreKey::Map(MapStorageKey {
            node_uri,
            lane_uri,
            key,
        }),
        LaneKey::Value { lane_uri } => StoreKey::Value(ValueStorageKey { node_uri, lane_uri }),
    }
}

impl<'a> StoreEngine<'a> for SwimNodeStore {
    type Key = LaneKey;
    type Value = Vec<u8>;
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let SwimNodeStore { delegate, node_uri } = self;
        let key = map_key(key, node_uri.clone());

        delegate.put(key, value)
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let SwimNodeStore { delegate, node_uri } = self;
        let key = map_key(key, node_uri.clone());

        delegate.get(key)
    }

    fn delete(&self, key: Self::Key) -> Result<(), Self::Error> {
        let SwimNodeStore { delegate, node_uri } = self;
        let key = map_key(key, node_uri.clone());

        delegate.delete(key)
    }
}
