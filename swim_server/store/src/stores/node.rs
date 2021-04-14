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
use crate::stores::{MapStorageKey, StoreKey, ValueStorageKey};
use crate::{KeyedSnapshot, PlaneStore, RangedSnapshotLoad, StoreEngine, StoreError};
use serde::Serialize;
use std::fmt::Debug;
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
pub trait NodeStore: for<'a> StoreEngine<'a> + Send + Sync + Clone + Debug {
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
        transient: bool,
    ) -> MapDataModel<Self::Delegate, K, V>
    where
        I: Into<Text>,
        K: Serialize,
        V: Serialize,
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
        transient: bool,
    ) -> ValueDataModel<Self::Delegate>
    where
        I: Into<Text>,
        V: Serialize,
        Self: Sized;
}

/// A node store which is used to open value and map lane data models.
#[derive(Debug)]
pub struct SwimNodeStore<D> {
    /// The plane store that value and map data models will delegate their store engine operations
    /// to.
    delegate: Arc<D>,
    /// The node URI that this store represents.
    node_uri: Text,
}

impl<D> RangedSnapshotLoad for SwimNodeStore<D>
where
    D: PlaneStore<Prefix = StoreKey>,
{
    type Prefix = LaneKey;

    fn load_ranged_snapshot<F, K, V>(
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

        self.delegate.load_ranged_snapshot(prefix, map_fn)
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

impl<D> SwimNodeStore<D> {
    /// Create a new Swim node store which will delegate its engine operations to `delegate` and
    /// represents a node at `node_uri`.
    pub fn new<I: Into<Text>>(delegate: D, node_uri: I) -> SwimNodeStore<D> {
        SwimNodeStore {
            delegate: Arc::new(delegate),
            node_uri: node_uri.into(),
        }
    }
}

impl<D> NodeStore for SwimNodeStore<D>
where
    D: PlaneStore,
{
    type Delegate = D;

    fn map_lane_store<I, K, V>(
        &self,
        lane: I,
        transient: bool,
    ) -> MapDataModel<Self::Delegate, K, V>
    where
        I: Into<Text>,
        K: Serialize,
        V: Serialize,
    {
        MapDataModel::new(self.clone(), lane, transient)
    }

    fn value_lane_store<I, V>(&self, lane: I, transient: bool) -> ValueDataModel<Self::Delegate>
    where
        I: Into<Text>,
        V: Serialize,
    {
        ValueDataModel::new(self.clone(), lane, transient)
    }
}

fn map_key(lane_key: LaneKey, node_uri: Text) -> StoreKey {
    match lane_key {
        LaneKey::Map { lane_uri, key } => StoreKey::Map(MapStorageKey {
            node_uri,
            lane_uri,
            key,
        }),
        LaneKey::Value { lane_uri } => StoreKey::Value(ValueStorageKey { node_uri, lane_uri }),
    }
}

impl<'a, D> StoreEngine<'a> for SwimNodeStore<D>
where
    D: PlaneStore<Key = StoreKey>,
{
    type Key = LaneKey;

    fn put(&self, key: Self::Key, value: Vec<u8>) -> Result<(), StoreError> {
        let SwimNodeStore { delegate, node_uri } = self;
        let key = map_key(key, node_uri.clone());

        delegate.put(key, value)
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, StoreError> {
        let SwimNodeStore { delegate, node_uri } = self;
        let key = map_key(key, node_uri.clone());

        delegate.get(key)
    }

    fn delete(&self, key: Self::Key) -> Result<(), StoreError> {
        let SwimNodeStore { delegate, node_uri } = self;
        let key = map_key(key, node_uri.clone());

        delegate.delete(key)
    }
}
