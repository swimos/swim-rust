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

pub mod lane;
pub mod mock;

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use bytes::BytesMut;
use swim_api::store::{MapPersistence, NodePersistenceBase, PlanePersistence};
use swim_model::Text;

use crate::plane::{PlaneStore, PrefixPlaneStore};
use crate::server::{StoreEngine, StoreKey};

use swim_store::{EngineInfo, RangeConsumer, StoreError};

pub trait PrefixNodeStore<'a> {
    type RangeCon: RangeConsumer + Send + 'a;

    /// Executes a ranged snapshot read prefixed by a lane key.
    ///
    /// #Arguments
    /// * `prefix` - Common prefix for the records to read.
    fn ranged_snapshot_consumer(&'a self, prefix: StoreKey) -> Result<Self::RangeCon, StoreError>;
}

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
pub trait NodeStore:
    for<'a> PrefixNodeStore<'a> + StoreEngine + Send + Sync + Clone + Debug + 'static
{
    type Delegate: PlaneStore;

    /// Returns information about the delegate store
    fn engine_info(&self) -> EngineInfo;

    fn lane_id_of(&self, lane: &str) -> Result<u64, StoreError>;

    /// Executes a ranged snapshot read prefixed by a lane key and deserialize each key-value pair
    /// using `map_fn`.
    ///
    /// Returns an optional snapshot iterator if entries were found that will yield deserialized
    /// key-value pairs.
    fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: StoreKey,
        map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>;

    /// Delete all values for a map lane.
    fn delete_map(&self, lane_id: u64) -> Result<(), StoreError>;
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
}

impl<D: PlaneStore> StoreEngine for SwimNodeStore<D> {
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        self.delegate.put(key, value)
    }

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        self.delegate.get(key)
    }

    fn delete(&self, key: StoreKey) -> Result<(), StoreError> {
        self.delegate.delete(key)
    }
}

impl<'a, D: PrefixPlaneStore<'a>> PrefixNodeStore<'a> for SwimNodeStore<D> {
    type RangeCon = D::RangeCon;

    fn ranged_snapshot_consumer(&'a self, prefix: StoreKey) -> Result<Self::RangeCon, StoreError> {
        self.delegate.ranged_snapshot_consumer(prefix)
    }
}

impl<D: PlaneStore> NodeStore for SwimNodeStore<D> {
    type Delegate = D;

    fn engine_info(&self) -> EngineInfo {
        self.delegate.engine_info()
    }

    fn lane_id_of(&self, lane: &str) -> Result<u64, StoreError> {
        let node_id = format!("{}/{}", self.node_uri, lane);
        self.delegate.node_id_of(node_id)
    }

    fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: StoreKey,
        map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        self.delegate.get_prefix_range(prefix, map_fn)
    }

    fn delete_map(&self, lane_id: u64) -> Result<(), StoreError> {
        self.delegate.delete_map(lane_id)
    }
}

#[derive(Debug, Clone)]
pub struct StoreWrapper<S>(pub S);

impl<'a, S> MapPersistence<'a> for StoreWrapper<S>
where
    S: NodeStore,
{
    type MapCon = <S as PrefixNodeStore<'a>>::RangeCon;

    fn read_map(&'a self, lane_id: Self::LaneId) -> Result<Self::MapCon, StoreError> {
        let StoreWrapper(store) = self;
        let key = StoreKey::Map { lane_id, key: None };
        store.ranged_snapshot_consumer(key)
    }
}

impl<S> PlanePersistence for StoreWrapper<S>
where
    S: PlaneStore,
{
    type Node = StoreWrapper<S::NodeStore>;

    fn node_store(&mut self, node_uri: &str) -> Result<Self::Node, StoreError> {
        let StoreWrapper(inner) = self;
        Ok(StoreWrapper(inner.node_store(node_uri)))
    }
}

impl<S> NodePersistenceBase for StoreWrapper<S>
where
    S: NodeStore,
{
    type LaneId = u64;

    fn id_for(&self, name: &str) -> Result<Self::LaneId, StoreError> {
        let StoreWrapper(store) = self;
        store.lane_id_of(name)
    }

    fn get_value(
        &self,
        lane_id: Self::LaneId,
        buffer: &mut BytesMut,
    ) -> Result<Option<usize>, StoreError> {
        let StoreWrapper(store) = self;
        if let Some(bytes) = store.get(StoreKey::Value { lane_id })? {
            let n = bytes.len();
            buffer.extend_from_slice(bytes.as_ref());
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    fn put_value(&self, lane_id: Self::LaneId, value: &[u8]) -> Result<(), StoreError> {
        let StoreWrapper(store) = self;
        store.put(StoreKey::Value { lane_id }, value)
    }

    fn update_map(
        &self,
        lane_id: Self::LaneId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        let StoreWrapper(store) = self;
        let key = StoreKey::Map {
            lane_id,
            key: Some(key.to_owned()),
        };
        store.put(key, value)
    }

    fn remove_map(&self, lane_id: Self::LaneId, key: &[u8]) -> Result<(), StoreError> {
        let StoreWrapper(store) = self;
        let key = StoreKey::Map {
            lane_id,
            key: Some(key.to_owned()),
        };
        store.delete(key)
    }

    fn clear(&self, id: Self::LaneId) -> Result<(), StoreError> {
        let StoreWrapper(store) = self;
        store.delete_map(id)
    }
}
