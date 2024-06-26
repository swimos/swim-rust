// Copyright 2015-2024 Swim Inc.
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

#[cfg(test)]
pub mod mock;

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use bytes::BytesMut;
use futures::future::{ready, BoxFuture};
use futures::FutureExt;
use swimos_api::error::StoreError;
use swimos_api::persistence::{NodePersistence, PlanePersistence, RangeConsumer};
use swimos_model::Text;

use crate::plane::PlaneStore;
use crate::server::{StoreEngine, StoreKey};

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
pub trait NodeStore: StoreEngine + Send + Sync + Clone + Debug + 'static {
    type Delegate: PlaneStore;

    type RangeCon<'a>: RangeConsumer + Send + 'a
    where
        Self: 'a;

    /// Executes a ranged snapshot read prefixed by a lane key.
    ///
    /// # Arguments
    /// * `prefix` - Common prefix for the records to read.
    fn ranged_snapshot_consumer(&self, prefix: StoreKey) -> Result<Self::RangeCon<'_>, StoreError>;

    fn lane_id_of(&self, lane: &str) -> Result<u64, StoreError>;

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

impl<D: PlaneStore> NodeStore for SwimNodeStore<D> {
    type Delegate = D;

    type RangeCon<'a> = D::RangeCon<'a>
    where
        Self: 'a;

    fn ranged_snapshot_consumer(&self, prefix: StoreKey) -> Result<Self::RangeCon<'_>, StoreError> {
        self.delegate.ranged_snapshot_consumer(prefix)
    }

    fn lane_id_of(&self, lane: &str) -> Result<u64, StoreError> {
        let node_id = format!("{}/{}", self.node_uri, lane);
        self.delegate.node_id_of(node_id)
    }

    fn delete_map(&self, lane_id: u64) -> Result<(), StoreError> {
        self.delegate.delete_map(lane_id)
    }
}

/// Wrapper type to expose the stores defined in the swimos_persistence crate as the interface
/// specified in swimos_api.
/// TODO: This should be unnecessary after the prototype server is removed.
#[derive(Debug, Clone)]
pub struct StoreWrapper<S>(pub S);

impl<S> PlanePersistence for StoreWrapper<S>
where
    S: PlaneStore,
{
    type Node = StoreWrapper<S::NodeStore>;

    fn node_store(&self, node_uri: &str) -> BoxFuture<'static, Result<Self::Node, StoreError>> {
        let StoreWrapper(inner) = self;
        ready(Ok(StoreWrapper(inner.node_store(node_uri)))).boxed()
    }
}

impl<S> NodePersistence for StoreWrapper<S>
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

    fn put_value(&mut self, lane_id: Self::LaneId, value: &[u8]) -> Result<(), StoreError> {
        let StoreWrapper(store) = self;
        store.put(StoreKey::Value { lane_id }, value)
    }

    fn update_map(
        &mut self,
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

    fn remove_map(&mut self, lane_id: Self::LaneId, key: &[u8]) -> Result<(), StoreError> {
        let StoreWrapper(store) = self;
        let key = StoreKey::Map {
            lane_id,
            key: Some(key.to_owned()),
        };
        store.delete(key)
    }

    fn clear_map(&mut self, id: Self::LaneId) -> Result<(), StoreError> {
        let StoreWrapper(store) = self;
        store.delete_map(id)
    }

    fn delete_value(&mut self, lane_id: Self::LaneId) -> Result<(), StoreError> {
        let StoreWrapper(store) = self;
        store.delete(StoreKey::Value { lane_id })
    }

    type MapCon<'a> = <S as NodeStore>::RangeCon<'a>
    where
        Self: 'a;

    fn read_map(&self, lane_id: Self::LaneId) -> Result<Self::MapCon<'_>, StoreError> {
        let StoreWrapper(store) = self;
        let key = StoreKey::Map { lane_id, key: None };
        store.ranged_snapshot_consumer(key)
    }
}
