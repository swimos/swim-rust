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

use crate::error::StoreError;
use std::fmt::{Display, Formatter};

use bytes::BytesMut;

/// Kinds of stores that can be persisted in the state of an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    /// A store containing a single value.
    Value,
    /// A store consisting of a map from keys to values.
    Map,
}

impl Display for StoreKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreKind::Value => write!(f, "Value"),
            StoreKind::Map => write!(f, "Map"),
        }
    }
}

/// Defines that operations that must be provided for a store implementation that allows
/// a Swim agent to persist its state.
pub trait NodePersistence: NodePersistenceBase + for<'a> MapPersistence<'a> {}

impl<P> NodePersistence for P where P: NodePersistenceBase + for<'a> MapPersistence<'a> {}

/// Operations for [`NodePersistence`] that do not produce a long lived borrow of the store.
pub trait NodePersistenceBase {
    /// The store assigns IDs of this type to each named state in the store.
    type LaneId: Copy + Unpin + Send + Sync + Eq + 'static;

    /// Get the ID associatd with the specified name.
    fn id_for(&self, name: &str) -> Result<Self::LaneId, StoreError>;

    /// Attempt to copy the single value associated with an ID into the provided buffer. The value associated
    /// with the id should be copied into the buffer (leaving any existing content intact) and the number
    /// of bytes copied should be returned. If no data is associated with the key the result should be valid
    /// and undefined.
    fn get_value(
        &self,
        id: Self::LaneId,
        buffer: &mut BytesMut,
    ) -> Result<Option<usize>, StoreError>;

    /// Write a new value into the store for the specified ID.
    fn put_value(&self, id: Self::LaneId, value: &[u8]) -> Result<(), StoreError>;

    /// Remove a value in the store for the associated ID. If no value is present, this operation
    /// should still succeed.
    fn delete_value(&self, id: Self::LaneId) -> Result<(), StoreError>;

    /// Update the value associated with a specific key in a map in the store.
    fn update_map(&self, id: Self::LaneId, key: &[u8], value: &[u8]) -> Result<(), StoreError>;

    /// Remove an entry with the specified key from a map in the store. If the map exists but
    /// has no entry with the key, this should succeed.
    fn remove_map(&self, id: Self::LaneId, key: &[u8]) -> Result<(), StoreError>;

    /// Clear all entries for a map in the store.
    fn clear_map(&self, id: Self::LaneId) -> Result<(), StoreError>;
}

/// View of a entry from a map in a store.
pub type KeyValue<'a> = (&'a [u8], &'a [u8]);

/// Enumerates the entries of a map from a store. This cannot be an iterator as the entries
/// can borrow from the consumer.
pub trait RangeConsumer {
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError>;
}

pub trait MapPersistence<'a>: NodePersistenceBase {
    type MapCon: RangeConsumer + Send + 'a;

    /// Produce a [`RangeConsumer`] that will enumerate the entries for a map in the store.
    fn read_map(&'a self, id: Self::LaneId) -> Result<Self::MapCon, StoreError>;
}

/// Implementors of this trait can produce a family of independent stores, keyed by name, for
/// any agent within a single plane.
pub trait PlanePersistence {
    type Node: NodePersistence + Clone + Send + Sync + 'static;

    ///Attempt to open or create a store for an agent at the specified URI.
    fn node_store(&self, node_uri: &str) -> Result<Self::Node, StoreError>;
}

/// A dummy store implementation for when no peristence is required.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreDisabled;

impl RangeConsumer for StoreDisabled {
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError> {
        Ok(None)
    }
}

impl<'a> MapPersistence<'a> for StoreDisabled {
    type MapCon = StoreDisabled;

    fn read_map(&'a self, _id: Self::LaneId) -> Result<Self::MapCon, StoreError> {
        Ok(StoreDisabled)
    }
}

impl NodePersistenceBase for StoreDisabled {
    type LaneId = ();

    fn id_for(&self, _name: &str) -> Result<Self::LaneId, StoreError> {
        Ok(())
    }

    fn get_value(
        &self,
        _id: Self::LaneId,
        _buffer: &mut BytesMut,
    ) -> Result<Option<usize>, StoreError> {
        Ok(None)
    }

    fn put_value(&self, _id: Self::LaneId, _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn update_map(&self, _id: Self::LaneId, _key: &[u8], _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn remove_map(&self, _id: Self::LaneId, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn clear_map(&self, _id: Self::LaneId) -> Result<(), StoreError> {
        Ok(())
    }

    fn delete_value(&self, _id: Self::LaneId) -> Result<(), StoreError> {
        Ok(())
    }
}

impl PlanePersistence for StoreDisabled {
    type Node = StoreDisabled;

    fn node_store(&self, _node_uri: &str) -> Result<Self::Node, StoreError> {
        Ok(StoreDisabled)
    }
}
