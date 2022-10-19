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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    Value,
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

pub trait NodePersistenceBase {
    type LaneId: Copy + Unpin + Send + Sync + Eq + 'static;

    fn id_for(&self, name: &str) -> Result<Self::LaneId, StoreError>;

    fn get_value(
        &self,
        id: Self::LaneId,
        buffer: &mut BytesMut,
    ) -> Result<Option<usize>, StoreError>;

    fn put_value(&self, id: Self::LaneId, value: &[u8]) -> Result<(), StoreError>;
    fn delete_value(&self, id: Self::LaneId) -> Result<(), StoreError>;

    fn update_map(&self, id: Self::LaneId, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    fn remove_map(&self, id: Self::LaneId, key: &[u8]) -> Result<(), StoreError>;

    fn clear(&self, id: Self::LaneId) -> Result<(), StoreError>;
}

pub type KeyValue<'a> = (&'a [u8], &'a [u8]);

pub trait RangeConsumer {
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError>;
}

pub trait MapPersistence<'a>: NodePersistenceBase {
    type MapCon: RangeConsumer + Send + 'a;

    fn read_map(&'a self, id: Self::LaneId) -> Result<Self::MapCon, StoreError>;
}

pub trait NodePersistence: NodePersistenceBase + for<'a> MapPersistence<'a> {}

impl<P> NodePersistence for P where P: NodePersistenceBase + for<'a> MapPersistence<'a> {}

pub trait PlanePersistence {
    type Node: NodePersistence + Clone + Send + Sync + 'static;

    fn node_store(&self, node_uri: &str) -> Result<Self::Node, StoreError>;
}

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

    fn clear(&self, _id: Self::LaneId) -> Result<(), StoreError> {
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
