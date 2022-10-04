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
    type LaneId: Copy + Send + Sync + 'static;

    fn id_for(&self, name: &str) -> Result<Self::LaneId, StoreError>;

    fn get_value(
        &self,
        id: Self::LaneId,
        buffer: &mut BytesMut,
    ) -> Result<Option<usize>, StoreError>;

    fn put_value(&self, id: Self::LaneId, value: &[u8]) -> Result<(), StoreError>;

    fn update_map(&self, id: Self::LaneId, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    fn remove_map(&self, id: Self::LaneId, key: &[u8]) -> Result<(), StoreError>;

    fn clear(&self, id: Self::LaneId) -> Result<(), StoreError>;
}

pub trait RangeConsumer {
    fn consume_next<'a>(&'a mut self) -> Result<Option<(&'a [u8], &'a [u8])>, StoreError>;
}

pub trait MapPersistence<'a>: NodePersistenceBase {
    type MapCon: RangeConsumer + 'a;

    fn read_map(&'a self, id: Self::LaneId) -> Result<Self::MapCon, StoreError>;
}

pub trait NodePersistence: NodePersistenceBase + for<'a> MapPersistence<'a> {}

impl<P> NodePersistence for P where P: NodePersistenceBase + for<'a> MapPersistence<'a> {}

pub trait PlanePersistence {
    type Node: NodePersistence;

    fn node_store(&mut self, node_uri: &str) -> Result<Self::Node, StoreError>;
}
