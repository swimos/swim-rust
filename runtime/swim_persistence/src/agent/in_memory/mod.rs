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

use swim_api::store::{PlanePersistence, NodePersistence};
use swim_store::RangeConsumer;

pub struct InMemoryPlanePersistence {

}

#[derive(Clone)]
pub struct InMemoryNodePersistence {

}

impl PlanePersistence for InMemoryPlanePersistence {
    type Node = InMemoryNodePersistence;

    fn node_store(&self, node_uri: &str) -> Result<Self::Node, swim_store::StoreError> {
        todo!()
    }
}

pub struct InMemRangeConsumer;

impl RangeConsumer for InMemRangeConsumer {
    fn consume_next(&mut self) -> Result<Option<swim_store::KeyValue<'_>>, swim_store::StoreError> {
        todo!()
    }
}

impl NodePersistence for InMemoryNodePersistence {
    type MapCon<'a> = InMemRangeConsumer;

    type LaneId = u64;

    fn id_for(&self, name: &str) -> Result<Self::LaneId, swim_store::StoreError> {
        todo!()
    }

    fn get_value(
        &self,
        id: Self::LaneId,
        buffer: &mut bytes::BytesMut,
    ) -> Result<Option<usize>, swim_store::StoreError> {
        todo!()
    }

    fn put_value(&self, id: Self::LaneId, value: &[u8]) -> Result<(), swim_store::StoreError> {
        todo!()
    }

    fn delete_value(&self, id: Self::LaneId) -> Result<(), swim_store::StoreError> {
        todo!()
    }

    fn update_map(&self, id: Self::LaneId, key: &[u8], value: &[u8]) -> Result<(), swim_store::StoreError> {
        todo!()
    }

    fn remove_map(&self, id: Self::LaneId, key: &[u8]) -> Result<(), swim_store::StoreError> {
        todo!()
    }

    fn clear_map(&self, id: Self::LaneId) -> Result<(), swim_store::StoreError> {
        todo!()
    }

    fn read_map(&self, id: Self::LaneId) -> Result<Self::MapCon<'_>, swim_store::StoreError> {
        todo!()
    }
}