// Copyright 2015-2023 Swim Inc.
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

use swimos_api::error::StoreError;

use crate::agent::{NodeStore, SwimNodeStore};
use crate::nostore::NoRange;
use crate::plane::mock::MockPlaneStore;
use crate::server::{StoreEngine, StoreKey};

#[derive(Clone, Debug)]
pub struct MockNodeStore {
    _priv: (),
}

impl MockNodeStore {
    pub fn mock() -> SwimNodeStore<MockPlaneStore> {
        let plane_store = MockPlaneStore;
        SwimNodeStore::new(plane_store, "test_node")
    }
}

impl NodeStore for MockNodeStore {
    type Delegate = MockPlaneStore;

    type RangeCon<'a> = NoRange
    where
        Self: 'a;

    fn ranged_snapshot_consumer(
        &self,
        _prefix: StoreKey,
    ) -> Result<Self::RangeCon<'_>, StoreError> {
        Ok(NoRange)
    }

    fn lane_id_of(&self, _lane: &str) -> Result<u64, StoreError> {
        Ok(0)
    }

    fn delete_map(&self, _lane_id: u64) -> Result<(), StoreError> {
        Ok(())
    }
}

impl StoreEngine for MockNodeStore {
    fn put(&self, _key: StoreKey, _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: StoreKey) -> Result<(), StoreError> {
        Ok(())
    }
}
