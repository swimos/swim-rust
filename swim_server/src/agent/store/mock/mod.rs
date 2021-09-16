// Copyright 2015-2021 SWIM.AI inc.
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

use crate::agent::store::{NodeStore, SwimNodeStore};
use crate::plane::store::mock::MockPlaneStore;
use crate::store::{StoreEngine, StoreKey};
use futures::future::{ready, BoxFuture};
use futures::FutureExt;
use store::{EngineInfo, StoreError};

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

    fn engine_info(&self) -> EngineInfo {
        EngineInfo {
            path: "".to_string(),
            kind: "Mock".to_string(),
        }
    }

    fn lane_id_of<I>(&self, _lane: I) -> BoxFuture<u64>
    where
        I: Into<String>,
    {
        ready(0).boxed()
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
