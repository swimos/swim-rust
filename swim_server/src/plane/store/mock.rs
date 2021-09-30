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

use crate::agent::store::mock::MockNodeStore;
use crate::agent::store::SwimNodeStore;
use crate::plane::store::PlaneStore;
use crate::store::{StoreEngine, StoreKey};
use store::keyspaces::{Keyspace, KeyspaceResolver};
use store::{EngineInfo, StoreError};
use swim_model::Text;

#[derive(Clone, Debug)]
pub struct MockPlaneStore;
impl PlaneStore for MockPlaneStore {
    type NodeStore = SwimNodeStore<MockPlaneStore>;

    fn node_store<I>(&self, _node: I) -> Self::NodeStore
    where
        I: Into<Text>,
    {
        MockNodeStore::mock()
    }

    fn get_prefix_range<F, K, V>(
        &self,
        _prefix: StoreKey,
        _map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        Ok(None)
    }

    fn engine_info(&self) -> EngineInfo {
        EngineInfo {
            path: "".to_string(),
            kind: "Mock".to_string(),
        }
    }

    fn node_id_of<I>(&self, _lane: I) -> Result<u64, StoreError>
    where
        I: Into<String>,
    {
        Ok(0)
    }
}

impl KeyspaceResolver for MockPlaneStore {
    type ResolvedKeyspace = ();

    fn resolve_keyspace<K: Keyspace>(&self, _space: &K) -> Option<&Self::ResolvedKeyspace> {
        None
    }
}

impl StoreEngine for MockPlaneStore {
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
