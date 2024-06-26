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

use crate::agent::mock::MockNodeStore;
use crate::agent::SwimNodeStore;
use crate::keyspaces::{Keyspace, KeyspaceResolver};
use crate::nostore::NoRange;
use crate::plane::PlaneStore;
use crate::server::{StoreEngine, StoreKey};
use swimos_api::error::StoreError;
use swimos_model::Text;

#[derive(Clone, Debug)]
pub struct MockPlaneStore;
impl PlaneStore for MockPlaneStore {
    type NodeStore = SwimNodeStore<MockPlaneStore>;

    type RangeCon<'a> = NoRange
    where
        Self: 'a;

    fn ranged_snapshot_consumer(
        &self,
        _prefix: StoreKey,
    ) -> Result<Self::RangeCon<'_>, StoreError> {
        Ok(NoRange)
    }

    fn node_store<I>(&self, _node: I) -> Self::NodeStore
    where
        I: Into<Text>,
    {
        MockNodeStore::mock()
    }

    fn node_id_of<I>(&self, _lane: I) -> Result<u64, StoreError>
    where
        I: Into<String>,
    {
        Ok(0)
    }

    fn delete_map(&self, _lane_id: u64) -> Result<(), StoreError> {
        Ok(())
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
