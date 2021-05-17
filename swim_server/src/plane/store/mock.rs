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
use crate::plane::store::PlaneStore;
use crate::store::keystore::{KeyRequest, KeystoreTask};
use crate::store::{StoreEngine, StoreKey};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use std::sync::Arc;
use store::keyspaces::{KeyType, KeyspaceByteEngine};
use store::{KeyedSnapshot, StoreError, StoreInfo};
use swim_common::model::text::Text;

#[derive(Clone, Debug)]
pub struct MockPlaneStore;
impl PlaneStore for MockPlaneStore {
    type NodeStore = MockNodeStore;

    fn node_store<I>(&self, _node: I) -> Self::NodeStore
    where
        I: Into<Text>,
    {
        MockNodeStore
    }

    fn load_ranged_snapshot<F, K, V>(
        &self,
        _prefix: StoreKey,
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        Ok(None)
    }

    fn store_info(&self) -> StoreInfo {
        StoreInfo {
            path: "".to_string(),
            kind: "Mock".to_string(),
        }
    }

    fn lane_id_of<I>(&self, _lane: I) -> BoxFuture<'static, KeyType>
    where
        I: Into<String>,
    {
        async { 0 }.boxed()
    }
}

impl KeystoreTask for MockPlaneStore {
    fn run<DB, S>(_db: Arc<DB>, _events: S) -> BoxFuture<'static, Result<(), StoreError>>
    where
        DB: KeyspaceByteEngine,
        S: Stream<Item = KeyRequest> + Unpin + Send + 'static,
    {
        Box::pin(async { Ok(()) })
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
