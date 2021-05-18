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

use crate::agent::store::NodeStore;
use crate::plane::store::mock::MockPlaneStore;
use crate::store::{StoreEngine, StoreKey};
use futures::future::{ready, BoxFuture};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use store::engines::KeyedSnapshot;
use store::keyspaces::{Keyspace, KeyspaceRangedSnapshotLoad};
use store::{serialize, StoreError, StoreInfo};
use utilities::sync::trigger;

#[derive(Debug, Clone)]
struct TrackingMapStore {
    load_tx: Arc<Mutex<Option<trigger::Sender>>>,
    values: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl NodeStore for TrackingMapStore {
    type Delegate = MockPlaneStore;

    fn store_info(&self) -> StoreInfo {
        StoreInfo {
            path: "tracking".to_string(),
            kind: "tracking".to_string(),
        }
    }

    fn lane_id_of<I>(&self, _lane: I) -> BoxFuture<u64>
    where
        I: Into<String>,
    {
        ready(0).boxed()
    }
}

impl KeyspaceRangedSnapshotLoad for TrackingMapStore {
    fn keyspace_load_ranged_snapshot<F, K, V, S>(
        &self,
        _keyspace: &S,
        _prefix: &[u8],
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
        S: Keyspace,
    {
        panic!("Unexpected snapshot request")
    }
}

impl StoreEngine for TrackingMapStore {
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        match key {
            k @ StoreKey::Map { .. } => {
                let mut guard = self.values.lock().unwrap();
                guard.insert(serialize(&k)?, value.to_vec());
                Ok(())
            }
            StoreKey::Value { .. } => {
                panic!("Expected a map key")
            }
        }
    }

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        match key {
            k @ StoreKey::Map { .. } => {
                let guard = self.values.lock().unwrap();
                Ok(guard.get(serialize(&k)?.as_slice()).map(|o| o.clone()))
            }
            StoreKey::Value { .. } => {
                panic!("Expected a map key")
            }
        }
    }

    fn delete(&self, key: StoreKey) -> Result<(), StoreError> {
        match key {
            k @ StoreKey::Map { .. } => {
                let mut guard = self.values.lock().unwrap();
                guard.remove(serialize(&k)?.as_slice());
                Ok(())
            }
            StoreKey::Value { .. } => {
                panic!("Expected a map key")
            }
        }
    }
}

#[test]
fn t() {}
