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

use crate::agent::model::map::map_store::MapDataModel;
use crate::agent::store::NodeStore;
use crate::plane::store::mock::MockPlaneStore;
use crate::store::{StoreEngine, StoreKey};
use futures::future::{ready, BoxFuture};
use futures::FutureExt;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use store::engines::KeyedSnapshot;
use store::keyspaces::{KeyType, Keyspace, KeyspaceRangedSnapshotLoad};
use store::{serialize, Snapshot, StoreError, StoreInfo};
use utilities::sync::trigger;

#[derive(Debug, Clone)]
struct TrackingMapStore {
    load_tx: Arc<Mutex<Option<trigger::Sender>>>,
    values: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
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
        prefix: &[u8],
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
        S: Keyspace,
    {
        let guard = self.values.lock().unwrap();
        let mut entries = guard.deref().clone().into_iter();
        let mapped = entries.try_fold(Vec::new(), |mut entries, (k, v)| {
            if k.starts_with(prefix) {
                match map_fn(k.as_ref(), v.as_ref()) {
                    Ok((k, v)) => {
                        entries.push((k, v));
                        Ok(entries)
                    }
                    Err(e) => Err(e),
                }
            } else {
                Ok(entries)
            }
        })?;

        if mapped.is_empty() {
            Ok(None)
        } else {
            Ok(Some(KeyedSnapshot::new(mapped.into_iter())))
        }
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
fn empty_snapshot() {
    let values = Arc::new(Mutex::new(BTreeMap::new()));
    let loaded = Arc::new(Mutex::new(None));
    let store = TrackingMapStore {
        load_tx: loaded,
        values,
    };

    let model = MapDataModel::<TrackingMapStore, i32, i32>::new(store, 0);
    match model.snapshot() {
        Ok(None) => {}
        r => panic!("Expected Ok(None). Found: `{:?}`", r),
    }
}

fn make_store_key(lane_id: KeyType, key: String) -> StoreKey {
    StoreKey::Map {
        lane_id,
        key: Some(serialize(&key).expect("Failed to serialize store key")),
    }
}

#[test]
fn model_snapshot_single_id() {
    let mut expected = BTreeMap::new();
    expected.insert("a".to_string(), 1);
    expected.insert("b".to_string(), 2);
    expected.insert("c".to_string(), 3);
    expected.insert("d".to_string(), 4);
    expected.insert("e".to_string(), 5);

    let serialized = expected
        .clone()
        .into_iter()
        .fold(BTreeMap::new(), |mut map, (k, v)| {
            let store_key = make_store_key(0, k);
            map.insert(serialize(&store_key).unwrap(), serialize(&v).unwrap());
            map
        });

    let values = Arc::new(Mutex::new(serialized));
    let loaded = Arc::new(Mutex::new(None));
    let store = TrackingMapStore {
        load_tx: loaded,
        values,
    };

    let model = MapDataModel::<TrackingMapStore, String, i32>::new(store, 0);
    match model.snapshot() {
        Ok(Some(ss)) => {
            let values_map: BTreeMap<String, i32> = ss.into_iter().collect();
            assert_eq!(values_map, expected)
        }
        r => panic!("Expected Ok(Some(_)). Found: `{:?}`", r),
    }
}

#[test]
fn model_snapshot_multiple_ids() {
    let mut expected_id_0 = BTreeMap::new();
    expected_id_0.insert("a".to_string(), 1);
    expected_id_0.insert("b".to_string(), 2);
    expected_id_0.insert("c".to_string(), 3);
    expected_id_0.insert("d".to_string(), 4);
    expected_id_0.insert("e".to_string(), 5);

    let mut expected_id_1 = BTreeMap::new();
    expected_id_1.insert("f".to_string(), 6);
    expected_id_1.insert("g".to_string(), 7);
    expected_id_1.insert("h".to_string(), 8);
    expected_id_1.insert("i".to_string(), 9);
    expected_id_1.insert("j".to_string(), 10);

    let mut serialized_id_0 =
        expected_id_0
            .clone()
            .into_iter()
            .fold(BTreeMap::new(), |mut map, (k, v)| {
                let store_key = make_store_key(0, k);
                map.insert(serialize(&store_key).unwrap(), serialize(&v).unwrap());
                map
            });

    let serialized_id_1 = expected_id_1
        .into_iter()
        .fold(BTreeMap::new(), |mut map, (k, v)| {
            let store_key = make_store_key(1, k);
            map.insert(serialize(&store_key).unwrap(), serialize(&v).unwrap());
            map
        });

    serialized_id_0.extend(serialized_id_1);

    let values = Arc::new(Mutex::new(serialized_id_0));
    let loaded = Arc::new(Mutex::new(None));
    let store = TrackingMapStore {
        load_tx: loaded,
        values,
    };

    let model = MapDataModel::<TrackingMapStore, String, i32>::new(store, 0);
    match model.snapshot() {
        Ok(Some(ss)) => {
            let values_map: BTreeMap<String, i32> = ss.into_iter().collect();
            assert_eq!(values_map, expected_id_0)
        }
        r => panic!("Expected Ok(Some(_)). Found: `{:?}`", r),
    }
}

#[test]
fn model_crud() {
    let values = Arc::new(Mutex::new(BTreeMap::new()));
    let loaded = Arc::new(Mutex::new(None));
    let store = TrackingMapStore {
        load_tx: loaded,
        values: values.clone(),
    };
    let model = MapDataModel::<TrackingMapStore, String, i32>::new(store, 0);

    let mut to_load = BTreeMap::new();
    to_load.insert("a".to_string(), 1);
    to_load.insert("b".to_string(), 2);
    to_load.insert("c".to_string(), 3);
    to_load.insert("d".to_string(), 4);
    to_load.insert("e".to_string(), 5);

    to_load.iter().for_each(|(k, v)| {
        let store_result = model.put(k, v);
        assert!(store_result.is_ok());
    });

    let store_result = model.put(&"b".to_string(), &13);
    assert!(store_result.is_ok());

    let store_result = model.delete(&"e".to_string());
    assert!(store_result.is_ok());

    let mut expected = BTreeMap::new();
    expected.insert("a".to_string(), 1);
    expected.insert("b".to_string(), 13);
    expected.insert("c".to_string(), 3);
    expected.insert("d".to_string(), 4);

    match model.snapshot() {
        Ok(Some(ss)) => {
            let values_map: BTreeMap<String, i32> = ss.into_iter().collect();
            assert_eq!(values_map, expected)
        }
        r => panic!("Expected Ok(Some(_)). Found: `{:?}`", r),
    }
}
