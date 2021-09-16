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

use crate::agent::lane::store::error::{StoreErrorHandler, StoreTaskError};
use crate::agent::lane::store::StoreIo;
use crate::agent::model::value::value_store::io::ValueLaneStoreIo;
use crate::agent::model::value::value_store::ValueDataModel;
use crate::agent::model::value::ValueLane;
use crate::agent::store::NodeStore;
use crate::plane::store::mock::MockPlaneStore;
use crate::store::{StoreEngine, StoreKey};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use store::engines::KeyedSnapshot;
use store::keyspaces::{Keyspace, KeyspaceRangedSnapshotLoad};
use store::{serialize, StoreError, StoreInfo};
use store::{serialize, EngineInfo, StoreError};
use utilities::sync::trigger;

#[derive(Clone, Debug)]
struct TrackingValueStore {
    value: Arc<Mutex<Option<Vec<u8>>>>,
}

impl NodeStore for TrackingValueStore {
    type Delegate = MockPlaneStore;

    fn engine_info(&self) -> EngineInfo {
        EngineInfo {
            path: "tracking".to_string(),
            kind: "tracking".to_string(),
        }
    }

    fn lane_id_of(&self, _lane: &str) -> Result<u64, StoreError> {
        Ok(0)
    }
}

impl KeyspaceRangedSnapshotLoad for TrackingValueStore {
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

impl StoreEngine for TrackingValueStore {
    fn put(&self, _key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        self.value.lock().unwrap().replace(value.to_vec());
        Ok(())
    }

    fn get(&self, _key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.value.lock().unwrap().clone())
    }

    fn delete(&self, _key: StoreKey) -> Result<(), StoreError> {
        self.value.lock().unwrap().take();
        Ok(())
    }
}

#[test]
fn load_some() {
    let test_string = "test".to_string();
    let arcd = Arc::new(Mutex::new(Some(serialize(&test_string).unwrap())));
    let store = TrackingValueStore { value: arcd };
    let model = ValueDataModel::<TrackingValueStore, String>::new(store, 0);

    match model.load() {
        Ok(Some(string)) => {
            assert_eq!(string, test_string);
        }
        v => {
            panic!("Invalid result returned: `{:?}`", v)
        }
    }
}

#[test]
fn load_none() {
    let store = TrackingValueStore {
        value: Arc::new(Mutex::new(None)),
    };

    let model = ValueDataModel::<TrackingValueStore, String>::new(store, 0);

    assert_eq!(model.load(), Ok(None));
}

#[test]
fn store_load() {
    let store = TrackingValueStore {
        value: Arc::new(Mutex::new(None)),
    };
    let model = ValueDataModel::<TrackingValueStore, String>::new(store, 0);
    let input = "hello".to_string();

    assert!(model.store(&input).is_ok());
    match model.load() {
        Ok(Some(string)) => {
            assert_eq!(string, input);
        }
        v => {
            panic!("Invalid result returned: `{:?}`", v)
        }
    }
}

#[tokio::test]
async fn io() {
    let store_initial = "loaded".to_string();
    let store = TrackingValueStore {
        value: Arc::new(Mutex::new(Some(serialize(&store_initial).unwrap()))),
    };

    let model = ValueDataModel::<TrackingValueStore, String>::new(store, 0);
    let (lane, observer) = ValueLane::<String>::store_observable(
        &model,
        NonZeroUsize::new(8).unwrap(),
        Default::default(),
    );
    let observer_stream = observer.into_stream();

    let store_io = ValueLaneStoreIo::new(observer_stream, model);
    let _task_handle = tokio::spawn(store_io.attach(StoreErrorHandler::new(0, info)));

    let lane_value = lane.load().await;
    assert_eq!(*lane_value, store_initial);
}

#[tokio::test]
async fn store_fail() {
    let (lane, observer) =
        ValueLane::observable("initial".to_string(), NonZeroUsize::new(8).unwrap());
    let observer_stream = observer.into_stream();
    let store = FailingStore;
    let info = store.store_info();
    let store = FailingStore {
        fail_on: FailingStoreMode::Put,
    };

    let model = ValueDataModel::new(store, 0);
    let store_io = ValueLaneStoreIo::new(observer_stream, model);

    lane.store("avro vulcan".to_string()).await;

    let task_result = store_io.attach(StoreErrorHandler::new(0, info)).await;
    let task_result = store_io
        .attach("test".to_string(), StoreErrorHandler::new(0))
        .await;
    match task_result {
        Ok(_) => {
            panic!("Expected a store error")
        }
        Err(mut r) => {
            let StoreTaskError { error, .. } = r.errors.pop().unwrap();
            assert_eq!(error, StoreError::Closing)
        }
    }
}

#[derive(Debug, Clone)]
struct FailingStore;

impl NodeStore for FailingStore {
    type Delegate = MockPlaneStore;

    fn engine_info(&self) -> EngineInfo {
        EngineInfo {
            path: "failing".to_string(),
            kind: "failing".to_string(),
        }
    }

    fn lane_id_of(&self, _lane: &str) -> Result<u64, StoreError> {
        Ok(0)
    }
}

impl KeyspaceRangedSnapshotLoad for FailingStore {
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

impl StoreEngine for FailingStore {
    fn put(&self, _: StoreKey, _: &[u8]) -> Result<(), StoreError> {
        Err(StoreError::Closing)
    }

    fn get(&self, _: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        Err(StoreError::Closing)
    }

    fn delete(&self, _: StoreKey) -> Result<(), StoreError> {
        Err(StoreError::Closing)
    }
}
