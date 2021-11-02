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

use crate::agent::lane::model::value::ValueDataModel;
use crate::agent::NodeStore;
use crate::plane::mock::MockPlaneStore;
use crate::server::{StoreEngine, StoreKey};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use swim_store::{serialize, EngineInfo, StoreError};

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

    fn load_ranged_snapshot<F, K, V>(
        &self,
        _prefix: StoreKey,
        _map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
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
