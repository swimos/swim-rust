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
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use store::{StoreError, StoreInfo};

#[derive(Clone, Debug)]
struct TrackingValueStore(Arc<Mutex<Option<Vec<u8>>>>);

impl NodeStore for TrackingValueStore {
    type Delegate = MockPlaneStore;

    fn store_info(&self) -> StoreInfo {
        panic!("Unexpected store information request")
    }

    fn lane_id_of<I>(&self, _lane: I) -> BoxFuture<u64>
    where
        I: Into<String>,
    {
        ready(0).boxed()
    }
}

impl StoreEngine for TrackingValueStore {
    fn put(&self, key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        self.0.lock().unwrap().replace(value.to_vec());
        Ok(())
    }

    fn get(&self, key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.0.lock().unwrap().clone())
    }

    fn delete(&self, key: StoreKey) -> Result<(), StoreError> {
        self.0.lock().unwrap().take();
        Ok(())
    }
}
