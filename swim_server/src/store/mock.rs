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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::store::keystore::{lane_key_task, KeyRequest, KeystoreTask, STEP};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use store::keyspaces::{Keyspace, KeyspaceByteEngine};
use store::{deserialize, serialize, StoreError};

pub struct MockStore {
    values: Mutex<HashMap<String, HashMap<Vec<u8>, Vec<u8>>>>,
}

impl KeystoreTask for MockStore {
    fn run<DB, S>(db: Arc<DB>, events: S) -> BoxFuture<'static, Result<(), StoreError>>
    where
        DB: KeyspaceByteEngine,
        S: Stream<Item = KeyRequest> + Unpin + Send + 'static,
    {
        lane_key_task(db, events).boxed()
    }
}

impl MockStore {
    pub fn with_keyspaces(keyspaces: Vec<String>) -> MockStore {
        let keyspaces = keyspaces.into_iter().fold(HashMap::new(), |mut map, name| {
            map.insert(name, HashMap::new());
            map
        });

        MockStore {
            values: Mutex::new(keyspaces),
        }
    }
}

impl KeyspaceByteEngine for MockStore {
    fn put_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        let mut guard = self.values.lock().unwrap();
        let keyspace = guard
            .get_mut(keyspace.name())
            .ok_or(StoreError::KeyspaceNotFound)?;
        keyspace.insert(key.to_vec(), value.to_vec());

        Ok(())
    }

    fn get_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let mut guard = self.values.lock().unwrap();
        let keyspace = guard
            .get_mut(keyspace.name())
            .ok_or(StoreError::KeyspaceNotFound)?;

        Ok(keyspace.get(key).cloned())
    }

    fn delete_keyspace<K: Keyspace>(&self, keyspace: K, key: &[u8]) -> Result<(), StoreError> {
        let mut guard = self.values.lock().unwrap();
        let keyspace = guard
            .get_mut(keyspace.name())
            .ok_or(StoreError::KeyspaceNotFound)?;
        keyspace.remove(key);

        Ok(())
    }

    fn merge_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
        step: u64,
    ) -> Result<(), StoreError> {
        let mut guard = self.values.lock().unwrap();
        let keyspace = guard
            .get_mut(keyspace.name())
            .ok_or(StoreError::KeyspaceNotFound)?;

        match keyspace.entry(key.to_vec()) {
            Entry::Occupied(mut entry) => {
                let mut value = deserialize::<u64>(entry.get()).unwrap();
                value += step;
                *entry.get_mut() = serialize(&value).unwrap();
                Ok(())
            }
            Entry::Vacant(entry) => {
                entry.insert(serialize(&STEP).unwrap());
                Ok(())
            }
        }
    }

    fn get_prefix_range<F, K, V, S>(
        &self,
        _keyspace: S,
        _prefix: &[u8],
        _map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
        S: Keyspace,
    {
        todo!()
    }
}
