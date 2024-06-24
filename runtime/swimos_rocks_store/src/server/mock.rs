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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Mutex;

use swimos_api::error::StoreError;

use crate::keyspaces::{Keyspace, KeyspaceByteEngine};
use crate::nostore::NoRange;
use crate::server::keystore::STEP;
use crate::utils::{deserialize_u64, serialize_u64_vec};

type Keyspaces = HashMap<String, HashMap<Vec<u8>, Vec<u8>>>;

pub struct MockStore {
    values: Mutex<Keyspaces>,
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
    type RangeCon<'a> = NoRange
    where
        Self: 'a;

    fn get_prefix_range_consumer<'a, S>(
        &'a self,
        _keyspace: S,
        _prefix: &[u8],
    ) -> Result<Self::RangeCon<'a>, StoreError>
    where
        S: Keyspace,
    {
        Ok(NoRange)
    }

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
                let mut value = deserialize_u64(entry.get()).unwrap();
                value += step;
                *entry.get_mut() = serialize_u64_vec(value);
                Ok(())
            }
            Entry::Vacant(entry) => {
                entry.insert(serialize_u64_vec(STEP));
                Ok(())
            }
        }
    }

    fn delete_key_range<S>(
        &self,
        keyspace: S,
        start: &[u8],
        ubound: &[u8],
    ) -> Result<(), StoreError>
    where
        S: Keyspace,
    {
        let mut guard = self.values.lock().unwrap();
        let keyspace = guard
            .get_mut(keyspace.name())
            .ok_or(StoreError::KeyspaceNotFound)?;
        *keyspace = std::mem::take(keyspace)
            .into_iter()
            .filter(|(key, _)| !((start <= key.as_slice()) && (key.as_slice() < ubound)))
            .collect();
        Ok(())
    }
}
