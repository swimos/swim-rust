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
use crate::store::StoreKey;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;

#[cfg(test)]
mod tests;

mod io;
pub use io::MapLaneStoreIo;
use swim_store::{deserialize, serialize, serialize_then, StoreError};

const INCONSISTENT_DB: &str = "Missing key or value in store";

pub struct MapDataModel<D, K, V> {
    delegate: D,
    lane_id: u64,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<D, K, V> MapDataModel<D, K, V> {
    pub fn new(delegate: D, lane_id: u64) -> Self {
        MapDataModel {
            delegate,
            lane_id,
            _key: Default::default(),
            _value: Default::default(),
        }
    }
}

impl<D, K, V> MapDataModel<D, K, V>
where
    D: NodeStore,
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    pub fn clear(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    pub fn put(&self, key: &K, value: &V) -> Result<(), StoreError> {
        let MapDataModel {
            delegate, lane_id, ..
        } = self;

        let key = serialize(key)?;
        let value = serialize(value)?;
        let k = StoreKey::Map {
            lane_id: *lane_id,
            key: Some(key),
        };
        delegate.put(k, value.as_slice())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        let MapDataModel {
            delegate, lane_id, ..
        } = self;

        let opt = serialize_then(delegate, key, |del, key| {
            del.get(StoreKey::Map {
                lane_id: *lane_id,
                key: Some(key),
            })
        })?;

        match opt {
            Some(bytes) => deserialize(bytes.as_slice()).map(Some),
            None => Ok(None),
        }
    }

    pub fn delete(&self, key: &K) -> Result<(), StoreError> {
        let MapDataModel {
            delegate, lane_id, ..
        } = self;

        serialize_then(delegate, key, |del, key| {
            del.delete(StoreKey::Map {
                lane_id: *lane_id,
                key: Some(key),
            })
        })
    }

    pub fn snapshot(&self) -> Result<Option<Vec<(K, V)>>, StoreError> {
        let store_key = StoreKey::Map {
            lane_id: self.lane_id,
            key: None,
        };

        self.delegate.load_ranged_snapshot(store_key, |key, value| {
            let store_key = deserialize::<StoreKey>(key)?;

            match store_key {
                StoreKey::Map { key, .. } => {
                    let key = deserialize::<K>(&key.ok_or(StoreError::KeyNotFound)?)?;
                    let value = deserialize::<V>(value)?;

                    Ok((key, value))
                }
                StoreKey::Value { .. } => Err(StoreError::Decoding(INCONSISTENT_DB.to_string())),
            }
        })
    }
}
