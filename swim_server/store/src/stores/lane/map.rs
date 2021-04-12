// Copyright 2015-2020 SWIM.AI inc.
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

use crate::stores::lane::{serialize, serialize_then, LaneKey};
use crate::stores::node::SwimNodeStore;
use crate::stores::MapStorageKey;
use crate::{deserialize, KeyedSnapshot, RangedSnapshot, Snapshot, StoreEngine, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct MapDataModel<K, V> {
    delegate: MapDataModelDelegate,
    lane_uri: Arc<String>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

pub enum MapDataModelDelegate {
    Mem,
    Db(SwimNodeStore),
}

impl<K, V> MapDataModel<K, V> {
    pub fn new(delegate: SwimNodeStore, lane_uri: String, transient: bool) -> Self {
        let delegate = if transient {
            MapDataModelDelegate::Mem
        } else {
            MapDataModelDelegate::Db(delegate)
        };

        MapDataModel {
            delegate,
            lane_uri: Arc::new(lane_uri),
            _key: Default::default(),
            _value: Default::default(),
        }
    }
}

impl<K, V> MapDataModel<K, V>
where
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    pub async fn put(&self, key: &K, value: &V) -> Result<(), StoreError> {
        let MapDataModel {
            delegate, lane_uri, ..
        } = self;

        match delegate {
            MapDataModelDelegate::Mem => unimplemented!(),
            MapDataModelDelegate::Db(store) => {
                let key = serialize(key)?;
                let value = serialize(value)?;
                let k = LaneKey::Map {
                    lane_uri: lane_uri.clone(),
                    key: Some(key),
                };
                store.put(k, value)
            }
        }
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        let MapDataModel {
            delegate, lane_uri, ..
        } = self;

        match delegate {
            MapDataModelDelegate::Mem => unimplemented!(),
            MapDataModelDelegate::Db(store) => {
                let opt = serialize_then(store, key, |del, key| {
                    del.get(LaneKey::Map {
                        lane_uri: lane_uri.clone(),
                        key: Some(key),
                    })
                })?;

                match opt {
                    Some(bytes) => bincode::deserialize(bytes.as_slice())
                        .map_err(|e| StoreError::Decoding(e.to_string()))
                        .map(Some),
                    None => Ok(None),
                }
            }
        }
    }

    pub async fn delete(&self, key: &K) -> Result<(), StoreError> {
        let MapDataModel {
            delegate, lane_uri, ..
        } = self;

        match delegate {
            MapDataModelDelegate::Mem => unimplemented!(),
            MapDataModelDelegate::Db(store) => serialize_then(store, key, |del, key| {
                del.delete(LaneKey::Map {
                    lane_uri: lane_uri.clone(),
                    key: Some(key),
                })
            }),
        }
    }
}

impl<K, V> RangedSnapshot for MapDataModel<K, V> {
    type Prefix = Arc<String>;

    fn ranged_snapshot<F, DK, DV>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<DK, DV>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(DK, DV), StoreError>,
    {
        let prefix = LaneKey::Map {
            lane_uri: prefix,
            key: None,
        };

        match &self.delegate {
            MapDataModelDelegate::Mem => {
                unimplemented!()
            }
            MapDataModelDelegate::Db(store) => store.ranged_snapshot(prefix, map_fn),
        }
    }
}

impl<K, V> Snapshot<K, V> for MapDataModel<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Snapshot = KeyedSnapshot<K, V>;

    fn snapshot(&self) -> Result<Option<Self::Snapshot>, StoreError> {
        self.ranged_snapshot(self.lane_uri.clone(), |key, value| {
            let store_key = deserialize::<MapStorageKey>(&key)?;

            let key = deserialize::<K>(&store_key.key.ok_or(StoreError::KeyNotFound)?)?;
            let value = deserialize::<V>(&value)?;

            Ok((key, value))
        })
    }
}
