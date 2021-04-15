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

use crate::engines::KeyedSnapshot;
use crate::stores::lane::{deserialize, serialize, serialize_then};
use crate::stores::node::SwimNodeStore;
use crate::stores::{LaneKey, MapStorageKey};
use crate::{NodeStore, PlaneStore, Snapshot, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use swim_common::model::text::Text;

pub struct MapDataModel<D, K, V> {
    delegate: SwimNodeStore<D>,
    lane_uri: Text,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<D, K, V> MapDataModel<D, K, V> {
    pub fn new<I: Into<Text>>(delegate: SwimNodeStore<D>, lane_uri: I) -> Self {
        MapDataModel {
            delegate,
            lane_uri: lane_uri.into(),
            _key: Default::default(),
            _value: Default::default(),
        }
    }
}

impl<D, K, V> MapDataModel<D, K, V>
where
    D: PlaneStore,
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    pub fn put(&self, key: &K, value: &V) -> Result<(), StoreError> {
        let MapDataModel {
            delegate, lane_uri, ..
        } = self;

        let key = serialize(key)?;
        let value = serialize(value)?;
        let k = LaneKey::Map {
            lane_uri,
            key: Some(key),
        };
        delegate.put(k, value.as_slice())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        let MapDataModel {
            delegate, lane_uri, ..
        } = self;

        let opt = serialize_then(delegate, key, |del, key| {
            del.get(LaneKey::Map {
                lane_uri,
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
            delegate, lane_uri, ..
        } = self;

        serialize_then(delegate, key, |del, key| {
            del.delete(LaneKey::Map {
                lane_uri,
                key: Some(key),
            })
        })
    }
}

impl<D, K, V> Snapshot<K, V> for MapDataModel<D, K, V>
where
    D: PlaneStore,
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Snapshot = KeyedSnapshot<K, V>;

    fn snapshot(&self) -> Result<Option<Self::Snapshot>, StoreError> {
        let lane_key = LaneKey::Map {
            lane_uri: &self.lane_uri,
            key: None,
        };

        self.delegate.load_ranged_snapshot(lane_key, |key, value| {
            let store_key = deserialize::<MapStorageKey>(&key)?;

            let key = deserialize::<K>(&store_key.key.ok_or(StoreError::KeyNotFound)?)?;
            let value = deserialize::<V>(&value)?;

            Ok((key, value))
        })
    }
}
