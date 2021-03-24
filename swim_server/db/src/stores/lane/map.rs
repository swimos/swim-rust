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

use crate::stores::lane::{serialize, serialize_into_vec, LaneKey};
use crate::stores::node::SwimNodeStore;
use crate::{StoreEngine, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;

pub trait MapStore<'a, K, V>
where
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    fn put(&self, key: &K, value: &V) -> Result<(), StoreError>;

    fn get(&self, key: &K) -> Result<Option<V>, StoreError>;

    fn delete(&self, key: &K) -> Result<bool, StoreError>;
}

pub struct MapLaneStore<K, V> {
    delegate: SwimNodeStore,
    lane_uri: Arc<String>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K, V> MapLaneStore<K, V> {
    pub fn new(delegate: SwimNodeStore, lane_uri: String) -> Self {
        MapLaneStore {
            delegate,
            lane_uri: Arc::new(lane_uri),
            _key: Default::default(),
            _value: Default::default(),
        }
    }
}

impl<'s, 'a, K, V> MapStore<'a, K, V> for MapLaneStore<K, V>
where
    K: Serialize,
    V: Serialize + DeserializeOwned + 's,
{
    fn put(&self, key: &K, value: &V) -> Result<(), StoreError> {
        let MapLaneStore {
            delegate, lane_uri, ..
        } = self;
        let key = serialize(key)?;
        let value = serialize(value)?;
        let k = LaneKey::Map {
            lane_uri: lane_uri.clone(),
            key,
        };
        delegate.put(k, value)
    }

    fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        let MapLaneStore {
            delegate, lane_uri, ..
        } = self;
        let opt = serialize_into_vec(delegate, key, |del, key| {
            del.get(LaneKey::Map {
                lane_uri: lane_uri.clone(),
                key,
            })
        })?;

        match opt {
            Some(bytes) => bincode::deserialize(bytes.as_slice())
                .map_err(Into::into)
                .map(Some),
            None => Ok(None),
        }
    }

    fn delete(&self, key: &K) -> Result<bool, StoreError> {
        let MapLaneStore {
            delegate, lane_uri, ..
        } = self;
        serialize_into_vec(delegate, key, |del, key| {
            del.delete(LaneKey::Map {
                lane_uri: lane_uri.clone(),
                key,
            })
        })
    }
}
