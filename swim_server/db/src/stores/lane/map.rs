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

use crate::stores::lane::{
    serialize, serialize_into_vec, FromDelegate, LaneKey, LaneStore, SwimLaneStore,
};
use crate::{OwnedStoreEngine, StoreEngine, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait MapStore<'a, K, V>: FromDelegate<'a>
where
    K: Serialize + 'a,
    V: Serialize + 'a,
{
    fn put(&self, key: &K, value: &V) -> Result<(), StoreError>;

    fn get(&self, key: &K) -> Result<Option<Vec<u8>>, StoreError>;

    fn delete(&self, key: &K) -> Result<bool, StoreError>;
}

impl<'a, E> FromDelegate<'a> for MapLaneStore<E>
where
    E: StoreEngine<'a>,
{
    type Delegate = SwimLaneStore<E>;

    fn from_delegate(delegate: Self::Delegate, address: String) -> Self {
        MapLaneStore::new(delegate, address)
    }
}

pub struct MapLaneStore<D> {
    delegate: SwimLaneStore<D>,
    lane_uri: String,
}

impl<D> MapLaneStore<D> {
    pub fn new(delegate: SwimLaneStore<D>, lane_uri: String) -> Self {
        MapLaneStore { delegate, lane_uri }
    }
}

impl<'a, D, K: 'a, V: 'a> MapStore<'a, K, V> for MapLaneStore<D>
where
    D: LaneStore<'a> + OwnedStoreEngine<Key = &'a LaneKey<'a>, Value = &'a [u8]>,
    K: Serialize,
    V: Serialize + DeserializeOwned,
{
    fn put(&self, key: &K, value: &V) -> Result<(), StoreError> {
        let MapLaneStore { delegate, lane_uri } = self;
        let key = serialize(key)?;
        let value = serialize(value)?;

        delegate.put(&LaneKey::Map { lane_uri, key }, value.as_slice())
    }

    fn get(&self, key: &K) -> Result<Option<Vec<u8>>, StoreError> {
        let MapLaneStore { delegate, lane_uri } = self;
        serialize_into_vec(delegate, key, |del, key| {
            del.get(&LaneKey::Map { lane_uri, key })
        })
    }

    fn delete(&self, key: &K) -> Result<bool, StoreError> {
        let MapLaneStore { delegate, lane_uri } = self;
        serialize_into_vec(delegate, key, |del, key| {
            del.delete(&LaneKey::Map { lane_uri, key })
        })
    }
}
