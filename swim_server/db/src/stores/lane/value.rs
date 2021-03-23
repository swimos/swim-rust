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

use crate::stores::lane::{serialize_into_slice, FromDelegate, LaneKey, SwimLaneStore};
use crate::{OwnedStoreEngine, StoreEngine, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait ValueStore<'a, V>: FromDelegate<'a>
where
    V: Serialize,
{
    fn store(&self, value: &V) -> Result<(), StoreError>;

    fn load(&self) -> Result<V, StoreError>;

    fn clear(&self) -> Result<(), StoreError>;
}

pub struct ValueLaneStore<'a, D> {
    delegate: SwimLaneStore<D>,
    key: LaneKey<'a>,
}

impl<'a, D> FromDelegate<'a> for ValueLaneStore<'a, D>
where
    D: OwnedStoreEngine,
{
    type Delegate = SwimLaneStore<D>;

    fn from_delegate(delegate: Self::Delegate, address: String) -> Self {
        ValueLaneStore::new(delegate, address)
    }
}

impl<'a, D> ValueLaneStore<'a, D> {
    pub fn new(delegate: SwimLaneStore<D>, lane_uri: String) -> Self {
        ValueLaneStore {
            delegate,
            key: LaneKey::Value { lane_uri },
        }
    }
}

impl<'a, D, V> ValueStore<'a, V> for ValueLaneStore<'a, D>
where
    D: OwnedStoreEngine<Key = &'a LaneKey<'a>, Value = &'a [u8]>,
    V: Serialize + DeserializeOwned,
{
    fn store(&self, value: &V) -> Result<(), StoreError> {
        let ValueLaneStore { delegate, key } = self;
        serialize_into_slice(delegate, value, |del, obj| del.put(key, obj))
    }

    fn load(&self) -> Result<V, StoreError> {
        let ValueLaneStore { delegate, key } = self;

        let bytes = delegate
            .get(key)?
            // todo err
            .ok_or_else(|| StoreError::Error("Entry not found".to_string()))?;
        let slice = bytes.as_slice();
        bincode::deserialize(slice).map_err(Into::into)
    }

    fn clear(&self) -> Result<(), StoreError> {
        let ValueLaneStore { delegate, key } = self;
        delegate.delete(key).map(|_| ())
    }
}
