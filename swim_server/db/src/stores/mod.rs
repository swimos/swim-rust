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

pub mod lane;
pub mod node;
pub mod plane;

use std::marker::PhantomData;

use crate::engines::{StoreDelegate, StoreSnapshot};
use crate::{Snapshot, StoreEngine, StoreError};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub enum StoreKey {
    Map(MapStorageKey),
    Value(ValueStorageKey),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct MapStorageKey {
    node_uri: String,
    lane_uri: String,
    key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct ValueStorageKey {
    pub node_uri: String,
    pub lane_uri: String,
}

pub struct DatabaseStore<K> {
    delegate: StoreDelegate,
    _key_pd: PhantomData<K>,
}

impl<'a, K> DatabaseStore<K> {
    pub fn new<D>(delegate: D) -> Self
    where
        D: StoreEngine<'a> + Into<StoreDelegate>,
    {
        DatabaseStore {
            delegate: delegate.into(),
            _key_pd: Default::default(),
        }
    }

    fn serialize_op<S: Serialize, F, O>(&self, key: &S, f: F) -> Result<O, StoreError>
    where
        F: Fn(&StoreDelegate, Vec<u8>) -> Result<O, StoreError>,
    {
        let key = bincode::serialize(key).map_err(StoreError::from)?;
        Ok(f(&self.delegate, key)?)
    }
}

impl<'a, K: 'a> StoreEngine<'a> for DatabaseStore<K>
where
    K: Serialize,
{
    type Key = &'a K;
    type Value = &'a [u8];
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.serialize_op(key, |d, v| d.put(v.as_slice(), value))
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        self.serialize_op(key, |d, v| d.get(v.as_slice()))
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        self.serialize_op(key, |d, v| d.delete(v.as_slice()))
    }
}

impl<'a, V> Snapshot<'a> for DatabaseStore<V>
where
    V: Send + Sync,
{
    type Snapshot = StoreSnapshot<'a>;

    fn snapshot(&'a self) -> Self::Snapshot {
        unimplemented!()
    }
}
