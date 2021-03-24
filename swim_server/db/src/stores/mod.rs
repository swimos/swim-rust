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

use crate::stores::lane::serialize_into_vec;
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub enum StoreKey {
    Map(MapStorageKey),
    Value(ValueStorageKey),
}

#[derive(Serialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct MapStorageKey {
    node_uri: Arc<String>,
    lane_uri: Arc<String>,
    key: Vec<u8>,
}

#[derive(Serialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct ValueStorageKey {
    pub node_uri: Arc<String>,
    pub lane_uri: Arc<String>,
}

pub struct DatabaseStore<K> {
    delegate: StoreDelegate,
    _key_pd: PhantomData<K>,
}

impl<K> Clone for DatabaseStore<K> {
    fn clone(&self) -> Self {
        DatabaseStore {
            delegate: self.delegate.clone(),
            _key_pd: Default::default(),
        }
    }
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
}

impl<'a, K: 'a> StoreEngine<'a> for DatabaseStore<K>
where
    K: Serialize + 'static,
{
    type Key = &'a K;
    type Value = Vec<u8>;
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        serialize_into_vec(&self.delegate, key, |delegate, key| {
            delegate.put(key.as_slice(), value.as_slice())
        })
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        serialize_into_vec(&self.delegate, key, |delegate, key| {
            delegate.get(key.as_slice())
        })
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        serialize_into_vec(&self.delegate, key, |delegate, key| {
            delegate.delete(key.as_slice())
        })
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
