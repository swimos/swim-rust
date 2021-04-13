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

use crate::engines::db::StoreDelegate;
use crate::{KeyedSnapshot, RangedSnapshot, StoreEngine, StoreError};

use crate::stores::lane::{serialize, serialize_then};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A storage key used for either map or value lanes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub enum StoreKey {
    Map(MapStorageKey),
    Value(ValueStorageKey),
}

/// A storage key for map lanes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct MapStorageKey {
    /// The node URI that this key corresponds to.
    pub node_uri: Arc<String>,
    /// The lane URI that this key corresponds to.
    pub lane_uri: Arc<String>,
    /// An optional serialized key for the key within the lane.
    ///
    /// This is optional as it is not required for executing ranged snapshots on a storage engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Vec<u8>>,
}

/// A storage key for value lanes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct ValueStorageKey {
    pub node_uri: Arc<String>,
    pub lane_uri: Arc<String>,
}

/// A store which may either be an LMDB or Rocks database.
pub struct DatabaseStore<K> {
    /// The delegate store.
    delegate: StoreDelegate,
    _key_pd: PhantomData<K>,
}

impl<K> RangedSnapshot for DatabaseStore<K> {
    type Prefix = StoreKey;

    fn ranged_snapshot<F, DK, DV>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<DK, DV>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(DK, DV), StoreError>,
    {
        match prefix {
            StoreKey::Map(key) => {
                let prefix = serialize(&key)?;
                self.delegate.ranged_snapshot(prefix, map_fn)
            }
            StoreKey::Value(key) => {
                let prefix = serialize(&key)?;
                self.delegate.ranged_snapshot(prefix, map_fn)
            }
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
        serialize_then(&self.delegate, key, |delegate, key| {
            delegate.put(key.as_slice(), value.as_slice())
        })
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        serialize_then(&self.delegate, key, |delegate, key| {
            delegate.get(key.as_slice())
        })
    }

    fn delete(&self, key: Self::Key) -> Result<(), Self::Error> {
        serialize_then(&self.delegate, key, |delegate, key| {
            delegate.delete(key.as_slice())
        })
    }
}
