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

use crate::engines::{StoreDelegateSnapshot, StoreDelegateSnapshotIterator};
use crate::stores::lane::{serialize, serialize_then, LaneKey};
use crate::stores::node::SwimNodeStore;
use crate::stores::{MapStorageKey, StoreKey};
use crate::{RangedSnapshot, Snapshot, StoreEngine, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::{Debug, Formatter};
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
            key: Some(key),
        };
        delegate.put(k, value)
    }

    fn get(&self, key: &K) -> Result<Option<V>, StoreError> {
        let MapLaneStore {
            delegate, lane_uri, ..
        } = self;
        let opt = serialize_then(delegate, key, |del, key| {
            del.get(LaneKey::Map {
                lane_uri: lane_uri.clone(),
                key: Some(key),
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
        serialize_then(delegate, key, |del, key| {
            del.delete(LaneKey::Map {
                lane_uri: lane_uri.clone(),
                key: Some(key),
            })
        })
    }
}

impl<K, V> RangedSnapshot for MapLaneStore<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Key = K;
    type Value = V;
    type RangedSnapshot = MapLaneRangedSnapshot<K, V>;
    type Prefix = Arc<String>;

    fn ranged_snapshot(
        &self,
        prefix: Self::Prefix,
    ) -> Result<Option<Self::RangedSnapshot>, StoreError> {
        let prefix = LaneKey::Map {
            lane_uri: prefix,
            key: None,
        };

        match self.delegate.ranged_snapshot(prefix)? {
            Some(snapshot) => Ok(Some(MapLaneRangedSnapshot {
                snapshot,
                _k_pd: Default::default(),
                _v_pd: Default::default(),
            })),
            None => Ok(None),
        }
    }
}

pub struct MapLaneRangedSnapshot<K, V> {
    snapshot: StoreDelegateSnapshot,
    _k_pd: PhantomData<K>,
    _v_pd: PhantomData<V>,
}

impl<K, V> Debug for MapLaneRangedSnapshot<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapLaneRangedSnapshot").finish()
    }
}

impl<K, V> IntoIterator for MapLaneRangedSnapshot<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Item = Result<(K, V), StoreError>;
    type IntoIter = MapLaneRangedSnapshotIterator<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        MapLaneRangedSnapshotIterator {
            snapshot: self.snapshot.into_iter(),
            _k_pd: Default::default(),
            _v_pd: Default::default(),
        }
    }
}

pub struct MapLaneRangedSnapshotIterator<K, V> {
    snapshot: StoreDelegateSnapshotIterator,
    _k_pd: PhantomData<K>,
    _v_pd: PhantomData<V>,
}

impl<K, V> Iterator for MapLaneRangedSnapshotIterator<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Item = Result<(K, V), StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.snapshot.next().map(|(key, value)| {
            let store_key = bincode::deserialize::<MapStorageKey>(&key)?;
            let key = bincode::deserialize::<K>(&store_key.key.ok_or(StoreError::KeyNotFound)?)?;
            let value = bincode::deserialize::<V>(&value)?;

            Ok((key, value))
        })
    }
}

impl<K, V> Snapshot<K, V> for MapLaneStore<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Snapshot = MapLaneRangedSnapshot<K, V>;

    fn snapshot(&self) -> Result<Option<Self::Snapshot>, StoreError> {
        self.ranged_snapshot(self.lane_uri.clone())
    }
}
