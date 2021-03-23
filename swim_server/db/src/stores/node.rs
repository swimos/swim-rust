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

use crate::stores::lane::{LaneKey, LaneStore, SwimLaneStore};
use crate::stores::{MapStorageKey, StoreKey, ValueStorageKey};
use crate::{StoreEngine, StoreError};
use std::borrow::Cow;
use std::sync::Arc;

pub trait NodeStore<'a>: StoreEngine<'a> + Send + Sync + 'static {
    type LaneStore: LaneStore<'a>;

    fn lane_store(&self) -> Self::LaneStore;
}

pub struct SwimNodeStore<D> {
    delegate: Arc<D>,
    node_uri: Cow<'static, str>,
}

impl<D> Clone for SwimNodeStore<D> {
    fn clone(&self) -> Self {
        SwimNodeStore {
            delegate: self.delegate.clone(),
            node_uri: self.node_uri.clone(),
        }
    }
}

impl<D> SwimNodeStore<D> {
    pub fn new<S>(delegate: D, node_uri: Cow<'static, str>) -> SwimNodeStore<D> {
        SwimNodeStore {
            delegate: Arc::new(delegate),
            node_uri,
        }
    }
}

// // todo remove clones
// fn map_key(key: &LaneKey, node_uri: String) -> StoreKey {
//     match key {
//         LaneKey::Map { lane_uri, key } => StoreKey::Map(MapStorageKey {
//             node_uri,
//             lane_uri: lane_uri.clone(),
//             key: key.clone(),
//         }),
//         LaneKey::Value { lane_uri } => StoreKey::Value(ValueStorageKey {
//             node_uri,
//             lane_uri: lane_uri.clone(),
//         }),
//     }
// }

impl<'a, D> NodeStore<'a> for SwimNodeStore<D>
where
    D: StoreEngine<'a, Key = &'a StoreKey, Value = &'a [u8], Error = StoreError>
        + Send
        + Sync
        + 'static,
{
    type LaneStore = SwimLaneStore<Self>;

    fn lane_store(&self) -> Self::LaneStore {
        SwimLaneStore::new(self.clone())
    }
}

impl<'a, D: 'a> StoreEngine<'a> for SwimNodeStore<D>
where
    D: StoreEngine<'a, Key = &'a StoreKey, Value = &'a [u8], Error = StoreError>,
{
    type Key = &'a StoreKey;
    type Value = &'a [u8];
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let SwimNodeStore { delegate, node_uri } = self;
        delegate.put(&key, value)
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        unimplemented!()
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, Self::Error> {
        unimplemented!()
    }
}
