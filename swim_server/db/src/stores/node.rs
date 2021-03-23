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

pub trait NodeStore<'a>: StoreEngine<'a> + Send + Sync + 'static {
    type LaneStore: LaneStore<'a>;

    fn lane_store<I>(&mut self, node: I) -> Result<Self::LaneStore, StoreError>
    where
        I: ToString;
}

pub struct SwimNodeStore<D> {
    delegate: D,
    node_uri: String,
}

// todo remove clones
fn map_key(key: &LaneKey, node_uri: String) -> StoreKey {
    match key {
        LaneKey::Map { lane_uri, key } => StoreKey::Map(MapStorageKey {
            node_uri,
            lane_uri: lane_uri.clone(),
            key: key.clone(),
        }),
        LaneKey::Value { lane_uri } => StoreKey::Value(ValueStorageKey {
            node_uri,
            lane_uri: lane_uri.clone(),
        }),
    }
}

impl<'a, D> NodeStore<'a> for SwimNodeStore<D>
where
    D: Send + Sync + 'static,
{
    type LaneStore = SwimLaneStore<Self>;

    fn lane_store<I>(&mut self, _node: I) -> Result<Self::LaneStore, StoreError>
    where
        I: ToString,
    {
        unimplemented!()
    }
}

impl<'a, D> StoreEngine<'a> for SwimNodeStore<D> {
    type Key = ();
    type Value = ();
    type Error = StoreError;

    fn put(&self, _key: Self::Key, _value: Self::Value) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        unimplemented!()
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, Self::Error> {
        unimplemented!()
    }
}
