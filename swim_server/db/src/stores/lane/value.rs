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

use crate::stores::lane::{serialize_into_vec, LaneKey};
use crate::stores::node::SwimNodeStore;
use crate::{StoreEngine, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;

pub trait ValueStore<'a, V>
where
    V: Serialize,
{
    fn store(&self, value: &V) -> Result<(), StoreError>;

    fn load(&self) -> Result<V, StoreError>;

    fn clear(&self) -> Result<(), StoreError>;
}

pub struct ValueLaneStore<V> {
    delegate: SwimNodeStore,
    lane_uri: Arc<String>,
    _value: PhantomData<V>,
}

impl<V> ValueLaneStore<V> {
    pub fn new(delegate: SwimNodeStore, lane_uri: String) -> Self {
        ValueLaneStore {
            delegate,
            lane_uri: Arc::new(lane_uri),
            _value: Default::default(),
        }
    }
}

impl<'a, V> ValueStore<'a, V> for ValueLaneStore<V>
where
    V: Serialize + DeserializeOwned,
{
    fn store(&self, value: &V) -> Result<(), StoreError> {
        let ValueLaneStore {
            delegate, lane_uri, ..
        } = self;
        serialize_into_vec(delegate, value, |del, obj| {
            del.put(
                LaneKey::Value {
                    lane_uri: lane_uri.clone(),
                },
                obj,
            )
        })
    }

    fn load(&self) -> Result<V, StoreError> {
        let ValueLaneStore {
            delegate, lane_uri, ..
        } = self;

        let bytes = delegate
            .get(LaneKey::Value {
                lane_uri: lane_uri.clone(),
            })?
            // todo err
            .ok_or_else(|| StoreError::Error("Entry not found".to_string()))?;
        let slice = bytes.as_slice();
        bincode::deserialize(slice).map_err(Into::into)
    }

    fn clear(&self) -> Result<(), StoreError> {
        let ValueLaneStore {
            delegate, lane_uri, ..
        } = self;
        delegate
            .delete(LaneKey::Value {
                lane_uri: lane_uri.clone(),
            })
            .map(|_| ())
    }
}
