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

use crate::stores::lane::serialize_then;
use crate::stores::node::SwimNodeStore;
use crate::stores::LaneKey;
use crate::{NodeStore, PlaneStore, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use swim_common::model::text::Text;

/// A value lane data model.
pub struct ValueDataModel<D> {
    /// The store to delegate this model's operations to.
    delegate: SwimNodeStore<D>,
    /// The lane URI that this store is operating on.
    lane_uri: Text,
}

impl<D> ValueDataModel<D> {
    /// Constructs a new value data model.
    ///
    /// # Arguments
    /// `delegate`: if this data model is *not* transient, then delegate operations to this store.
    /// `lane_uri`: the lane URI that this store represents.
    pub fn new<I: Into<Text>>(delegate: SwimNodeStore<D>, lane_uri: I) -> Self {
        ValueDataModel {
            delegate,
            lane_uri: lane_uri.into(),
        }
    }

    fn key(&self) -> LaneKey {
        LaneKey::Value {
            lane_uri: &self.lane_uri,
        }
    }
}

impl<D: PlaneStore> ValueDataModel<D> {
    /// Serializes and stores `value`.
    pub fn store<V>(&self, value: &V) -> Result<(), StoreError>
    where
        V: Serialize,
    {
        serialize_then(&self.delegate, value, |delegate, bytes| {
            delegate.put(self.key(), bytes)
        })
    }

    /// Loads the value in the data model if it exists.
    pub fn load<V>(&self) -> Result<Option<V>, StoreError>
    where
        V: DeserializeOwned,
    {
        match self.delegate.get(self.key()) {
            Ok(Some(bytes)) => {
                let slice = bytes.as_slice();
                bincode::deserialize(slice).map_err(|e| StoreError::Decoding(e.to_string()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Clears the value within the data model.
    pub fn clear(&self) -> Result<(), StoreError> {
        self.delegate.delete(self.key()).map(|_| ())
    }
}
