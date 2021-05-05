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

use crate::engines::keyspaces::KeyType;
use crate::stores::lane::{deserialize, serialize_then};
use crate::stores::node::SwimNodeStore;
use crate::{NodeStore, PlaneStore, StoreError, StoreKey};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// A value lane data model.
pub struct ValueDataModel<D> {
    /// The store to delegate this model's operations to.
    delegate: SwimNodeStore<D>,
    /// The lane URI that this store is operating on.
    lane_id: KeyType,
}

impl<D> ValueDataModel<D> {
    /// Constructs a new value data model.
    ///
    /// # Arguments
    /// `delegate`: if this data model is *not* transient, then delegate operations to this store.
    /// `lane_id`: the lane URI that this store represents.
    pub fn new(delegate: SwimNodeStore<D>, lane_id: KeyType) -> Self {
        ValueDataModel { delegate, lane_id }
    }

    fn key(&self) -> StoreKey {
        StoreKey::Value {
            lane_id: self.lane_id,
        }
    }
}

impl<D: PlaneStore> ValueDataModel<D> {
    /// Serialize and store `value`.
    pub fn store<V>(&self, value: &V) -> Result<(), StoreError>
    where
        V: Serialize,
    {
        serialize_then(&self.delegate, value, |delegate, bytes| {
            delegate.put(self.key(), bytes.as_slice())
        })
    }

    /// Loads the value in the store if it exists.
    pub fn load<V>(&self) -> Result<Option<V>, StoreError>
    where
        V: DeserializeOwned,
    {
        match self.delegate.get(self.key()) {
            Ok(Some(bytes)) => deserialize::<V>(bytes.as_slice()).map(Some),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Clears the value within the store.
    pub fn clear(&self) -> Result<(), StoreError> {
        self.delegate.delete(self.key()).map(|_| ())
    }
}
