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

use crate::stores::lane::{serialize_then, LaneKey};
use crate::stores::node::SwimNodeStore;
use crate::{StoreEngine, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

/// A value lane data model.
pub struct ValueDataModel {
    /// The store to delegate this model's operations to.
    delegate: ValueDataModelDelegate,
    /// The lane URI that this store is operating on.
    lane_uri: Arc<String>,
}

impl ValueDataModel {
    /// Constructs a new value data model.
    ///
    /// # Arguments
    /// `delegate`: if this data model is *not* transient, then delegate operations to this store.
    /// `lane_uri`: the lane URI that this store represents.
    /// `transient`: whether this store should be an in-memory model.
    pub fn new(delegate: SwimNodeStore, lane_uri: String, transient: bool) -> Self {
        if transient {
            ValueDataModel {
                delegate: ValueDataModelDelegate::Mem,
                lane_uri: Arc::new(lane_uri),
            }
        } else {
            ValueDataModel {
                delegate: ValueDataModelDelegate::Db(delegate),
                lane_uri: Arc::new(lane_uri),
            }
        }
    }

    fn key(&self) -> LaneKey {
        LaneKey::Value {
            lane_uri: self.lane_uri.clone(),
        }
    }
}

pub enum ValueDataModelDelegate {
    Mem,
    Db(SwimNodeStore),
}

impl ValueDataModel {
    /// Serializes and stores `value`.
    pub async fn store<V>(&self, value: &V) -> Result<(), StoreError>
    where
        V: Serialize,
    {
        match &self.delegate {
            ValueDataModelDelegate::Mem => unimplemented!(),
            ValueDataModelDelegate::Db(store) => serialize_then(store, value, |delegate, bytes| {
                delegate.put(self.key(), bytes)
            }),
        }
    }

    /// Loads the value in the data model if it exists.
    pub async fn load<V>(&self) -> Result<Option<V>, StoreError>
    where
        V: DeserializeOwned,
    {
        match &self.delegate {
            ValueDataModelDelegate::Mem => unimplemented!(),
            ValueDataModelDelegate::Db(store) => match store.get(self.key()) {
                Ok(Some(bytes)) => {
                    let slice = bytes.as_slice();
                    bincode::deserialize(slice).map_err(|e| StoreError::Decoding(e.to_string()))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            },
        }
    }

    /// Clears the value within the data model.
    pub async fn clear(&self) -> Result<(), StoreError> {
        match &self.delegate {
            ValueDataModelDelegate::Mem => unimplemented!(),
            ValueDataModelDelegate::Db(store) => store.delete(self.key()).map(|_| ()),
        }
    }
}
