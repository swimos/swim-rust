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

pub struct ValueDataModel {
    delegate: ValueDataModelDelegate,
    lane_uri: Arc<String>,
}

impl ValueDataModel {
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
    pub async fn store<V>(&self, value: &V) -> Result<(), StoreError>
    where
        V: Serialize + DeserializeOwned,
    {
        match &self.delegate {
            ValueDataModelDelegate::Mem => unimplemented!(),
            ValueDataModelDelegate::Db(store) => serialize_then(store, value, |delegate, bytes| {
                delegate.put(self.key(), bytes)
            }),
        }
    }

    pub async fn load<V>(&self) -> Result<Option<V>, StoreError>
    where
        V: Serialize + DeserializeOwned,
    {
        match &self.delegate {
            ValueDataModelDelegate::Mem => unimplemented!(),
            ValueDataModelDelegate::Db(store) => match store.get(self.key()) {
                Ok(Some(bytes)) => {
                    let slice = bytes.as_slice();
                    bincode::deserialize(slice).map_err(Into::into)
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            },
        }
    }

    pub async fn clear(&self) -> Result<(), StoreError> {
        match &self.delegate {
            ValueDataModelDelegate::Mem => unimplemented!(),
            ValueDataModelDelegate::Db(store) => store.delete(self.key()).map(|_| ()),
        }
    }
}
