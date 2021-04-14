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
