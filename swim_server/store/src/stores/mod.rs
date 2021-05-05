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
use serde::{Deserialize, Serialize};

pub mod lane;
pub mod node;
pub mod plane;

pub const INCONSISTENT_DB: &str = "Missing key or value in store";

/// A lane key that is either a map lane key or a value lane key.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub enum StoreKey {
    /// A map lane key.
    ///
    /// Within plane stores, map lane keys are defined in the format of `/lane_id/key` where `key`
    /// is the key of a lane's map data structure.
    Map {
        /// The lane ID.
        lane_id: KeyType,
        /// An optional, serialized, key. This is optional as ranged snapshots to not require the
        /// key.
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<Vec<u8>>,
    },
    /// A value lane key.
    Value {
        /// The lane ID.
        lane_id: KeyType,
    },
}
