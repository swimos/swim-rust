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

use crate::{StoreEngine, StoreError};
use serde::Serialize;
use std::sync::Arc;

pub mod map;
pub mod value;

/// A lane key that is either a map lane key or a value lane key.
pub enum LaneKey {
    /// A map lane key.
    ///
    /// Within plane stores, map lane keys are defined in the format of `/node_uri/lane_uri/key`
    /// where `key` is the key of a lane's map data structure.
    Map {
        /// The lane URI.
        lane_uri: Arc<String>,
        /// An optional, serialized, key. This is optional as ranged snapshots to not require the
        /// key.
        key: Option<Vec<u8>>,
    },
    /// A value lane key.
    Value {
        /// The lane URi.
        lane_uri: Arc<String>,
    },
}

/// Serialize `obj` and then execute `f` with the bytes if the operation succeeded. Returns the
/// output of `f`.
pub fn serialize_then<'a, S, F, O, E>(engine: &E, obj: &S, f: F) -> Result<O, StoreError>
where
    S: Serialize,
    E: StoreEngine<'a>,
    F: Fn(&E, Vec<u8>) -> Result<O, StoreError>,
{
    f(engine, serialize(obj)?)
}

pub fn serialize<S: Serialize>(obj: &S) -> Result<Vec<u8>, StoreError> {
    bincode::serialize(obj).map_err(|e| StoreError::Encoding(e.to_string()))
}
