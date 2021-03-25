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

pub enum LaneKey {
    Map {
        lane_uri: Arc<String>,
        key: Option<Vec<u8>>,
    },
    Value {
        lane_uri: Arc<String>,
    },
}

pub fn serialize_then<'a, S, F, O, E>(engine: &E, obj: &S, f: F) -> Result<O, StoreError>
where
    S: Serialize,
    E: StoreEngine<'a>,
    F: Fn(&E, Vec<u8>) -> Result<O, StoreError>,
{
    f(engine, serialize(obj)?)
}

pub fn serialize<S: Serialize>(obj: &S) -> Result<Vec<u8>, StoreError> {
    bincode::serialize(obj).map_err(StoreError::from)
}
