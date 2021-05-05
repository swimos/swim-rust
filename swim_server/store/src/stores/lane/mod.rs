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

use crate::StoreError;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

pub mod map;
pub mod value;

/// Serialize `obj` and then execute `f` with the bytes if the operation succeeded. Returns the
/// output of `f`.
pub fn serialize_then<S, F, O, E>(engine: &E, obj: &S, f: F) -> Result<O, StoreError>
where
    S: Serialize,
    F: Fn(&E, Vec<u8>) -> Result<O, StoreError>,
{
    f(engine, serialize(obj)?).map_err(Into::into)
}

pub fn serialize<S: Serialize>(obj: &S) -> Result<Vec<u8>, StoreError> {
    bincode::serialize(obj).map_err(|e| StoreError::Encoding(e.to_string()))
}

pub fn deserialize<'de, D: Deserialize<'de>>(obj: &'de [u8]) -> Result<D, StoreError> {
    bincode::deserialize(obj).map_err(|e| StoreError::Decoding(e.to_string()))
}
