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

use serde::Serialize;

use common::model::Value;

use crate::{FormSerializeErr, ValueSerializer};

use super::Result;

#[cfg(test)]
mod simple_data_types;

#[cfg(test)]
mod collections;

#[cfg(test)]
mod nested;

#[cfg(test)]
mod vectors;

pub fn to_value<T>(value: &T) -> Result<Value>
where
    T: Serialize,
{
    let mut serializer = ValueSerializer::default();
    match value.serialize(&mut serializer) {
        Ok(_) => Ok(serializer.output()),
        Err(e) => Err(e),
    }
}

pub fn assert_err(
    parsed: ::std::result::Result<Value, FormSerializeErr>,
    expected: FormSerializeErr,
) {
    match parsed {
        Ok(v) => {
            eprintln!("Expected error: {:?}", v);
            panic!();
        }
        Err(e) => assert_eq!(e, expected),
    }
}
