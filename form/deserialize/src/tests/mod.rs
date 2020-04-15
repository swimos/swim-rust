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

use serde::Deserialize;

use common::model::Value;

use crate::ValueDeserializer;

#[cfg(test)]
mod nested;

#[cfg(test)]
mod collections;

#[cfg(test)]
mod simple_data_types;

#[cfg(test)]
mod vectors;

fn from_value<'de, T>(value: &'de Value) -> super::Result<T>
where
    T: Deserialize<'de>,
{
    let mut deserializer = match value {
        Value::Record(_, _) => ValueDeserializer::for_values(value),
        _ => ValueDeserializer::for_single_value(value),
    };

    let t = T::deserialize(&mut deserializer)?;
    Ok(t)
}
