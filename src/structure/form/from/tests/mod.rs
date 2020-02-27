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

use crate::model::Value;
use crate::structure::form::from::SerializerError;

#[cfg(test)]
mod simple_data_types;

#[cfg(test)]
mod map;

#[cfg(test)]
mod nested;

#[cfg(test)]
mod vectors;

#[cfg(test)]
mod from;

pub fn assert_err(parsed: Result<Value, SerializerError>, expected: SerializerError) {
    match parsed {
        Ok(v) => {
            eprintln!("Expected error: {:?}", v);
            panic!();
        }
        Err(e) => assert_eq!(e, expected),
    }
}
