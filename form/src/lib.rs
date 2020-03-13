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

use common::model::Value;
use deserialize::FormDeserializeErr;
use serialize::FormSerializeErr;

#[cfg(test)]
mod tests;

#[allow(unused_imports)]
#[macro_use]
pub extern crate form_derive;
pub extern crate common;
pub extern crate deserialize;
pub extern crate serialize;

pub mod impls;

pub trait Form: Sized {
    fn try_into_value(&self) -> std::result::Result<Value, FormSerializeErr>;
    fn try_from_value(value: &Value) -> std::result::Result<Self, FormDeserializeErr>;
}
