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

use num_bigint::{BigInt, BigUint};

use common::model::{Item, Value};
pub use writer::ValueWriter;

use crate::reader::ValueReadError;
use crate::{Form, TransmuteValue};
use common::model::blob::Blob;

#[cfg(test)]
mod tests;
mod writer;

#[derive(Clone, Debug, PartialEq)]
pub enum WriteValueError {
    Message(String),
    UnsupportedType(String),
    IncorrectType(Value),
    IllegalItem(Item),
    IllegalState(String),
    Malformatted,
}

pub fn as_value<T>(v: &T) -> Value
where
    T: TransmuteValue,
{
    let mut writer = ValueWriter::default();
    v.transmute_to_value(&mut writer);
    writer.finish()
}
