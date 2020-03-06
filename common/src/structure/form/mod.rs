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

use core::fmt;
use std::fmt::{Debug, Display};

use serde::export::Formatter;
use serde::{Deserialize, Serialize};

use crate::model::{Item, Value};
use crate::structure::form::from::ValueDeserializer;
use crate::structure::form::to::ValueSerializer;

mod from;
mod to;

pub type Result<T> = ::std::result::Result<T, FormParseErr>;

pub struct Form {}

impl Default for Form {
    fn default() -> Self {
        Form {}
    }
}

#[allow(dead_code)]
impl Form {
    pub fn to_value<T>(&self, value: &T) -> Result<Value>
    where
        T: Serialize,
    {
        let mut serializer = ValueSerializer::default();
        match value.serialize(&mut serializer) {
            Ok(_) => Ok(serializer.output()),
            Err(e) => Err(e),
        }
    }

    pub fn from_value<'de, T>(&self, value: &'de Value) -> Result<T>
    where
        T: Deserialize<'de>,
    {
        let mut deserializer = match value {
            Value::Record(_, _) => ValueDeserializer::for_values(value),
            _ => ValueDeserializer::for_single_value(value),
        };

        let t = T::deserialize(&mut deserializer)?;
        deserializer.assert_stack_empty();
        Ok(t)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FormParseErr {
    Message(String),
    UnsupportedType(String),
    IncorrectType(Value),
    IllegalItem(Item),
    IllegalState(String),
    Malformatted,
}

impl Display for FormParseErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_string())
    }
}
