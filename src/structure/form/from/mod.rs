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

use serde::de::StdError;
use serde::export::Formatter;

use crate::model::{Item, Value};
use serde::Serialize;

mod form;
mod unit;

#[cfg(test)]
mod tests;

pub type Result<T> = ::std::result::Result<T, SerializerError>;

#[derive(Clone, Debug, PartialEq)]
pub enum SerializerError {
    Message(String),
    UnsupportedType(String),
}

#[allow(dead_code)]
pub fn to_value<T>(value: &T) -> Result<Value>
where
    T: Serialize,
{
    let mut serializer = Serializer::new();
    value.serialize(&mut serializer)?;

    Ok(serializer.output())
}

#[derive(Debug)]
pub struct Serializer {
    pub current_state: State,
    stack: Vec<State>,
}

#[derive(Debug, Clone)]
pub struct State {
    pub output: Value,
    pub serializer_state: SerializerState,
    pub attr_name: Option<String>,
}

#[derive(Clone, Debug)]
pub enum SerializerState {
    ReadingNested,
    // Reading key
    ReadingMap(bool),
    None,
}

#[derive(Debug, PartialEq)]
pub enum FormParseErr {
    None,
    IncorrectType(Value),
    Malformatted,
    InvalidString(String),
    IllegalItem(Item),
    NotABoolean,
}

impl Display for FormParseErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_string())
    }
}

impl StdError for FormParseErr {}

impl serde::ser::Error for FormParseErr {
    fn custom<T>(_msg: T) -> Self
    where
        T: Display,
    {
        FormParseErr::InvalidString(String::from("ser::Error"))
    }
}
