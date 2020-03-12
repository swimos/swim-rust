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
use std::fmt::{Debug, Display, Formatter};

use serde::de;

use common::model::{Attr, Item, Value};

#[cfg(test)]
mod tests;

mod deserializer;
mod enum_access;
mod map_access;

#[derive(Clone, Debug, PartialEq)]
pub enum FormDeserializeErr {
    Message(String),
    UnsupportedType(String),
    IncorrectType(Value),
    IllegalItem(Item),
    IllegalState(String),
    Malformatted,
}

pub type Result<T> = ::std::result::Result<T, FormDeserializeErr>;

impl Display for FormDeserializeErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_string())
    }
}

#[derive(Debug)]
pub struct ValueDeserializer<'de> {
    current_state: State<'de>,
    stack: Vec<State<'de>>,
    input: &'de Value,
}

#[derive(Debug, Clone)]
pub struct State<'de> {
    deserializer_state: DeserializerState<'de>,
    value: Option<&'de Value>,
}

#[derive(Clone, Debug)]
pub enum DeserializerState<'i> {
    ReadingRecord { item_index: usize },
    ReadingItem(&'i Item),
    ReadingAttribute(&'i Attr),
    ReadingSingleValue,
    None,
}

impl<'de> ValueDeserializer<'de> {
    pub fn for_values(input: &'de Value) -> Self {
        ValueDeserializer {
            current_state: State {
                deserializer_state: DeserializerState::None,
                value: Some(input),
            },
            stack: vec![],
            input,
        }
    }

    pub fn assert_stack_empty(&self) {
        assert_eq!(self.stack.len(), 0);
    }

    pub fn for_single_value(input: &'de Value) -> Self {
        ValueDeserializer {
            current_state: State {
                deserializer_state: DeserializerState::ReadingSingleValue,
                value: Some(input),
            },
            stack: vec![],
            input,
        }
    }

    pub fn push_record(&mut self, value: Option<&'de Value>) {
        self.push_state(State {
            deserializer_state: DeserializerState::ReadingRecord { item_index: 0 },
            value,
        });
    }

    pub fn push_state(&mut self, state: State<'de>) {
        self.stack.push(self.current_state.to_owned());
        self.current_state = state;
    }

    pub fn pop_state(&mut self) {
        if let Some(previous_state) = self.stack.pop() {
            self.current_state = previous_state;
        } else {
            self.current_state = State {
                deserializer_state: DeserializerState::None,
                value: None,
            };
        }
    }
}

impl de::Error for FormDeserializeErr {
    fn custom<T: Display>(msg: T) -> Self {
        FormDeserializeErr::Message(msg.to_string())
    }
}

impl std::error::Error for FormDeserializeErr {}
