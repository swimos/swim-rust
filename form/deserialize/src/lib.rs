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

#![allow(clippy::match_wild_err_arm)]

use core::fmt;
use std::fmt::{Debug, Display, Formatter};

use serde::de;

use common::model::{Attr, Item, Value};

#[cfg(test)]
mod tests;

mod deserializer;
mod enum_access;
mod map_access;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FormDeserializeErr {
    Message(String),
    UnsupportedType(String),
    IncorrectType(String),
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

pub struct ValueDeserializer<'de> {
    current_state: State<'de>,
    stack: Vec<State<'de>>,
    input: &'de Value,
}

#[derive(Clone)]
pub struct State<'de> {
    deserializer_state: DeserializerState<'de>,
    value: Option<&'de Value>,
}

#[derive(Clone)]
pub enum DeserializerState<'i> {
    ReadingRecord { item_index: usize },
    ReadingItem(&'i Item),
    ReadingAttribute(&'i Attr),
    ReadingSingleValue,
    None,
}

impl<'de> ValueDeserializer<'de> {
    pub fn err_unsupported<V>(&self, t: &str) -> std::result::Result<V, FormDeserializeErr> {
        Err(FormDeserializeErr::UnsupportedType(String::from(t)))
    }

    /// Checks that the current [`Value`] is of the correct structure and the attribute tag matches
    /// [`name`].
    fn check_struct_access(
        &mut self,
        name: &'static str,
    ) -> std::result::Result<(), FormDeserializeErr> where {
        if let DeserializerState::None = &self.current_state.deserializer_state {
            self.current_state.value = Some(self.input);
        }

        match &self.current_state.value {
            Some(value) => {
                if let Value::Record(attrs, _items) = value {
                    match attrs.first() {
                        Some(a) => {
                            if a.name == name {
                                Ok(())
                            } else {
                                Err(FormDeserializeErr::Malformatted)
                            }
                        }
                        None => Err(FormDeserializeErr::Message(String::from("Missing tag"))),
                    }
                } else {
                    self.err_incorrect_type("Value::Record", Some(&Value::Extant))
                }
            }
            None => Err(FormDeserializeErr::Message(String::from("Missing record"))),
        }
    }

    pub fn err_incorrect_type<V>(
        &self,
        expected: &str,
        actual: Option<&Value>,
    ) -> std::result::Result<V, FormDeserializeErr> {
        match actual {
            Some(v) => Err(FormDeserializeErr::IncorrectType(format!(
                "Expected: {}, found: {}",
                expected,
                v.kind()
            ))),
            None => Err(FormDeserializeErr::Message(String::from("Missing value"))),
        }
    }

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
