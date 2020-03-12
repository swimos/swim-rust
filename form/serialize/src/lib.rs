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

use serde::ser;

use core::fmt;
use std::fmt::{Debug, Display, Formatter};

pub type Result<T> = ::std::result::Result<T, FormSerializeErr>;

#[cfg(test)]
mod tests;

mod collection_access;
mod serializer;
mod struct_access;

use common::model::{Attr, Item, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum FormSerializeErr {
    Message(String),
    UnsupportedType(String),
    IncorrectType(Value),
    IllegalItem(Item),
    IllegalState(String),
    Malformatted,
}

impl Display for FormSerializeErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_string())
    }
}

#[derive(Debug)]
pub struct ValueSerializer {
    current_state: State,
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
    ReadingEnumName,
    // Reading key
    ReadingMap(bool),
    None,
}

impl ser::Error for FormSerializeErr {
    fn custom<T: Display>(msg: T) -> Self {
        FormSerializeErr::Message(msg.to_string())
    }
}

impl std::error::Error for FormSerializeErr {}

impl State {
    fn default() -> Self {
        State {
            output: Value::Extant,
            serializer_state: SerializerState::None,
            attr_name: None,
        }
    }

    fn new_with_state(parser_state: SerializerState) -> Self {
        State {
            output: Value::Record(Vec::new(), Vec::new()),
            serializer_state: parser_state,
            attr_name: None,
        }
    }
}

impl Default for ValueSerializer {
    fn default() -> Self {
        Self {
            current_state: State::default(),
            stack: vec![],
        }
    }
}

impl ValueSerializer {
    pub fn output(&mut self) -> Value {
        self.current_state.output.to_owned()
    }

    pub fn push_state(&mut self, ss: State) {
        self.stack.push(self.current_state.to_owned());
        self.current_state = ss;
    }

    pub fn push_attr(&mut self, attr: Attr) {
        if let Value::Record(attrs, _) = &mut self.current_state.output {
            attrs.push(attr);
        }
    }

    pub fn push_value(&mut self, value: Value) {
        match &mut self.current_state.output {
            Value::Record(ref mut attrs, ref mut items) => {
                match self.current_state.serializer_state {
                    SerializerState::ReadingMap(reading_key) => {
                        if reading_key {
                            let item = Item::Slot(value, Value::Extant);
                            items.push(item);
                        } else if let Some(last_item) = items.last_mut() {
                            match last_item {
                                Item::Slot(_, ref mut val) => {
                                    *val = value;
                                }
                                i => {
                                    panic!("Illegal state. Incorrect item type: {:?}", i);
                                }
                            }
                        }
                    }
                    SerializerState::ReadingEnumName => match value {
                        Value::Text(s) => attrs.push(Attr::from(s)),
                        v => panic!("Illegal type for attribute: {:?}", v),
                    },
                    _ => {
                        let item = match &self.current_state.attr_name {
                            Some(name) => Item::Slot(Value::Text(name.to_owned()), value),
                            None => Item::ValueItem(value),
                        };

                        items.push(item)
                    }
                }
            }
            Value::Extant => {
                self.current_state.output = value;
            }
            v => unimplemented!("{:?}", v),
        }
    }

    pub fn enter_nested(&mut self, state: SerializerState) {
        if let SerializerState::None = self.current_state.serializer_state {
            self.current_state.serializer_state = state;
            self.current_state.output = Value::Record(Vec::new(), Vec::new());
        } else {
            if let Value::Record(_, ref mut items) = &mut self.current_state.output {
                if let SerializerState::ReadingNested = self.current_state.serializer_state {
                    match &self.current_state.attr_name {
                        Some(name) => {
                            items.push(Item::Slot(Value::Text(name.to_owned()), Value::Extant));
                        }
                        None => {
                            items.push(Item::ValueItem(Value::Extant));
                        }
                    }
                }
            }
            self.push_state(State::new_with_state(state));
        }
    }

    pub fn exit_nested(&mut self) {
        if let Some(mut previous_state) = self.stack.pop() {
            if let SerializerState::ReadingNested = self.current_state.serializer_state {
                if let Value::Record(_, ref mut items) = previous_state.output {
                    if let Some(item) = items.last_mut() {
                        match item {
                            Item::Slot(_, ref mut v @ Value::Extant) => {
                                *v = self.current_state.output.to_owned();
                            }
                            Item::ValueItem(ref mut v) => {
                                *v = self.current_state.output.to_owned();
                            }
                            _ => {
                                items.push(Item::ValueItem(self.current_state.output.to_owned()));
                            }
                        }
                    } else {
                        items.push(Item::ValueItem(self.current_state.output.to_owned()));
                    }
                }
            }
            self.current_state = previous_state;
        }
    }
}
