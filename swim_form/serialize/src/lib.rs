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
use serde::ser;
use std::fmt::{Debug, Display, Formatter};

use swim_common::model::blob::Blob;
use swim_common::model::{Attr, Item, Value, ValueKind};

pub type SerializerResult<T> = ::std::result::Result<T, FormSerializeErr>;
const EXT_BLOB: &str = swim_common::model::blob::EXT_BLOB;

#[cfg(test)]
mod tests;

pub mod bigint;
mod collection_access;
mod serializer;
mod struct_access;

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
        match self {
            FormSerializeErr::Message(m) => write!(f, "{}", m),
            FormSerializeErr::UnsupportedType(t) => write!(f, "Unsupported type: {}", t),
            FormSerializeErr::IncorrectType(t) => write!(f, "Incorrect type: {}", t),
            FormSerializeErr::IllegalItem(i) => write!(f, "Illegal item: {}", i),
            FormSerializeErr::IllegalState(s) => write!(f, "Illegal state: {}", s),
            FormSerializeErr::Malformatted => write!(f, "Malformatted"),
        }
    }
}

pub struct ValueSerializer {
    current_state: State,
    stack: Vec<State>,
}

#[derive(Clone)]
pub struct State {
    pub output: Value,
    pub serializer_state: SerializerState,
    pub attr_name: Option<String>,
}

#[derive(Clone)]
pub enum SerializerState {
    ReadingNested,
    ReadingEnumName,
    // Reading key
    ReadingMap(bool),
    None,
    // Reading a special variant. Such as a blob or big integer.
    ReadingExt(ValueKind),
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

    pub fn push_value(&mut self, value: Value) -> SerializerResult<()> {
        match &mut self.current_state.serializer_state {
            SerializerState::ReadingExt(vk) => {
                // At this point we are an element deeper than we need to be. The current state needs
                // to be poppped, the Value transformed into the correct type and pushed up into the
                // correct position so the field's name matches the value.
                let r = match (vk, value) {
                    (ValueKind::Data, Value::Text(s)) => {
                        let b = Blob::from_encoded(Vec::from(s.as_bytes()));
                        Value::Data(b)
                    }
                    _ => unreachable!(),
                };

                self.current_state = self.stack.pop().expect("Serializer in an illegal state.");
                self.push_value(r)?
            }
            _ => match &mut self.current_state.output {
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
                v => unreachable!("{:?}", v),
            },
        }
        Ok(())
    }

    pub fn err_unsupported<V>(&self, t: &str) -> std::result::Result<V, FormSerializeErr> {
        Err(FormSerializeErr::UnsupportedType(String::from(t)))
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

    /// Changes the serializer's state to accept a known Value variant provided by a key `ext_name`.
    /// This operation should only be used *once* per field that requires it and only a single value
    /// should be pushed onto the current state.
    ///
    /// Returns whether or not the provided key was of a known Value variant.
    ///
    /// * Note:
    /// Implementations of serialize and deserialize must be generic across all serializers and
    /// deserializers and so provide no way to provide a specialised implementation without nightly's
    /// specialisation. As a result of this, structures that can be represented as a single field
    /// - such as a big integer or a BLOB - will be serialized incorrectly; such as into Value::Text.
    /// TypeId::of<T>() requires that the function has a static lifetime and Serde objects may not have
    /// a static lifetime, and intrinsics::type_name may not return a consistent name across
    /// compiler versions that can be checked when serializing an object.
    ///
    /// The only consistent approach to serializing these correctly is for structures to use a special
    /// serialize implementation that passes in its type name as a string that this serializer
    /// can use to map the current type to the correct Value variant. This happens *before* the
    /// actual value is serialized so that the serializer's state can be setup to accept and map the
    /// subsequent value to the correct Value variant.
    ///
    /// As a result of this, to use Serde's JSON serializer/deserializer (as well as others), the
    /// structure must be mimicked and have the `#[serde(with = ...)]` / `#[form(..)]` attributes
    /// removed so that this function is not called; otherwise, there will be an extra field in the output.
    #[doc(hidden)]
    pub fn enter_ext(&mut self, ext_name: &'static str) -> bool {
        match ext_name {
            EXT_BLOB => {
                self.push_state(State::new_with_state(SerializerState::ReadingExt(
                    ValueKind::Data,
                )));

                true
            }
            _ => {
                // no op as the name will be the structure's actual name
                false
            }
        }
    }
}
