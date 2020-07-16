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

use crate::writer::WriteValueError;
use crate::{Form, TransmuteValue};
use common::model::{Attr, Item, Value};

pub struct ValueWriter {
    current_state: State,
    stack: Vec<State>,
}

#[derive(Clone)]
pub struct State {
    pub output: Value,
    pub writer_state: WriterState,
    pub attr_name: Option<String>,
    reading_attr: bool,
}

#[derive(Clone, Debug)]
pub enum WriterState {
    // Nested length
    ReadingNested(Option<usize>),
    ReadingEnumName,
    // Reading key
    ReadingMap(bool),
    None,
}

impl State {
    fn default() -> Self {
        State {
            output: Value::Extant,
            writer_state: WriterState::None,
            attr_name: None,
            reading_attr: false,
        }
    }

    fn new_with_state(parser_state: WriterState) -> Self {
        State {
            output: Value::Record(Vec::new(), Vec::new()),
            writer_state: parser_state,
            attr_name: None,
            reading_attr: false,
        }
    }
}

impl Default for ValueWriter {
    fn default() -> Self {
        Self {
            current_state: State::default(),
            stack: vec![],
        }
    }
}

impl ValueWriter {
    pub fn finish(&mut self) -> Value {
        self.current_state.output.to_owned()
    }

    fn push_state(&mut self, ss: State) {
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
            Value::Record(ref mut attrs, ref mut items) => match self.current_state.writer_state {
                WriterState::ReadingMap(reading_key) => {
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
                WriterState::ReadingEnumName => match value {
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
            },
            Value::Extant => {
                self.current_state.output = value;
            }
            v => unimplemented!("{:?}", v),
        }
    }

    pub fn err_unsupported<V>(&self, t: &str) -> std::result::Result<V, WriteValueError> {
        Err(WriteValueError::UnsupportedType(String::from(t)))
    }

    pub fn enter_nested(&mut self, state: WriterState) {
        if let WriterState::None = self.current_state.writer_state {
            self.current_state.writer_state = state;
            self.current_state.output = Value::Record(Vec::new(), Vec::new());
        } else {
            if let Value::Record(_, ref mut items) = &mut self.current_state.output {
                if let WriterState::ReadingNested(opt_len) = self.current_state.writer_state {
                    match &self.current_state.attr_name {
                        Some(name) => {
                            items.push(Item::Slot(Value::Text(name.to_owned()), Value::Extant))
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

    // todo remove all the `to_owned` calls
    pub fn exit_nested(&mut self) {
        if let Some(mut previous_state) = self.stack.pop() {
            if let WriterState::ReadingNested(_) = self.current_state.writer_state {
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

    fn exit_field(&mut self) {
        self.current_state.reading_attr = false;
    }

    pub fn transmute_attr<F>(&mut self, name: &'static str, f: &F)
    where
        F: TransmuteValue,
    {
        f.transmute_to_value(self);

        match &mut self.current_state.output {
            Value::Record(attrs, items) => match items.pop() {
                Some(Item::ValueItem(value)) => {
                    attrs.push(Attr::of((name, value)));
                }

                v => unreachable!(
                    "Attempted to pull up a key-value pair as an attribute: {:?}",
                    v
                ),
            },
            _ => panic!("Attempted to pull up a unit value"),
        }
    }

    pub fn serialize_field<F>(&mut self, name_opt: Option<&'static str>, f: &F)
    where
        F: SerializeToValue,
    {
        if let Some(name) = name_opt {
            self.current_state.attr_name = Some(name.to_owned());
        }

        f.serialize(self, properties);
    }

    pub fn transmute_struct(&mut self, name: &'static str, len: usize) {
        self.enter_nested(WriterState::ReadingNested(Some(len)));
        self.push_attr(Attr::from(name));
    }

    pub fn transmute_sequence<S>(&mut self, seq: S)
    where
        S: IntoIterator,
        <S as IntoIterator>::Item: TransmuteValue,
    {
        let it = seq.into_iter();
        match it.size_hint() {
            (lb, Some(ub)) if lb == ub => {
                self.enter_nested(WriterState::ReadingNested(Some(lb)));
            }
            _ => self.enter_nested(WriterState::ReadingNested(None)),
        }

        it.for_each(|e| e.transmute_to_value(self));

        self.exit_nested();
    }
}
