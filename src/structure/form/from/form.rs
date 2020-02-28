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

use std::error::Error;
use std::fmt;
use std::fmt::Display;

use serde::{de, ser, Serialize};

use crate::model::{Attr, Item, Value};
use crate::structure::form::from::{Result, SerializerState, State, ValueSerializer};
use crate::structure::form::SerializerError;

impl ser::Error for SerializerError {
    fn custom<T: Display>(msg: T) -> Self {
        SerializerError::Message(msg.to_string())
    }
}

impl de::Error for SerializerError {
    fn custom<T: Display>(msg: T) -> Self {
        SerializerError::Message(msg.to_string())
    }
}

impl Display for SerializerError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(Error::description(self))
    }
}

impl std::error::Error for SerializerError {
    fn description(&self) -> &str {
        match *self {
            SerializerError::Message(ref msg) => msg,
            _ => "TODO",
        }
    }
}

impl<'a> ser::SerializeSeq for &'a mut ValueSerializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut ValueSerializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut ValueSerializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut ValueSerializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeMap for &'a mut ValueSerializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        if let SerializerState::ReadingMap(ref mut b) = self.current_state.serializer_state {
            *b = true;
        } else {
            panic!("Illegal state")
        }
        key.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        if let SerializerState::ReadingMap(ref mut b) = self.current_state.serializer_state {
            *b = false;
        } else {
            panic!("Illegal state")
        }

        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut ValueSerializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.current_state.attr_name = Some(key.to_owned());
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();

        Ok(())
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut ValueSerializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.current_state.attr_name = Some(key.to_owned());
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

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

    pub fn pop_state(&mut self) -> State {
        match self.stack.pop() {
            Some(s) => s,
            None => panic!("Stack underflow"),
        }
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
