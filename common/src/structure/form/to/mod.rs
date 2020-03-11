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

use std::fmt::Debug;
use std::fmt::Display;

use serde::{de, ser, Serialize, Serializer};

use crate::model::{Attr, Item, Value};
use crate::structure::form::FormParseErr;

#[cfg(test)]
mod tests;

use super::Result;
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};

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

// CLion/IntelliJ believes there is a missing implementation
//noinspection RsTraitImplementation
impl<'a> Serializer for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormParseErr;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.push_value(Value::from(v));
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.push_value(Value::from(v));
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.push_value(Value::Int64Value(v));
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        Err(FormParseErr::UnsupportedType(String::from("u8")))
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        Err(FormParseErr::UnsupportedType(String::from("u16")))
    }

    fn serialize_u32(self, _v: u32) -> Result<()> {
        Err(FormParseErr::UnsupportedType(String::from("u32")))
    }

    fn serialize_u64(self, _v: u64) -> Result<()> {
        Err(FormParseErr::UnsupportedType(String::from("u64")))
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.push_value(Value::Float64Value(v));
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.push_value(Value::Text(v.to_string()));
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.push_value(Value::Text(String::from(v)));
        Ok(())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        Err(FormParseErr::UnsupportedType(String::from("u8")))
    }

    fn serialize_none(self) -> Result<()> {
        self.push_value(Value::Extant);
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        self.push_value(Value::of_attr("Unit"));
        Ok(())
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        self.push_attr(Attr::from(name));
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.enter_nested(SerializerState::ReadingNested);
        self.current_state.serializer_state = SerializerState::ReadingEnumName;

        let result = self.serialize_str(variant);
        self.current_state.serializer_state = SerializerState::ReadingNested;
        self.exit_nested();

        result
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.push_attr(Attr::from(name));
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.push_attr(Attr::from(name));
        self.current_state.attr_name = Some(variant.to_owned());
        value.serialize(&mut *self)?;
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.enter_nested(SerializerState::ReadingNested);
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.push_attr(Attr::from(name));
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.enter_nested(SerializerState::ReadingEnumName);
        variant.serialize(&mut *self)?;
        self.current_state.serializer_state = SerializerState::ReadingNested;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.enter_nested(SerializerState::ReadingMap(false));
        Ok(self)
    }

    fn serialize_struct(self, name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.enter_nested(SerializerState::ReadingNested);
        self.push_attr(Attr::from(name));
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.enter_nested(SerializerState::ReadingEnumName);
        variant.serialize(&mut *self)?;
        self.current_state.serializer_state = SerializerState::ReadingNested;
        Ok(self)
    }
}

impl ser::Error for FormParseErr {
    fn custom<T: Display>(msg: T) -> Self {
        FormParseErr::Message(msg.to_string())
    }
}

impl de::Error for FormParseErr {
    fn custom<T: Display>(msg: T) -> Self {
        FormParseErr::Message(msg.to_string())
    }
}

impl std::error::Error for FormParseErr {
    fn description(&self) -> &str {
        match *self {
            FormParseErr::Message(ref msg) => msg,
            _ => "TODO",
        }
    }
}

impl<'a> SerializeSeq for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormParseErr;

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

impl<'a> SerializeTuple for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormParseErr;

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

impl<'a> SerializeTupleStruct for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormParseErr;

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

impl<'a> SerializeTupleVariant for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormParseErr;

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

impl<'a> SerializeMap for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormParseErr;

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

impl<'a> SerializeStruct for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormParseErr;

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

impl<'a> SerializeStructVariant for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormParseErr;

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
