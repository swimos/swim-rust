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

use crate::model::{Item, Value};

pub type Result<T> = ::std::result::Result<T, SerializerError>;

#[derive(Clone, Debug, PartialEq)]
pub enum SerializerError {
    Message(String),
    UnsupportedType(String),
}

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
            _ => "TODO"
        }
    }
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

//noinspection ALL,RsTraitImplementation
impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = SerializerError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.push_value(Value::BooleanValue(v));
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.push_value(Value::Int32Value(v));
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.push_value(Value::Int64Value(v));
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        Err(SerializerError::UnsupportedType(String::from("u8")))
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        Err(SerializerError::UnsupportedType(String::from("u16")))
    }

    fn serialize_u32(self, _v: u32) -> Result<()> {
        Err(SerializerError::UnsupportedType(String::from("u32")))
    }

    fn serialize_u64(self, _v: u64) -> Result<()> {
        Err(SerializerError::UnsupportedType(String::from("u64")))
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
        Err(SerializerError::UnsupportedType(String::from("u8")))
    }

    fn serialize_none(self) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<()> where T: ?Sized + Serialize {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        self.push_value(Value::Extant);
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()> where T: ?Sized + Serialize {
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
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        variant.serialize(&mut *self)?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.enter_nested(SerializerState::ReadingMap(false));
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct> {
        self.enter_nested(SerializerState::ReadingNested);
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        variant.serialize(&mut *self)?;
        Ok(self)
    }
}

impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>        where            T: ?Sized + Serialize    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>        where            T: ?Sized + Serialize    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut Serializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()> where T: ?Sized + Serialize {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()> where T: ?Sized + Serialize {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();
        Ok(())
    }
}

impl<'a> ser::SerializeMap for &'a mut Serializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()> where T: ?Sized + Serialize {
        if let SerializerState::ReadingMap(ref mut b) = self.current_state.serializer_state {
            *b = true;
        } else {
            panic!("Illegal state")
        }
        key.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()> where T: ?Sized + Serialize {
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

impl<'a> ser::SerializeStruct for &'a mut Serializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()> where T: ?Sized + Serialize {
        self.current_state.attr_name = Some(key.to_owned());
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();

        Ok(())
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut Serializer {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>        where            T: ?Sized + Serialize    {
        self.current_state.attr_name = Some(key.to_owned());
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.exit_nested();

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum SerializerState {
    ReadingNested,
    // Reading key
    ReadingMap(bool),
    None,
}

#[derive(Debug)]
pub struct Serializer {
    pub current_state: State,
    stack: Vec<State>,
}

#[derive(Debug, Clone)]
pub struct State {
    pub  output: Value,
    pub serializer_state: SerializerState,
    pub attr_name: Option<String>,
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

impl Serializer {
    pub fn new() -> Self {
        Self {
            current_state: State::default(),
            stack: vec![],
        }
    }

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
            None => {
                panic!("Stack underflow")
            }
        }
    }

    pub fn push_value(&mut self, value: Value) {
        match &mut self.current_state.output {
            Value::Record(_, ref mut items) => {
                match self.current_state.serializer_state {
                    SerializerState::ReadingMap(reading_key) => {
                        match reading_key {
                            true => {
                                let item = Item::Slot(value, Value::Extant);
                                items.push(item);
                            }
                            false => {
                                match items.last_mut() {
                                    Some(last_item) => {
                                        match last_item {
                                            Item::Slot(_, ref mut val) => {
                                                *val = value;
                                            }
                                            i @ _ => {
                                                panic!("Illegal state. Incorrect item type: {:?}", i);
                                            }
                                        }
                                    }
                                    None => {}
                                }
                            }
                        }
                    }
                    _ => {
                        let item = match &self.current_state.attr_name {
                            Some(name) => {
                                Item::Slot(Value::Text(name.to_owned()), value)
                            }
                            None => {
                                Item::ValueItem(value)
                            }
                        };

                        items.push(item)
                    }
                }
            }
            Value::Extant => {
                self.current_state.output = value;
            }
            v @ _ => {
                unimplemented!("{:?}", v)
            }
        }
    }

    pub fn enter_nested(&mut self, state: SerializerState) {
        if let SerializerState::None = self.current_state.serializer_state {
            self.current_state.serializer_state = state;
            self.current_state.output = Value::Record(Vec::new(), Vec::new());
        } else {
            match &mut self.current_state.output {
                Value::Record(_, ref mut items) => {
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
                _ => {
                    panic!()
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
            self.current_state = previous_state.to_owned();
        }
    }
}