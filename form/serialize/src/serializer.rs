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

use crate::{FormSerializeErr, Result, SerializerState, ValueSerializer};
use common::model::{Attr, Value};
use serde::{Serialize, Serializer};

// CLion/IntelliJ believes there is a missing implementation
//noinspection RsTraitImplementation
impl<'a> Serializer for &'a mut ValueSerializer {
    type Ok = ();
    type Error = FormSerializeErr;

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
        self.err_unsupported("u8")
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        self.err_unsupported("u16")
    }

    fn serialize_u32(self, _v: u32) -> Result<()> {
        self.err_unsupported("u32")
    }

    fn serialize_u64(self, _v: u64) -> Result<()> {
        self.err_unsupported("u64")
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
        self.err_unsupported("u8")
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
