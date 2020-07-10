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

use serde::{Serialize, Serializer};

use common::model::{Attr, Value};

use crate::{FormSerializeErr, SerializerResult, SerializerState, ValueSerializer};
use num_bigint::{BigInt, BigUint};
use std::str::FromStr;

pub const BIG_INT_PREFIX: &str = "____BIG___INT___";
pub const BIG_UINT_PREFIX: &str = "____BIG___UINT___";

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

    fn serialize_bool(self, v: bool) -> SerializerResult<()> {
        self.push_value(Value::from(v))
    }

    fn serialize_i8(self, v: i8) -> SerializerResult<()> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> SerializerResult<()> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> SerializerResult<()> {
        self.push_value(Value::from(v))
    }

    fn serialize_i64(self, v: i64) -> SerializerResult<()> {
        self.push_value(Value::Int64Value(v))
    }

    fn serialize_u8(self, _v: u8) -> SerializerResult<()> {
        self.err_unsupported("u8")
    }

    fn serialize_u16(self, _v: u16) -> SerializerResult<()> {
        self.err_unsupported("u16")
    }

    fn serialize_u32(self, _v: u32) -> SerializerResult<()> {
        self.err_unsupported("u32")
    }

    fn serialize_u64(self, _v: u64) -> SerializerResult<()> {
        self.err_unsupported("u64")
    }

    fn serialize_f32(self, v: f32) -> SerializerResult<()> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> SerializerResult<()> {
        self.push_value(Value::Float64Value(v))
    }

    fn serialize_char(self, v: char) -> SerializerResult<()> {
        self.push_value(Value::Text(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> SerializerResult<()> {
        if v.starts_with(BIG_INT_PREFIX) {
            let s = v.split(BIG_INT_PREFIX).collect::<Vec<&str>>();
            match BigInt::from_str(s.get(1).unwrap()) {
                Ok(bi) => {
                    self.push_value(Value::BigInt(bi));
                    Ok(())
                }
                Err(e) => Err(FormSerializeErr::Message(e.to_string())),
            }
        } else if v.starts_with(BIG_UINT_PREFIX) {
            let s = v.split(BIG_UINT_PREFIX).collect::<Vec<&str>>();
            match BigUint::from_str(s.get(1).unwrap()) {
                Ok(bi) => {
                    self.push_value(Value::BigUint(bi));
                    Ok(())
                }
                Err(e) => Err(FormSerializeErr::Message(e.to_string())),
            }
        } else {
            self.push_value(Value::Text(String::from(v)));
            Ok(())
        }
    }

    fn serialize_bytes(self, _v: &[u8]) -> SerializerResult<()> {
        self.err_unsupported("u8")
    }

    fn serialize_none(self) -> SerializerResult<()> {
        self.push_value(Value::Extant)
    }

    fn serialize_some<T>(self, value: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> SerializerResult<()> {
        self.push_value(Value::of_attr("Unit"))
    }

    fn serialize_unit_struct(self, name: &'static str) -> SerializerResult<()> {
        self.enter_nested(SerializerState::ReadingNested);
        self.push_attr(Attr::from(name));
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> SerializerResult<()> {
        self.enter_nested(SerializerState::ReadingNested);
        self.current_state.serializer_state = SerializerState::ReadingEnumName;

        let result = self.serialize_str(variant);
        self.current_state.serializer_state = SerializerState::ReadingNested;
        self.exit_nested();

        result
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        self.enter_nested(SerializerState::ReadingNested);
        self.push_attr(Attr::from(name));
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> SerializerResult<()>
    where
        T: ?Sized + Serialize,
    {
        self.push_attr(Attr::from(name));
        self.current_state.attr_name = Some(variant.to_owned());
        value.serialize(&mut *self)?;
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> SerializerResult<Self::SerializeSeq> {
        self.enter_nested(SerializerState::ReadingNested);
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> SerializerResult<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> SerializerResult<Self::SerializeTupleStruct> {
        match self.serialize_seq(Some(len)) {
            Ok(s) => {
                s.push_attr(Attr::from(name));
                Ok(s)
            }
            Err(e) => Err(e),
        }
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> SerializerResult<Self::SerializeTupleVariant> {
        self.enter_nested(SerializerState::ReadingEnumName);
        variant.serialize(&mut *self)?;
        self.current_state.serializer_state = SerializerState::ReadingNested;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> SerializerResult<Self::SerializeMap> {
        self.enter_nested(SerializerState::ReadingMap(false));
        Ok(self)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> SerializerResult<Self::SerializeStruct> {
        self.enter_nested(SerializerState::ReadingNested);
        self.push_attr(Attr::from(name));
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> SerializerResult<Self::SerializeStructVariant> {
        if self.enter_ext(name) {
            Ok(self)
        } else {
            self.enter_nested(SerializerState::ReadingEnumName);
            variant.serialize(&mut *self)?;
            self.current_state.serializer_state = SerializerState::ReadingNested;

            Ok(self)
        }
    }
}
