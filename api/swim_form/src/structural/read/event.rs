// Copyright 2015-2021 SWIM.AI inc.
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

use crate::structural::read::error::ExpectedEvent;
use crate::structural::read::ReadError;
use swim_model::Text;
use swim_model::ValueKind;
use swim_model::bigint::{BigInt, BigUint};
use std::borrow::Cow;

/// Reading a serialized representation of a record in the Swim data model produces
/// a stream of these events. An event is either a token, a notification that an
/// attribute or record body has started or ended or a notifcation of a slot (this will
/// occur between the slot key and the slot value). If a string does not require escaping
/// it will be provided as a reference into the original input rather than as separate
/// allocation.
#[derive(Debug, PartialEq, Clone)]
pub enum ReadEvent<'a> {
    Extant,
    TextValue(Cow<'a, str>),
    Number(NumericValue),
    Boolean(bool),
    Blob(Vec<u8>),
    StartAttribute(Cow<'a, str>),
    EndAttribute,
    StartBody,
    Slot,
    EndRecord,
}

impl<'a> ReadEvent<'a> {
    pub fn kind_error(&self, expected: ExpectedEvent) -> ReadError {
        let expected = Some(expected);
        match self {
            ReadEvent::Number(NumericValue::Int(_)) => {
                ReadError::unexpected_kind(ValueKind::Int64, expected)
            }
            ReadEvent::Number(NumericValue::UInt(_)) => {
                ReadError::unexpected_kind(ValueKind::UInt64, expected)
            }
            ReadEvent::Number(NumericValue::BigInt(_)) => {
                ReadError::unexpected_kind(ValueKind::BigInt, expected)
            }
            ReadEvent::Number(NumericValue::BigUint(_)) => {
                ReadError::unexpected_kind(ValueKind::BigUint, expected)
            }
            ReadEvent::Number(NumericValue::Float(_)) => {
                ReadError::unexpected_kind(ValueKind::Float64, expected)
            }
            ReadEvent::Boolean(_) => ReadError::unexpected_kind(ValueKind::Boolean, expected),
            ReadEvent::TextValue(_) => ReadError::unexpected_kind(ValueKind::Text, expected),
            ReadEvent::Extant => ReadError::unexpected_kind(ValueKind::Extant, expected),
            ReadEvent::Blob(_) => ReadError::unexpected_kind(ValueKind::Data, expected),
            ReadEvent::StartBody | ReadEvent::StartAttribute(_) => {
                ReadError::unexpected_kind(ValueKind::Record, expected)
            }
            _ => ReadError::InconsistentState,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum NumericValue {
    Int(i64),
    UInt(u64),
    BigInt(BigInt),
    BigUint(BigUint),
    Float(f64),
}

impl<'a> From<u8> for ReadEvent<'a> {
    fn from(n: u8) -> Self {
        ReadEvent::Number(NumericValue::Int(n.into()))
    }
}

impl<'a> From<i16> for ReadEvent<'a> {
    fn from(n: i16) -> Self {
        ReadEvent::Number(NumericValue::Int(n.into()))
    }
}

impl<'a> From<u16> for ReadEvent<'a> {
    fn from(n: u16) -> Self {
        ReadEvent::Number(NumericValue::UInt(n.into()))
    }
}

impl<'a> From<i8> for ReadEvent<'a> {
    fn from(n: i8) -> Self {
        ReadEvent::Number(NumericValue::Int(n.into()))
    }
}

impl<'a> From<i32> for ReadEvent<'a> {
    fn from(n: i32) -> Self {
        ReadEvent::Number(NumericValue::Int(n.into()))
    }
}

impl<'a> From<i64> for ReadEvent<'a> {
    fn from(n: i64) -> Self {
        ReadEvent::Number(NumericValue::Int(n))
    }
}

impl<'a> From<u32> for ReadEvent<'a> {
    fn from(n: u32) -> Self {
        ReadEvent::Number(NumericValue::UInt(n.into()))
    }
}

impl<'a> From<u64> for ReadEvent<'a> {
    fn from(n: u64) -> Self {
        ReadEvent::Number(NumericValue::UInt(n))
    }
}

impl<'a> From<f64> for ReadEvent<'a> {
    fn from(x: f64) -> Self {
        ReadEvent::Number(NumericValue::Float(x))
    }
}

impl<'a> From<f32> for ReadEvent<'a> {
    fn from(x: f32) -> Self {
        ReadEvent::Number(NumericValue::Float(x.into()))
    }
}

impl<'a> From<BigInt> for ReadEvent<'a> {
    fn from(n: BigInt) -> Self {
        ReadEvent::Number(NumericValue::BigInt(n))
    }
}

impl<'a> From<BigUint> for ReadEvent<'a> {
    fn from(n: BigUint) -> Self {
        ReadEvent::Number(NumericValue::BigUint(n))
    }
}

impl<'a> From<&'a str> for ReadEvent<'a> {
    fn from(s: &'a str) -> Self {
        ReadEvent::TextValue(Cow::Borrowed(s))
    }
}

impl<'a> From<String> for ReadEvent<'a> {
    fn from(s: String) -> Self {
        ReadEvent::TextValue(Cow::Owned(s))
    }
}

impl<'a> From<Cow<'a, str>> for ReadEvent<'a> {
    fn from(s: Cow<'a, str>) -> Self {
        ReadEvent::TextValue(s)
    }
}

impl<'a> From<Text> for ReadEvent<'a> {
    fn from(s: Text) -> Self {
        ReadEvent::TextValue(Cow::Owned(s.to_string()))
    }
}

impl<'a> From<bool> for ReadEvent<'a> {
    fn from(p: bool) -> Self {
        ReadEvent::Boolean(p)
    }
}

impl<'a> From<Vec<u8>> for ReadEvent<'a> {
    fn from(blob: Vec<u8>) -> Self {
        ReadEvent::Blob(blob)
    }
}
