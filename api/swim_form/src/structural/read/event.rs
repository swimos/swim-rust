// Copyright 2015-2021 Swim Inc.
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
use num_traits::ToPrimitive;
use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use swim_model::bigint::{BigInt, BigUint, ToBigInt};
use swim_model::Text;
use swim_model::ValueKind;

/// Reading a serialized representation of a record in the Swim data model produces
/// a stream of these events. An event is either a token, a notification that an
/// attribute or record body has started or ended or a notification of a slot (this will
/// occur between the slot key and the slot value). If a string does not require escaping
/// it will be provided as a reference into the original input rather than as separate
/// allocation.
#[derive(Debug, PartialEq, Clone, Hash)]
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

#[derive(Debug, Clone)]
pub enum NumericValue {
    Int(i64),
    UInt(u64),
    BigInt(BigInt),
    BigUint(BigUint),
    Float(f64),
}

impl Hash for NumericValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        const INT_HASH: u8 = 0;
        const BIGINT_HASH: u8 = 1;
        const FLOAT64_HASH: u8 = 2;

        match self {
            NumericValue::Int(n) => {
                state.write_u8(INT_HASH);
                state.write_i128(*n as i128);
            }
            NumericValue::UInt(n) => {
                state.write_u8(INT_HASH);
                state.write_i128(*n as i128);
            }
            NumericValue::BigInt(bi) => {
                if let Some(n) = bi.to_i128() {
                    state.write_u8(INT_HASH);
                    state.write_i128(n as i128);
                } else {
                    state.write_u8(BIGINT_HASH);
                    bi.hash(state);
                }
            }
            NumericValue::BigUint(bi) => {
                if let Some(n) = bi.to_i128() {
                    state.write_u8(INT_HASH);
                    state.write_i128(n as i128);
                } else if let Some(n) = bi.to_bigint() {
                    state.write_u8(BIGINT_HASH);
                    n.hash(state);
                } else {
                    unreachable!();
                }
            }
            NumericValue::Float(x) => {
                state.write_u8(FLOAT64_HASH);
                if x.is_nan() {
                    state.write_u64(0);
                } else {
                    state.write_u64(x.to_bits());
                }
            }
        }
    }
}

impl PartialEq for NumericValue {
    fn eq(&self, other: &Self) -> bool {
        match self {
            NumericValue::Int(n) => match other {
                NumericValue::Int(m) => n == m,
                NumericValue::UInt(m) => i64::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                NumericValue::BigInt(big_m) => big_m.to_i64().map(|ref m| n == m).unwrap_or(false),
                NumericValue::BigUint(big_m) => big_m.to_i64().map(|ref m| n == m).unwrap_or(false),
                NumericValue::Float(_) => false,
            },
            NumericValue::UInt(n) => match other {
                NumericValue::Int(m) => u64::try_from(*m).map(|ref m| n == m).unwrap_or(false),
                NumericValue::UInt(m) => n == m,
                NumericValue::BigInt(big_m) => big_m.to_u64().map(|ref m| n == m).unwrap_or(false),
                NumericValue::BigUint(big_m) => big_m.to_u64().map(|ref m| n == m).unwrap_or(false),
                NumericValue::Float(_) => false,
            },
            NumericValue::BigInt(left) => match other {
                NumericValue::Int(right) => {
                    left.to_i64().map(|ref left| left == right).unwrap_or(false)
                }
                NumericValue::UInt(right) => {
                    left.to_u64().map(|ref left| left == right).unwrap_or(false)
                }
                NumericValue::BigInt(right) => left == right,
                NumericValue::BigUint(right) => right
                    .to_bigint()
                    .map(|ref right| left == right)
                    .unwrap_or(false),
                NumericValue::Float(_) => false,
            },
            NumericValue::BigUint(left) => match other {
                NumericValue::Int(right) => {
                    left.to_i64().map(|ref left| left == right).unwrap_or(false)
                }
                NumericValue::UInt(right) => {
                    left.to_u64().map(|ref left| left == right).unwrap_or(false)
                }
                NumericValue::BigInt(right) => left
                    .to_bigint()
                    .map(|ref left| left == right)
                    .unwrap_or(false),
                NumericValue::BigUint(right) => left == right,
                NumericValue::Float(_) => false,
            },
            NumericValue::Float(x) => match other {
                NumericValue::Float(y) => {
                    if x.is_nan() {
                        y.is_nan()
                    } else {
                        x == y
                    }
                }
                _ => false,
            },
        }
    }
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
