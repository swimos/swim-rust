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

use super::Recognizer;
use crate::form::structural::read::error::ExpectedEvent;
use crate::form::structural::read::event::{NumericValue, ReadEvent};
use crate::form::structural::read::ReadError;
use crate::model::text::Text;
use crate::model::ValueKind;
use num_bigint::{BigInt, BigUint};
use num_traits::ToPrimitive;
use std::convert::TryFrom;
use std::num::NonZeroUsize;

pub struct UnitRecognizer;
pub struct I32Recognizer;
pub struct I64Recognizer;
pub struct U32Recognizer;
pub struct U64Recognizer;
pub struct UsizeRecognizer;
pub struct NonZeroUsizeRecognizer;
pub struct BigIntRecognizer;
pub struct BigUintRecognizer;
pub struct F64Recognizer;
pub struct StringRecognizer;
pub struct TextRecognizer;
pub struct DataRecognizer;
pub struct BoolRecognizer;

impl Recognizer for UnitRecognizer {
    type Target = ();

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Extant => Some(Ok(())),
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Extant))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for I32Recognizer {
    type Target = i32;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => {
                Some(i32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::UInt(n)) => {
                Some(i32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigInt(n)) => {
                Some(i32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigUint(n)) => {
                Some(i32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Int32))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for I64Recognizer {
    type Target = i64;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => Some(Ok(n)),
            ReadEvent::Number(NumericValue::UInt(n)) => {
                Some(i64::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigInt(n)) => {
                Some(i64::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigUint(n)) => {
                Some(i64::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Int64))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for U32Recognizer {
    type Target = u32;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => {
                Some(u32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::UInt(n)) => {
                Some(u32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigInt(n)) => {
                Some(u32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigUint(n)) => {
                Some(u32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt32))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for U64Recognizer {
    type Target = u64;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => {
                Some(u64::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::UInt(n)) => Some(Ok(n)),
            ReadEvent::Number(NumericValue::BigInt(n)) => {
                Some(u64::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigUint(n)) => {
                Some(u64::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for UsizeRecognizer {
    type Target = usize;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => {
                Some(usize::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::UInt(n)) => {
                Some(usize::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigInt(n)) => {
                Some(usize::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigUint(n)) => {
                Some(usize::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                ExpectedEvent::ValueEvent(ValueKind::Int32),
                ExpectedEvent::ValueEvent(ValueKind::Int64),
                ExpectedEvent::ValueEvent(ValueKind::UInt32),
                ExpectedEvent::ValueEvent(ValueKind::UInt64),
                ExpectedEvent::ValueEvent(ValueKind::BigInt),
                ExpectedEvent::ValueEvent(ValueKind::BigUint),
            ])))),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for NonZeroUsizeRecognizer {
    type Target = NonZeroUsize;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => match usize::try_from(n) {
                Ok(value) => Some(NonZeroUsize::new(value).ok_or(ReadError::NumberOutOfRange)),
                Err(_) => Some(Err(ReadError::NumberOutOfRange)),
            },
            ReadEvent::Number(NumericValue::UInt(n)) => match usize::try_from(n) {
                Ok(value) => Some(NonZeroUsize::new(value).ok_or(ReadError::NumberOutOfRange)),
                Err(_) => Some(Err(ReadError::NumberOutOfRange)),
            },
            ReadEvent::Number(NumericValue::BigInt(n)) => match usize::try_from(n) {
                Ok(value) => Some(NonZeroUsize::new(value).ok_or(ReadError::NumberOutOfRange)),
                Err(_) => Some(Err(ReadError::NumberOutOfRange)),
            },
            ReadEvent::Number(NumericValue::BigUint(n)) => match usize::try_from(n) {
                Ok(value) => Some(NonZeroUsize::new(value).ok_or(ReadError::NumberOutOfRange)),
                Err(_) => Some(Err(ReadError::NumberOutOfRange)),
            },
            ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                ExpectedEvent::ValueEvent(ValueKind::UInt32),
                ExpectedEvent::ValueEvent(ValueKind::UInt64),
                ExpectedEvent::ValueEvent(ValueKind::BigUint),
            ])))),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for BigIntRecognizer {
    type Target = BigInt;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => Some(Ok(BigInt::from(n))),
            ReadEvent::Number(NumericValue::UInt(n)) => Some(Ok(BigInt::from(n))),
            ReadEvent::Number(NumericValue::BigInt(n)) => Some(Ok(n)),
            ReadEvent::Number(NumericValue::BigUint(n)) => Some(Ok(BigInt::from(n))),
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::BigInt))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for BigUintRecognizer {
    type Target = BigUint;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => {
                Some(BigUint::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::UInt(n)) => Some(Ok(BigUint::from(n))),
            ReadEvent::Number(NumericValue::BigInt(n)) => {
                Some(BigUint::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigUint(n)) => Some(Ok(n)),
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::BigUint))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for F64Recognizer {
    type Target = f64;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Float(x)) => Some(Ok(x)),
            ReadEvent::Number(NumericValue::Int(n)) => Some(Ok(n as f64)),
            ReadEvent::Number(NumericValue::UInt(n)) => Some(Ok(n as f64)),
            ReadEvent::Number(NumericValue::BigInt(n)) => {
                Some(n.to_f64().ok_or(ReadError::NumberOutOfRange))
            }
            ReadEvent::Number(NumericValue::BigUint(n)) => {
                Some(n.to_f64().ok_or(ReadError::NumberOutOfRange))
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Float64))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for StringRecognizer {
    type Target = String;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(string) => Some(Ok(string.into())),
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for TextRecognizer {
    type Target = Text;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(string) => Some(Ok(string.into())),
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for DataRecognizer {
    type Target = Vec<u8>;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Blob(v) => Some(Ok(v)),
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Data))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Recognizer for BoolRecognizer {
    type Target = bool;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Boolean(p) => Some(Ok(p)),
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Boolean))
            )),
        }
    }

    fn reset(&mut self) {}
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn unit_recognizer() {
        let mut rec = UnitRecognizer;
        assert_eq!(rec.feed_event(ReadEvent::Extant), Some(Ok(())));
    }

    #[test]
    fn i32_recognizer() {
        let mut rec = I32Recognizer;
        assert_eq!(rec.feed_event(ReadEvent::from(-3i32)), Some(Ok(-3i32)));
    }

    #[test]
    fn i64_recognizer() {
        let mut rec = I64Recognizer;
        let n: i64 = i64::from(i32::MIN) * 2;
        assert_eq!(rec.feed_event(n.into()), Some(Ok(n)));
    }

    #[test]
    fn u32_recognizer() {
        let mut rec = U32Recognizer;
        let n: u32 = 567u32;
        assert_eq!(rec.feed_event(n.into()), Some(Ok(n)));
    }

    #[test]
    fn u64_recognizer() {
        let mut rec = U64Recognizer;
        let n: u64 = u64::from(u32::MAX) * 2;
        assert_eq!(rec.feed_event(n.into()), Some(Ok(n)));
    }

    #[test]
    fn f64_recognizer() {
        let mut rec = F64Recognizer;
        let x: f64 = 1.5;
        assert_eq!(rec.feed_event(x.into()), Some(Ok(x)));
    }

    #[test]
    fn bool_recognizer() {
        let mut rec = BoolRecognizer;
        assert_eq!(rec.feed_event(true.into()), Some(Ok(true)));
    }

    #[test]
    fn string_recognizer() {
        let mut rec = StringRecognizer;
        assert_eq!(rec.feed_event("name".into()), Some(Ok("name".to_string())));
    }

    #[test]
    fn text_recognizer() {
        let mut rec = TextRecognizer;
        assert_eq!(rec.feed_event("name".into()), Some(Ok(Text::new("name"))));
    }

    #[test]
    fn blob_recognizer() {
        let mut rec = DataRecognizer;
        let ev = ReadEvent::Blob(vec![1, 2, 3]);
        assert_eq!(rec.feed_event(ev), Some(Ok(vec![1, 2, 3])));
    }

    #[test]
    fn big_int_recognizer() {
        let mut rec = BigIntRecognizer;
        let ev = ReadEvent::Number(NumericValue::BigInt(BigInt::from(-5)));
        assert_eq!(rec.feed_event(ev), Some(Ok(BigInt::from(-5))));
    }

    #[test]
    fn big_uint_recognizer() {
        let mut rec = BigUintRecognizer;
        let ev = ReadEvent::Number(NumericValue::BigUint(BigUint::from(5u32)));
        assert_eq!(rec.feed_event(ev), Some(Ok(BigUint::from(5u32))));
    }
}
