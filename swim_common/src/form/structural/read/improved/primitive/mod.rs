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
use crate::form::structural::read::parser::{NumericLiteral, ParseEvent};
use crate::form::structural::read::ReadError;
use crate::model::text::Text;
use num_bigint::{BigInt, BigUint};
use std::convert::TryFrom;
use utilities::iteratee::Iteratee;

pub struct UnitRecognizer;
pub struct I32Recognizer;
pub struct I64Recognizer;
pub struct U32Recognizer;
pub struct U64Recognizer;
pub struct BigIntRecognizer;
pub struct BigUintRecognizer;
pub struct F64Recognizer;
pub struct StringRecognizer;
pub struct TextRecognizer;
pub struct DataRecognizer;
pub struct BoolRecognizer;

impl<'a> Iteratee<ParseEvent<'a>> for UnitRecognizer {
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Extant => Some(Ok(())),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<()> for UnitRecognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for I32Recognizer {
    type Item = Result<i32, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Number(NumericLiteral::Int(n)) => {
                Some(i32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ParseEvent::Number(NumericLiteral::UInt(n)) => {
                Some(i32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<i32> for I32Recognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for I64Recognizer {
    type Item = Result<i64, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Number(NumericLiteral::Int(n)) => Some(Ok(n)),
            ParseEvent::Number(NumericLiteral::UInt(n)) => {
                Some(i64::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<i64> for I64Recognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for U32Recognizer {
    type Item = Result<u32, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Number(NumericLiteral::Int(n)) => {
                Some(u32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ParseEvent::Number(NumericLiteral::UInt(n)) => {
                Some(u32::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<u32> for U32Recognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for U64Recognizer {
    type Item = Result<u64, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Number(NumericLiteral::Int(n)) => {
                Some(u64::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ParseEvent::Number(NumericLiteral::UInt(n)) => Some(Ok(n)),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<u64> for U64Recognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for BigIntRecognizer {
    type Item = Result<BigInt, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Number(NumericLiteral::Int(n)) => Some(Ok(BigInt::from(n))),
            ParseEvent::Number(NumericLiteral::UInt(n)) => Some(Ok(BigInt::from(n))),
            ParseEvent::Number(NumericLiteral::BigInt(n)) => Some(Ok(n)),
            ParseEvent::Number(NumericLiteral::BigUint(n)) => Some(Ok(BigInt::from(n))),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<BigInt> for BigIntRecognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for BigUintRecognizer {
    type Item = Result<BigUint, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Number(NumericLiteral::Int(n)) => {
                Some(BigUint::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ParseEvent::Number(NumericLiteral::UInt(n)) => Some(Ok(BigUint::from(n))),
            ParseEvent::Number(NumericLiteral::BigInt(n)) => {
                Some(BigUint::try_from(n).map_err(|_| ReadError::NumberOutOfRange))
            }
            ParseEvent::Number(NumericLiteral::BigUint(n)) => Some(Ok(n)),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<BigUint> for BigUintRecognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for F64Recognizer {
    type Item = Result<f64, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Number(NumericLiteral::Float(x)) => Some(Ok(x)),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<f64> for F64Recognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for StringRecognizer {
    type Item = Result<String, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::TextValue(string) => Some(Ok(string.into())),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<String> for StringRecognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for TextRecognizer {
    type Item = Result<Text, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::TextValue(string) => Some(Ok(string.into())),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<Text> for TextRecognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for DataRecognizer {
    type Item = Result<Vec<u8>, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Blob(v) => Some(Ok(v)),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<Vec<u8>> for DataRecognizer {
    fn reset(&mut self) {}
}

impl<'a> Iteratee<ParseEvent<'a>> for BoolRecognizer {
    type Item = Result<bool, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match input {
            ParseEvent::Boolean(p) => Some(Ok(p)),
            ow => Some(Err(super::bad_kind(&ow))),
        }
    }
}

impl Recognizer<bool> for BoolRecognizer {
    fn reset(&mut self) {}
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn unit_recognizer() {
        let mut rec = UnitRecognizer;
        assert_eq!(rec.feed(ParseEvent::Extant), Some(Ok(())));
    }

    #[test]
    fn i32_recognizer() {
        let mut rec = I32Recognizer;
        assert_eq!(rec.feed(ParseEvent::from(-3i32)), Some(Ok(-3i32)));
    }

    #[test]
    fn i64_recognizer() {
        let mut rec = I64Recognizer;
        let n: i64 = i64::from(i32::min_value()) * 2;
        assert_eq!(rec.feed(n.into()), Some(Ok(n)));
    }

    #[test]
    fn u32_recognizer() {
        let mut rec = U32Recognizer;
        let n: u32 = 567u32;
        assert_eq!(rec.feed(n.into()), Some(Ok(n)));
    }

    #[test]
    fn u64_recognizer() {
        let mut rec = U64Recognizer;
        let n: u64 = u64::from(u32::max_value()) * 2;
        assert_eq!(rec.feed(n.into()), Some(Ok(n)));
    }

    #[test]
    fn f64_recognizer() {
        let mut rec = F64Recognizer;
        let x: f64 = 1.5;
        assert_eq!(rec.feed(x.into()), Some(Ok(x)));
    }

    #[test]
    fn bool_recognizer() {
        let mut rec = BoolRecognizer;
        assert_eq!(rec.feed(true.into()), Some(Ok(true)));
    }

    #[test]
    fn string_recognizer() {
        let mut rec = StringRecognizer;
        assert_eq!(rec.feed("name".into()), Some(Ok("name".to_string())));
    }

    #[test]
    fn text_recognizer() {
        let mut rec = TextRecognizer;
        assert_eq!(rec.feed("name".into()), Some(Ok(Text::new("name"))));
    }

    #[test]
    fn blob_recognizer() {
        let mut rec = DataRecognizer;
        let ev = ParseEvent::Blob(vec![1, 2, 3]);
        assert_eq!(rec.feed(ev), Some(Ok(vec![1, 2, 3])));
    }

    #[test]
    fn big_int_recognizer() {
        let mut rec = BigIntRecognizer;
        let ev = ParseEvent::Number(NumericLiteral::BigInt(BigInt::from(-5)));
        assert_eq!(rec.feed(ev), Some(Ok(BigInt::from(-5))));
    }

    #[test]
    fn big_uint_recognizer() {
        let mut rec = BigUintRecognizer;
        let ev = ParseEvent::Number(NumericLiteral::BigUint(BigUint::from(5u32)));
        assert_eq!(rec.feed(ev), Some(Ok(BigUint::from(5u32))));
    }
}
