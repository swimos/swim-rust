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

mod error;
mod record;
#[cfg(test)]
mod tests;
mod tokens;

use crate::form::structural::read::improved::RecognizerReadable;
pub use crate::form::structural::read::parser::error::ParseError;
use crate::form::structural::read::ReadError;
use crate::model::text::Text;
use nom_locate::LocatedSpan;
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use utilities::iteratee::Iteratee;

/// Wraps a string in a strucutre that keeps track of the line and column
/// as the input is parsed.
pub type Span<'a> = LocatedSpan<&'a str>;

#[derive(Debug, PartialEq, Clone)]
pub enum NumericLiteral {
    Int(i64),
    UInt(u64),
    BigInt(BigInt),
    BigUint(BigUint),
    Float(f64),
}

/// Incrementally parsing a Recon document produces a sequence of these events. An
/// event is either a token, a notification that an attribute or record body has
/// started or ended or a notifcation of a slot (this will occur between the slot
/// key and the slot value). If a string does not requires escaping it will be
/// provided as a reference into the original input rather than an separate
/// allocation.
#[derive(Debug, PartialEq, Clone)]
pub enum ParseEvent<'a> {
    Extant,
    TextValue(Cow<'a, str>),
    Number(NumericLiteral),
    Boolean(bool),
    Blob(Vec<u8>),
    StartAttribute(Cow<'a, str>),
    EndAttribute,
    StartBody,
    Slot,
    EndRecord,
}

impl<'a> From<u8> for ParseEvent<'a> {
    fn from(n: u8) -> Self {
        ParseEvent::Number(NumericLiteral::Int(n.into()))
    }
}

impl<'a> From<i16> for ParseEvent<'a> {
    fn from(n: i16) -> Self {
        ParseEvent::Number(NumericLiteral::Int(n.into()))
    }
}

impl<'a> From<u16> for ParseEvent<'a> {
    fn from(n: u16) -> Self {
        ParseEvent::Number(NumericLiteral::UInt(n.into()))
    }
}

impl<'a> From<i8> for ParseEvent<'a> {
    fn from(n: i8) -> Self {
        ParseEvent::Number(NumericLiteral::Int(n.into()))
    }
}

impl<'a> From<i32> for ParseEvent<'a> {
    fn from(n: i32) -> Self {
        ParseEvent::Number(NumericLiteral::Int(n.into()))
    }
}

impl<'a> From<i64> for ParseEvent<'a> {
    fn from(n: i64) -> Self {
        ParseEvent::Number(NumericLiteral::Int(n))
    }
}

impl<'a> From<u32> for ParseEvent<'a> {
    fn from(n: u32) -> Self {
        ParseEvent::Number(NumericLiteral::UInt(n.into()))
    }
}

impl<'a> From<u64> for ParseEvent<'a> {
    fn from(n: u64) -> Self {
        ParseEvent::Number(NumericLiteral::UInt(n))
    }
}

impl<'a> From<f64> for ParseEvent<'a> {
    fn from(x: f64) -> Self {
        ParseEvent::Number(NumericLiteral::Float(x))
    }
}

impl<'a> From<f32> for ParseEvent<'a> {
    fn from(x: f32) -> Self {
        ParseEvent::Number(NumericLiteral::Float(x.into()))
    }
}

impl<'a> From<BigInt> for ParseEvent<'a> {
    fn from(n: BigInt) -> Self {
        ParseEvent::Number(NumericLiteral::BigInt(n))
    }
}

impl<'a> From<BigUint> for ParseEvent<'a> {
    fn from(n: BigUint) -> Self {
        ParseEvent::Number(NumericLiteral::BigUint(n))
    }
}

impl<'a> From<&'a str> for ParseEvent<'a> {
    fn from(s: &'a str) -> Self {
        ParseEvent::TextValue(Cow::Borrowed(s))
    }
}

impl<'a> From<String> for ParseEvent<'a> {
    fn from(s: String) -> Self {
        ParseEvent::TextValue(Cow::Owned(s))
    }
}

impl<'a> From<Cow<'a, str>> for ParseEvent<'a> {
    fn from(s: Cow<'a, str>) -> Self {
        ParseEvent::TextValue(s)
    }
}

impl<'a> From<Text> for ParseEvent<'a> {
    fn from(s: Text) -> Self {
        ParseEvent::TextValue(Cow::Owned(s.to_string()))
    }
}

impl<'a> From<bool> for ParseEvent<'a> {
    fn from(p: bool) -> Self {
        ParseEvent::Boolean(p)
    }
}

impl<'a> From<Vec<u8>> for ParseEvent<'a> {
    fn from(blob: Vec<u8>) -> Self {
        ParseEvent::Blob(blob)
    }
}

/// Create an itearator that will parse a sequence of events from a complete string.
pub fn parse_iterator(
    input: Span<'_>,
) -> impl Iterator<Item = Result<ParseEvent<'_>, nom::error::Error<Span<'_>>>> + '_ {
    record::ParseIterator::new(input)
}

pub fn parse_recognize<T: RecognizerReadable>(input: Span<'_>) -> Result<T, ParseError> {
    let mut recognizer = T::make_recognizer();
    let mut iterator = record::ParseIterator::new(input);
    loop {
        if let Some(ev) = iterator.next() {
            if let Some(r) = recognizer.feed(ev?) {
                break r;
            }
        } else {
            break recognizer
                .flush()
                .unwrap_or(Err(ReadError::IncompleteRecord));
        }
    }
    .map_err(ParseError::Structure)
}
