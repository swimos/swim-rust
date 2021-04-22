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

pub use crate::form::structural::read::parser::error::ParseError;
use crate::form::structural::read::{HeaderReader, StructuralReadable};
use nom_locate::LocatedSpan;
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::convert::TryFrom;

pub type Span<'a> = LocatedSpan<&'a str>;

#[derive(Debug, PartialEq)]
pub enum NumericLiteral {
    Int(i64),
    UInt(u64),
    BigInt(BigInt),
    BigUint(BigUint),
    Float(f64),
}

#[derive(Debug, PartialEq)]
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

pub fn parse_iterator(
    input: Span<'_>,
) -> impl Iterator<Item = Result<ParseEvent<'_>, nom::error::Error<Span<'_>>>> + '_ {
    record::ParseIterator::new(input)
}

pub fn parse_from_str<T: StructuralReadable>(input: Span<'_>) -> Result<T, ParseError> {
    let mut iterator = record::ParseIterator::new(input);
    let parsed = if let Some(event) = iterator.next() {
        match event? {
            ParseEvent::Extant => T::read_extant()?,
            ParseEvent::TextValue(value) => T::read_text(value)?,
            ParseEvent::Number(NumericLiteral::Int(value)) => {
                if let Ok(n) = i32::try_from(value) {
                    T::read_i32(n)?
                } else {
                    T::read_i64(value)?
                }
            }
            ParseEvent::Number(NumericLiteral::UInt(value)) => {
                if let Ok(n) = i32::try_from(value) {
                    T::read_i32(n)?
                } else if let Ok(n) = i64::try_from(value) {
                    T::read_i64(n)?
                } else {
                    T::read_u64(value)?
                }
            }
            ParseEvent::Number(NumericLiteral::BigInt(value)) => T::read_big_int(value)?,
            ParseEvent::Number(NumericLiteral::BigUint(value)) => T::read_big_uint(value)?,
            ParseEvent::Number(NumericLiteral::Float(value)) => T::read_f64(value)?,
            ParseEvent::Boolean(value) => T::read_bool(value)?,
            ParseEvent::Blob(data) => T::read_blob(data)?,
            ParseEvent::StartAttribute(name) => {
                let header_reader = T::record_reader()?;
                let body_reader = record::read_from_header(header_reader, &mut iterator, name)?;
                T::try_terminate(body_reader)?
            }
            ParseEvent::StartBody => {
                let body_reader =
                    record::read_body(T::record_reader()?.start_body()?, &mut iterator, false)?;
                T::try_terminate(body_reader)?
            }
            _ => {
                return Err(ParseError::InvalidEventStream);
            }
        }
    } else {
        T::read_extant()?
    };
    Ok(parsed)
}
