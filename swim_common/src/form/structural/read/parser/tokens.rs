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

use crate::form::structural::read::parser::{NumericLiteral, Span};
use crate::model::parser::{is_identifier_char, is_identifier_start, unescape};
use crate::model::text::Text;
use either::Either;
use nom::branch::alt;
use nom::combinator::{map, map_res, opt, peek, recognize};
use nom::multi::{many0_count, many1_count};
use nom::sequence::{delimited, pair, preceded, tuple};
use nom::IResult;
use num_bigint::{BigInt, BigUint, ParseBigIntError, Sign};
use num_traits::Num;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::ops::Neg;

fn unwrap_span(span: Span<'_>) -> &str {
    *span
}

fn resolve_escapes(span: Span<'_>) -> Result<Cow<'_, str>, InvalidEscapes> {
    let input = *span;
    if input.contains('\\') {
        match unescape(input) {
            Ok(text) => Ok(Cow::Owned(text.into())),
            Err(text) => Err(InvalidEscapes(text)),
        }
    } else {
        Ok(Cow::Borrowed(input))
    }
}

fn base64_digit(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '+' || c == '/'
}

fn padding(c: char) -> bool {
    c == '='
}

fn base64_digit_or_padding(c: char) -> bool {
    base64_digit(c) || padding(c)
}

#[derive(Debug)]
struct InvalidEscapes(Text);

impl Display for InvalidEscapes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\" contains invalid escape sequences.", self.0)
    }
}

impl std::error::Error for InvalidEscapes {}

fn escape(input: Span<'_>) -> IResult<Span<'_>, &str> {
    use nom::character::streaming as character;
    map(
        recognize(pair(character::char('\\'), character::anychar)),
        unwrap_span,
    )(input)
}

pub fn string_literal(input: Span<'_>) -> IResult<Span<'_>, Cow<'_, str>> {
    use nom::character::streaming as character;
    map_res(
        delimited(
            character::char('"'),
            recognize(many0_count(alt((
                recognize(character::satisfy(|c| c != '\\' && c != '\"')),
                recognize(escape),
            )))),
            character::char('"'),
        ),
        resolve_escapes,
    )(input)
}

pub fn seperator(input: Span<'_>) -> IResult<Span<'_>, char> {
    use nom::character::streaming as character;
    character::one_of(",;")(input)
}

macro_rules! token_mod {
    ($name:ident, $submod:ident) => {
        pub mod $name {

            use super::*;
            use nom::bytes::$submod::tag_no_case;
            use nom::character::$submod as character;
            use nom::number::$submod as number;

            pub fn identifier(input: Span<'_>) -> IResult<Span<'_>, &str> {
                map(
                    recognize(pair(
                        character::satisfy(is_identifier_start),
                        many0_count(character::satisfy(is_identifier_char)),
                    )),
                    unwrap_span,
                )(input)
            }

            pub fn identifier_or_bool(input: Span<'_>) -> IResult<Span<'_>, Either<&str, bool>> {
                map(identifier, |id| match id {
                    "true" => Either::Right(true),
                    "false" => Either::Right(false),
                    _ => Either::Left(id),
                })(input)
            }

            fn natural(
                tag: &'static str,
                digits: &'static str,
            ) -> impl FnMut(Span<'_>) -> IResult<Span<'_>, Span<'_>> {
                move |input: Span<'_>| {
                    preceded(
                        tag_no_case(tag),
                        recognize(many1_count(character::one_of(digits))),
                    )(input)
                }
            }

            pub fn numeric_literal(input: Span<'_>) -> IResult<Span<'_>, NumericLiteral> {
                alt((binary, hexadecimal, decimal_or_float))(input)
            }

            fn signed<F>(mut base: F) -> impl FnMut(Span<'_>) -> IResult<Span<'_>, (bool, Span<'_>)>
            where
                F: FnMut(Span<'_>) -> IResult<Span<'_>, Span<'_>>,
            {
                move |input: Span<'_>| {
                    pair(
                        map(opt(character::char('-')), |maybe| maybe.is_some()),
                        &mut base,
                    )(input)
                }
            }

            fn hexadecimal_str(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
                natural("0x", "0123456789abcdefABCDEF")(input)
            }

            fn hexadecimal(input: Span<'_>) -> IResult<Span<'_>, NumericLiteral> {
                map_res(signed(hexadecimal_str), |(negative, rep)| {
                    try_to_int_literal(negative, *rep, 16)
                })(input)
            }

            fn binary_str(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
                natural("0b", "01")(input)
            }

            fn binary(input: Span<'_>) -> IResult<Span<'_>, NumericLiteral> {
                map_res(signed(binary_str), |(negative, rep)| {
                    try_to_int_literal(negative, *rep, 2)
                })(input)
            }

            fn decimal_str(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
                recognize(many1_count(character::one_of("0123456789")))(input)
            }

            fn decimal_or_float(input: Span<'_>) -> IResult<Span<'_>, NumericLiteral> {
                alt((
                    map_res(
                        map_res(
                            pair(signed(decimal_str), peek(opt(character::one_of(".eE")))),
                            |(r, is_float)| {
                                if is_float.is_some() {
                                    Err(())
                                } else {
                                    Ok(r)
                                }
                            },
                        ),
                        |(negative, rep)| try_to_int_literal(negative, *rep, 10),
                    ),
                    map(number::double, NumericLiteral::Float),
                ))(input)
            }

            fn try_to_int_literal(
                negative: bool,
                rep: &str,
                radix: u32,
            ) -> Result<NumericLiteral, ParseBigIntError> {
                if let Ok(n) = u64::from_str_radix(rep, radix) {
                    if negative {
                        if let Ok(m) = i64::try_from(n) {
                            Ok(NumericLiteral::Int(-m))
                        } else {
                            Ok(NumericLiteral::BigInt(BigInt::from(n).neg()))
                        }
                    } else {
                        Ok(NumericLiteral::UInt(n))
                    }
                } else {
                    let n = BigUint::from_str_radix(rep, radix)?;
                    if negative {
                        Ok(NumericLiteral::BigInt(BigInt::from_biguint(Sign::Minus, n)))
                    } else {
                        Ok(NumericLiteral::BigUint(n))
                    }
                }
            }

            fn base64_block(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
                let digit = character::satisfy(base64_digit);
                let mut block = recognize(tuple((&digit, &digit, &digit, &digit)));
                block(input)
            }

            fn base64_final_block(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
                let digit = character::satisfy(base64_digit);
                let padding = character::satisfy(padding);
                let digit_or_padding = character::satisfy(base64_digit_or_padding);
                let mut block = recognize(tuple((&digit, &digit, &digit_or_padding, &padding)));
                block(input)
            }

            fn base64(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
                recognize(pair(many0_count(base64_block), opt(base64_final_block)))(input)
            }

            fn base64_literal(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
                preceded(character::char('%'), base64)(input)
            }

            pub fn blob(input: Span<'_>) -> IResult<Span<'_>, Vec<u8>> {
                map_res(base64_literal, |span| base64::decode(*span))(input)
            }
        }
    };
}

token_mod!(streaming, streaming);
token_mod!(complete, complete);
