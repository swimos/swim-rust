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

use crate::parser::record::attr_name_final;
use crate::parser::tokens::complete::{identifier, numeric_literal};
use crate::parser::tokens::{separator, string_literal};
use crate::parser::Span;
use either::Either;
use nom::branch::alt;
use nom::character::complete;
use nom::character::complete::multispace0;
use nom::combinator::{map, map_res, opt};
use nom::error::{Error, ErrorKind};
use nom::sequence::{pair, preceded, separated_pair};
use nom::{IResult, Parser};
use std::borrow::Cow;
use swim_form::structural::read::event::NumericValue;

#[derive(Default)]
pub struct HeaderParser {
    state: HeaderParserState,
}

#[derive(Debug)]
enum HeaderParserState {
    Init,
    ReadingPrimarySlot,
    ReadingSecondarySlot,
    Fin,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReadEvent<'a> {
    Tag(Cow<'a, str>),
    Slot(Cow<'a, str>, Either<Cow<'a, str>, f64>),
    End,
}

impl Default for HeaderParserState {
    fn default() -> Self {
        HeaderParserState::Init
    }
}

impl<'a> Parser<Span<'a>, ReadEvent<'a>, nom::error::Error<Span<'a>>> for HeaderParser {
    fn parse(&mut self, input: Span<'a>) -> IResult<Span<'a>, ReadEvent<'a>, Error<Span<'a>>> {
        let HeaderParser { state } = self;
        match state {
            HeaderParserState::Init => {
                let (input, (name, has_body)) = attr(input)?;
                if has_body {
                    *state = HeaderParserState::ReadingPrimarySlot;
                } else {
                    *state = HeaderParserState::Fin;
                }
                Ok((input, ReadEvent::Tag(name)))
            }
            HeaderParserState::ReadingPrimarySlot => match parse_primary_slot(input) {
                Ok((input, (name, value))) => {
                    *state = HeaderParserState::ReadingSecondarySlot;
                    Ok((input, ReadEvent::Slot(name, value)))
                }
                Err(e) => {
                    *state = HeaderParserState::Fin;
                    Err(e)
                }
            },
            HeaderParserState::ReadingSecondarySlot => match parse_secondary_slot(input) {
                Ok((input, (name, value, complete))) => {
                    if complete {
                        *state = HeaderParserState::Fin;
                    } else {
                        *state = HeaderParserState::ReadingSecondarySlot;
                    }

                    Ok((input, ReadEvent::Slot(name, value)))
                }
                Err(e) => {
                    *state = HeaderParserState::Fin;
                    Err(e)
                }
            },
            HeaderParserState::Fin => Ok((input, ReadEvent::End)),
        }
    }
}

pub struct HeaderParseIterator<'a> {
    input: Span<'a>,
    parser: Option<HeaderParser>,
}

impl<'a> HeaderParseIterator<'a> {
    pub fn new(repr: &'a str) -> HeaderParseIterator<'a> {
        HeaderParseIterator {
            input: Span::new(repr),
            parser: Some(HeaderParser::default()),
        }
    }

    pub fn offset(&self) -> usize {
        self.input.location_offset()
    }
}

impl<'a> Iterator for HeaderParseIterator<'a> {
    type Item = Result<ReadEvent<'a>, Error<Span<'a>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let HeaderParseIterator { input, parser } = self;

        match parser {
            Some(header_parser) => match header_parser.parse(*input) {
                Ok((span, out)) => match out {
                    ReadEvent::End => {
                        *parser = None;
                        None
                    }
                    e => {
                        *input = span;
                        Some(Ok(e))
                    }
                },
                Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                    *parser = None;
                    Some(Err(e))
                }
                Err(nom::Err::Incomplete(_)) => {
                    *parser = None;
                    let err = nom::error::Error::new(*input, ErrorKind::Alt);
                    Some(Err(err))
                }
            },
            None => None,
        }
    }
}

fn attr(input: Span<'_>) -> IResult<Span<'_>, (Cow<'_, str>, bool)> {
    preceded(
        complete::char('@'),
        pair(
            attr_name_final,
            map(opt(complete::char('(')), |o| o.is_some()),
        ),
    )(input)
}

fn parse_primary_slot(
    input: Span<'_>,
) -> IResult<Span<'_>, (Cow<'_, str>, Either<Cow<'_, str>, f64>)> {
    separated_pair(
        preceded(multispace0, identifier.map(Cow::Borrowed)),
        preceded(multispace0, complete::char(':')),
        preceded(
            multispace0,
            alt((
                string_literal.map(Either::Left),
                identifier.map(|i| Either::Left(Cow::Borrowed(i))),
                map_res(numeric_literal, |r| match r {
                    NumericValue::Float(val) => Ok(Either::Right(val)),
                    _ => Err(()),
                }),
            )),
        ),
    )(input)
}

fn parse_secondary_slot(
    input: Span<'_>,
) -> IResult<Span<'_>, (Cow<'_, str>, Either<Cow<'_, str>, f64>, bool)> {
    let (input, _) = preceded(multispace0, separator)(input)?;
    map(
        pair(
            parse_primary_slot,
            map(opt(preceded(multispace0, complete::char(')'))), |o| {
                o.is_some()
            }),
        ),
        |((ident, val), complete)| (ident, val, complete),
    )(input)
}

#[cfg(test)]
mod tests {
    use crate::parser::header::{HeaderParser, ReadEvent};
    use crate::parser::Span;
    use either::Either;
    use nom::Parser;

    fn parse_ok(input: &str, mut expected: Vec<ReadEvent<'_>>) {
        expected.reverse();
        let mut parser = HeaderParser::default();
        let mut span = Span::new(input);

        loop {
            match parser.parse(span) {
                Ok((new_span, event)) => match expected.pop() {
                    Some(expected_event) => {
                        assert_eq!(expected_event, event);
                        span = new_span;

                        if expected.last().is_none() {
                            break;
                        }
                    }
                    None => {
                        panic!("Parser produced an unexpected event: {:?}", event);
                    }
                },
                Err(e) => {
                    panic!("Parser errored with: '{:?}'", e)
                }
            }
        }

        if let Ok((_, event)) = parser.parse(span) {
            match event {
                ReadEvent::End => {}
                e => {
                    panic!("Parser produced more events than expected: {:?}", e);
                }
            }
        }
    }

    #[test]
    fn simple_header() {
        parse_ok("@auth", vec![ReadEvent::Tag("auth".into()), ReadEvent::End]);
    }

    #[test]
    fn simple_header_with_body() {
        parse_ok(
            "@auth@insert(key:\"key\", value:2)",
            vec![ReadEvent::Tag("auth".into()), ReadEvent::End],
        );
    }

    #[test]
    fn literals() {
        let expected = vec![
            ReadEvent::Tag("link".into()),
            ReadEvent::Slot("lane".into(), Either::Left("lane".into())),
            ReadEvent::Slot("node".into(), Either::Left("node".into())),
            ReadEvent::End,
        ];

        parse_ok("@link(lane:lane, node:node)", expected.clone());
        parse_ok("@link(lane: lane ; node : node     )", expected);
    }

    #[test]
    fn mixed_separators() {
        parse_ok(
            "@auth(key:\"key\", value:2.0; rate : 1.0)",
            vec![
                ReadEvent::Tag("auth".into()),
                ReadEvent::Slot("key".into(), Either::Left("key".into())),
                ReadEvent::Slot("value".into(), Either::Right(2.0)),
                ReadEvent::Slot("rate".into(), Either::Right(1.0)),
                ReadEvent::End,
            ],
        );
    }

    #[test]
    fn with_string_slots() {
        parse_ok(
            "@link(lane:\"lane\", node:\"node\")",
            vec![
                ReadEvent::Tag("link".into()),
                ReadEvent::Slot("lane".into(), Either::Left("lane".into())),
                ReadEvent::Slot("node".into(), Either::Left("node".into())),
                ReadEvent::End,
            ],
        );
    }

    #[test]
    fn with_mixed_slots() {
        parse_ok(
            "@link(lane:\"lane\", node:\"node\", rate:0.5, prio:1.0)",
            vec![
                ReadEvent::Tag("link".into()),
                ReadEvent::Slot("lane".into(), Either::Left("lane".into())),
                ReadEvent::Slot("node".into(), Either::Left("node".into())),
                ReadEvent::Slot("rate".into(), Either::Right(0.5)),
                ReadEvent::Slot("prio".into(), Either::Right(1.0)),
                ReadEvent::End,
            ],
        );
    }

    #[test]
    fn mixed_slots_spaces() {
        parse_ok(
            "@link(                   lane        : \"lane\"  ,   node   :   \"node\"  , rate : 0.5, prio:1.0  )",
            vec![
                ReadEvent::Tag("link".into()),
                ReadEvent::Slot("lane".into(), Either::Left("lane".into())),
                ReadEvent::Slot("node".into(), Either::Left("node".into())),
                ReadEvent::Slot("rate".into(), Either::Right(0.5)),
                ReadEvent::Slot("prio".into(), Either::Right(1.0)),
                ReadEvent::End,
            ],
        );
    }

    #[test]
    fn end_twice() {
        parse_ok(
            "@auth",
            vec![
                ReadEvent::Tag("auth".into()),
                ReadEvent::End,
                ReadEvent::End,
            ],
        );
    }
}
