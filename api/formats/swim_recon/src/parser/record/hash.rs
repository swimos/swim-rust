// Copyright 2015-2022 Swim Inc.
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
use super::Span;
use crate::hasher::HashError;
use crate::parser::record::{AttrBody, EventOrEnd, ItemsKind, RecBody};
use crate::parser::tokens::separator;
use crate::parser::IncrementalReconParser;
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::character::streaming as char_str;
use nom::combinator::{map, opt};
use nom::error::ErrorKind;
use nom::sequence::preceded;
use nom::{Finish, IResult, Parser};
use smallvec::SmallVec;
use std::hash::{Hash, Hasher};
use swim_form::structural::read::event::ReadEvent;

const AVG_NESTED_ATTR_COUNT: usize = 4;

/// A Recon parser that produces a sequence of parse events from a complete string and
/// calculates their hash.
pub struct HashParser {
    parser: Option<IncrementalReconParser>,
    closing_brackets: SmallVec<[bool; AVG_NESTED_ATTR_COUNT]>,
}

impl HashParser {
    pub fn new() -> Self {
        HashParser {
            parser: Some(IncrementalReconParser::new(false)),
            closing_brackets: SmallVec::with_capacity(AVG_NESTED_ATTR_COUNT),
        }
    }

    pub fn hash<H: Hasher>(self, mut input: Span, hasher: &mut H) -> Option<HashError> {
        let HashParser {
            mut parser,
            mut closing_brackets,
        } = self;

        loop {
            let mut events = match parser.as_mut()?.parse(input) {
                Ok((remaining, events)) => {
                    input = remaining;
                    events
                }
                Err(nom::Err::Incomplete(_)) => {
                    if let Some(mut p) = parser
                        .take()
                        .and_then(IncrementalReconParser::into_final_parser)
                    {
                        match p.parse(input).finish() {
                            Ok((remaining, events)) => {
                                input = remaining;
                                events
                            }
                            Err(err) => {
                                return Some(err.into());
                            }
                        }
                    } else {
                        let err = nom::error::Error::new(input, ErrorKind::Alt);
                        return Some(err.into());
                    }
                }
                Err(nom::Err::Error(err)) | Err(nom::Err::Failure(err)) => {
                    return Some(err.into());
                }
            };

            while let Some(event_or_end) = events.take_event() {
                match event_or_end {
                    EventOrEnd::Event(event, has_next) => match event {
                        ReadEvent::StartAttribute(_) => {
                            event.hash(hasher);

                            if !has_next && is_implicit_record(input) {
                                closing_brackets.push(true);
                                ReadEvent::StartBody.hash(hasher);
                            } else {
                                closing_brackets.push(false);
                            }
                        }

                        ReadEvent::EndAttribute => {
                            if let Some(add_closing_bracket) = closing_brackets.pop() {
                                if add_closing_bracket {
                                    ReadEvent::EndRecord.hash(hasher);
                                }
                            }

                            event.hash(hasher);
                        }

                        _ => {
                            event.hash(hasher);
                        }
                    },
                    EventOrEnd::End => return None,
                }
            }
        }
    }
}

impl Default for HashParser {
    fn default() -> Self {
        HashParser {
            parser: Some(IncrementalReconParser::new(false)),
            closing_brackets: SmallVec::with_capacity(AVG_NESTED_ATTR_COUNT),
        }
    }
}

/// State showing the validation progress of whether the
/// current attribute body is an implicit record or not.
#[derive(Debug, Clone, Copy)]
enum ValidationState {
    /// Validation is still in progress and we are at the
    /// top level in the body of an attribute.
    Top,
    /// Validation is still in progress and we are
    /// N levels deep inside nested records or attributes.
    Nested(usize),
    /// Validation completed with a result.
    Done(bool),
}

impl ValidationState {
    fn increment(level: usize) -> ValidationState {
        ValidationState::Nested(level + 1)
    }

    fn decrement(level: usize) -> ValidationState {
        if level == 1 {
            ValidationState::Top
        } else {
            ValidationState::Nested(level - 1)
        }
    }

    fn finish(result: bool) -> ValidationState {
        ValidationState::Done(result)
    }
}

fn is_implicit_record(input: Span) -> bool {
    let mut result: IResult<Span<'_>, ValidationState> = Ok((input, ValidationState::Top));

    loop {
        result = match result {
            Ok((rest, ValidationState::Top)) => preceded(
                opt(is_not(",;:{()")),
                alt((
                    map(separator, |_| ValidationState::finish(true)),
                    map(char_str::char(':'), |_| ValidationState::finish(true)),
                    map(char_str::char('{'), |_| ValidationState::increment(0)),
                    map(char_str::char('('), |_| ValidationState::increment(0)),
                    map(char_str::char(AttrBody::end_delim()), |_| {
                        ValidationState::finish(false)
                    }),
                )),
            )(rest),
            Ok((rest, ValidationState::Nested(level))) => preceded(
                opt(is_not("{()}")),
                alt((
                    map(char_str::char('{'), |_| ValidationState::increment(level)),
                    map(char_str::char('('), |_| ValidationState::increment(level)),
                    map(char_str::char(AttrBody::end_delim()), |_| {
                        ValidationState::decrement(level)
                    }),
                    map(char_str::char(RecBody::end_delim()), |_| {
                        ValidationState::decrement(level)
                    }),
                )),
            )(rest),
            Ok((_, ValidationState::Done(result))) => return result,
            Err(_) => return false,
        }
    }
}
