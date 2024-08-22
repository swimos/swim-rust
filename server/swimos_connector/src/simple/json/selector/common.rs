// Copyright 2015-2024 Swim Inc.
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

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::char,
    combinator::{map, opt},
    error::{Error, ErrorKind},
    multi::separated_list0,
    sequence::preceded,
    IResult, Needed, Parser, Slice,
};
use nom_locate::LocatedSpan;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::{fmt, slice};

pub type Span<'a> = LocatedSpan<&'a str>;

pub fn parse_consume<'i, P, O>(input: Span<'i>, mut parser: P) -> Result<O, ParseError>
where
    P: Parser<Span<'i>, O, Error<Span<'i>>>,
{
    let (input, output) = parser.parse(input).map_err(|err| match err {
        nom::Err::Incomplete(Needed::Size(cnt)) => ParseError(cnt.get()),
        nom::Err::Incomplete(Needed::Unknown) => ParseError(1),
        nom::Err::Error(e) | nom::Err::Failure(e) => ParseError(e.input.location_offset()),
    })?;

    if !input.is_empty() {
        return Err(ParseError(input.location_offset()));
    }

    Ok(output)
}

#[derive(Debug)]
pub struct SelectorPatternIter<'p> {
    pattern: &'p str,
    head: Option<Part<'p>>,
    inner: slice::Iter<'p, Segment>,
    state: PatternIterState,
}

impl<'p> SelectorPatternIter<'p> {
    pub fn new(
        pattern: &'p str,
        head: Option<Part<'p>>,
        inner: slice::Iter<'p, Segment>,
    ) -> SelectorPatternIter<'p> {
        SelectorPatternIter {
            pattern,
            head,
            inner,
            state: PatternIterState::Start,
        }
    }
}

#[derive(Debug)]
enum PatternIterState {
    Start,
    Path,
    Done,
}

impl<'p> Iterator for SelectorPatternIter<'p> {
    type Item = Part<'p>;

    fn next(&mut self) -> Option<Self::Item> {
        let SelectorPatternIter {
            pattern,
            head,
            inner,
            state,
        } = self;

        loop {
            match state {
                PatternIterState::Start => match head.take() {
                    Some(head) => break Some(head),
                    None => *state = PatternIterState::Path,
                },
                PatternIterState::Path => {
                    let item = match inner.next() {
                        Some(segment) => Some(segment_to_part(pattern, segment)),
                        None => {
                            *state = PatternIterState::Done;
                            None
                        }
                    };
                    break item;
                }
                PatternIterState::Done => break None,
            }
        }
    }
}

pub fn segment_to_part<'p>(pattern: &'p str, segment: &Segment) -> Part<'p> {
    match segment {
        Segment::Static(Bound { start, end }) => Part::Static(&pattern[*start..*end]),
        Segment::Selector(selector) => {
            let parts = selector
                .parts
                .iter()
                .map(|Bound { start, end }| &pattern[*start..*end])
                .collect();
            match selector.kind {
                SelectorKind::Key => Part::KeySelector(parts),
                SelectorKind::Value => Part::ValueSelector(parts),
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum Part<'p> {
    Static(&'p str),
    KeySelector(Vec<&'p str>),
    ValueSelector(Vec<&'p str>),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selector {
    pub kind: SelectorKind,
    pub parts: Vec<Bound>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Segment {
    Static(Bound),
    Selector(Selector),
}

impl Segment {
    pub fn fmt_with_input(&self, f: &mut fmt::Formatter<'_>, input: &str) -> fmt::Result {
        match self {
            Segment::Static(Bound { start, end }) => {
                // check if the previous character in the path was an escape character. while this
                // information was known when the path was parsed, it's not necessary to add the
                // state to the segment as it would only be used for the Display implementation and
                // not in any critical paths.
                let fmt = &input[*start..*end];
                // Safe as an escape character is not valid as a starting character.
                if *start != 0 && input.as_bytes()[start - 1] == b'$' {
                    write!(f, "${fmt}")
                } else {
                    f.write_str(fmt)
                }
            }
            Segment::Selector(Selector { kind, parts }) => {
                match kind {
                    SelectorKind::Key => write!(f, "$key")?,
                    SelectorKind::Value => write!(f, "$value")?,
                };

                for bound in parts {
                    let Bound { start, end } = bound;
                    write!(f, ".{}", &input[*start..*end])?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectorKind {
    Key,
    Value,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bound {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseError(pub usize);

impl Display for SelectorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectorKind::Key => write!(f, "$key"),
            SelectorKind::Value => write!(f, "$value"),
        }
    }
}

pub fn failure<I, O, E: nom::error::ParseError<I>>(input: I) -> IResult<I, O, E> {
    Err(nom::Err::Failure(E::from_error_kind(
        input,
        ErrorKind::Fail,
    )))
}

pub fn parse_payload_selector(input: Span) -> IResult<Span, Selector> {
    alt((
        map(parse_selector("$key"), |parts| Selector {
            kind: SelectorKind::Key,
            parts,
        }),
        map(parse_selector("$value"), |parts| Selector {
            kind: SelectorKind::Value,
            parts,
        }),
    ))(input)
}

pub fn parse_selector(selector: &str) -> impl FnMut(Span) -> IResult<Span, Vec<Bound>> + '_ {
    move |input| {
        let (input, _) = tag(selector)(input)?;
        let (input, segments) = opt(preceded(
            char('.'),
            separated_list0(char('.'), non_empty_segment),
        ))(input)?;
        Ok((input, segments.unwrap_or_default()))
    }
}

pub fn non_empty_segment(input: Span) -> IResult<Span, Bound> {
    let (input, segment) = take_while(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    if segment.is_empty() {
        Err(nom::Err::Failure(Error::new(input, ErrorKind::TakeWhile1)))
    } else {
        let start = segment.location_offset();
        let end = start + segment.fragment().len();
        Ok((input, Bound { start, end }))
    }
}

pub fn parse_static_expression<F>(term: F) -> impl FnMut(Span) -> IResult<Span, Span>
where
    F: FnMut(Span) -> IResult<Span, bool> + Copy,
{
    move |start_span| {
        let mut current_span = start_span;
        loop {
            let (input, cont) = preceded(
                take_while(|c: char| c.is_alphanumeric() || "-._".contains(c)),
                term,
            )(current_span)?;

            if cont {
                current_span = input;
            } else {
                let end = input.location_offset() - start_span.location_offset();
                let input = start_span.slice(end..);
                let path = start_span.slice(..end);

                if path.len() == 0 {
                    return Err(nom::Err::Error(Error::new(input, ErrorKind::Eof)));
                }

                break Ok((input, path));
            }
        }
    }
}
