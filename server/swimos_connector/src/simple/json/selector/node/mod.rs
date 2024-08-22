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

#[cfg(test)]
mod tests;

use crate::simple::json::selector::{
    common::{failure, parse_consume, parse_payload_selector, Bound, Segment, Span},
    ParseError, Part, SelectorPatternIter,
};
use nom::{
    branch::alt,
    bytes::complete::take_while_m_n,
    bytes::complete::{tag, take_while},
    character::complete::char,
    character::complete::{anychar, one_of},
    combinator::{eof, peek, value},
    combinator::{map, opt},
    error::{Error, ErrorKind},
    multi::many1,
    sequence::{preceded, terminated},
    IResult, Parser, Slice,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::{fmt, fmt::Display};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSelectorPattern {
    input: String,
    scheme: Option<Bound>,
    segments: Vec<Segment>,
}

impl FromStr for NodeSelectorPattern {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_pattern(s.into())
    }
}

impl<'p> IntoIterator for &'p NodeSelectorPattern {
    type Item = Part<'p>;
    type IntoIter = SelectorPatternIter<'p>;

    fn into_iter(self) -> Self::IntoIter {
        let NodeSelectorPattern {
            input,
            scheme,
            segments,
        } = self;

        SelectorPatternIter::new(
            input.as_str(),
            scheme
                .as_ref()
                .map(|Bound { start, end }| Part::Static(&self.input.as_str()[*start..*end])),
            segments.iter(),
        )
    }
}

fn parse_pattern(span: Span) -> Result<NodeSelectorPattern, ParseError> {
    parse_consume(span, Parser::and(parse_scheme, parse_path)).map(|(scheme, segments)| {
        NodeSelectorPattern {
            input: span.to_string(),
            scheme,
            segments,
        }
    })
}

fn start(input: Span) -> IResult<Span, ()> {
    value(
        (),
        peek(preceded(char('/'), take_while_m_n(1, 1, |_| true))),
    )(input)
}

fn parse_path(input: Span) -> IResult<Span, Vec<Segment>> {
    preceded(
        start,
        many1(alt((
            parse_payload_selector.map(Segment::Selector),
            // Anything prefixed by $ will be an escaped character. E.g, an input of /$key$_value
            // will yield a key selector and then a static segment of '_value'.
            preceded(opt(one_of("$")), parse_static).map(|span| {
                Segment::Static(Bound {
                    start: span.location_offset(),
                    end: span.location_offset() + span.fragment().len(),
                })
            }),
        ))),
    )(input)
}

fn parse_static(start_span: Span) -> IResult<Span, Span> {
    let mut current_span = start_span;
    loop {
        let (input, cont) = preceded(
            take_while(|c: char| c.is_alphanumeric() || "-._~".contains(c)),
            alt((
                preceded(tag("//"), failure),
                map(char('/'), |_| true),
                map(peek(tag("/$")), |_| false),
                map(peek(anychar), |_| false),
                map(eof, |_| false),
            )),
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

fn parse_scheme(input: Span) -> IResult<Span, Option<Bound>> {
    let (input, scheme) = opt(terminated(
        take_while(|c: char| c.is_ascii_alphabetic()),
        char(':'),
    ))(input)?;
    let indices = scheme.map(|s| {
        let start = s.location_offset();
        let end = start + s.fragment().len();
        Bound { start, end }
    });
    Ok((input, indices))
}

impl Display for NodeSelectorPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(Bound { start, end }) = self.scheme {
            write!(f, "{}:", &self.input[start..end])?;
        }
        for part in &self.segments {
            part.fmt_with_input(f, &self.input)?;
        }
        Ok(())
    }
}
