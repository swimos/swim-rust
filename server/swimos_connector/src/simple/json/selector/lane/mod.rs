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

use crate::{
    simple::json::selector::common::{
        parse_consume, parse_payload_selector, parse_static_expression, Bound, Segment, Span,
    },
    simple::json::selector::{ParseError, Part, SelectorPatternIter},
};
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{anychar, one_of},
    combinator::{eof, map, opt, peek},
    multi::many1,
    sequence::preceded,
    IResult, Parser,
};
use std::str::FromStr;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct LaneSelectorPattern {
    input: String,
    segments: Vec<Segment>,
}

impl FromStr for LaneSelectorPattern {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_pattern(s.into())
    }
}

impl<'p> IntoIterator for &'p LaneSelectorPattern {
    type Item = Part<'p>;
    type IntoIter = SelectorPatternIter<'p>;

    fn into_iter(self) -> Self::IntoIter {
        let LaneSelectorPattern { input, segments } = self;
        SelectorPatternIter::new(input.as_str(), None, segments.iter())
    }
}

fn parse_pattern(span: Span) -> Result<LaneSelectorPattern, ParseError> {
    parse_consume(span, parse).map(|segments| LaneSelectorPattern {
        input: span.to_string(),
        segments,
    })
}

fn parse(span: Span) -> IResult<Span, Vec<Segment>> {
    many1(alt((
        parse_payload_selector.map(Segment::Selector),
        // Anything prefixed by $ will be an escaped character. E.g, an input of $key$_value
        // will yield a key selector and then a static segment of '_value'.
        preceded(
            opt(one_of("$")),
            parse_static_expression(move |span| {
                alt((
                    map(peek(tag("$")), |_| false),
                    map(peek(anychar), |_| false),
                    map(eof, |_| false),
                ))(span)
            }),
        )
        .map(|span| {
            Segment::Static(Bound {
                start: span.location_offset(),
                end: span.location_offset() + span.fragment().len(),
            })
        }),
    )))(span)
}
