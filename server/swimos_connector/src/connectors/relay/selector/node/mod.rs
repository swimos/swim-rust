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

use crate::connectors::relay::selector::{
    common::{
        failure, parse_consume, parse_payload_selector, Segment, Selector, Span, StaticBound,
    },
    ParseError, Part, SelectorPatternIter,
};
use crate::deserialization::Deferred;
use crate::relay::RelayError;
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
use std::{fmt, fmt::Display, slice};
use swimos_model::Value;

/// A selector for deriving a node URI to send a command to, possibly using a record's contents.
///
/// Node selectors may select from the topic's name, a record's key or value and any subcomponents
/// that it contains. Alternatively, they may be a static node URI.
///
/// # Example
/// ```
/// # use std::error::Error;
/// use swimos_connector::relay::NodeSelector;
/// use std::str::FromStr;
///
/// # fn main() -> Result<(), Box<dyn Error + 'static>> {
/// let selector = NodeSelector::from_str("/agents/$key.name/$value.id/")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSelector {
    pattern: String,
    scheme: Option<StaticBound>,
    segments: Vec<Segment>,
}

impl NodeSelector {
    pub(crate) fn select<K, V>(
        &self,
        key: &mut K,
        value: &mut V,
        topic: &Value,
    ) -> Result<String, RelayError>
    where
        K: Deferred,
        V: Deferred,
    {
        let mut node_uri = String::new();

        for elem in self.into_iter() {
            match elem {
                Part::Static(p) => node_uri.push_str(p),
                Part::Selector(selector) => {
                    let value = selector
                        .select(topic, key, value)?
                        .ok_or(RelayError::InvalidRecord("".to_string()))?;
                    match value {
                        Value::BooleanValue(v) => {
                            node_uri.push_str(if *v { "true" } else { "false" })
                        }
                        Value::Int32Value(v) => node_uri.push_str(&v.to_string()),
                        Value::Int64Value(v) => node_uri.push_str(&v.to_string()),
                        Value::UInt32Value(v) => node_uri.push_str(&v.to_string()),
                        Value::UInt64Value(v) => node_uri.push_str(&v.to_string()),
                        Value::BigInt(v) => node_uri.push_str(&v.to_string()),
                        Value::BigUint(v) => node_uri.push_str(&v.to_string()),
                        Value::Text(v) => node_uri.push_str(v.as_str()),
                        _ => return Err(RelayError::InvalidRecord(self.pattern.to_string())),
                    }
                }
            }
        }

        Ok(node_uri)
    }
}

impl AsRef<str> for NodeSelector {
    fn as_ref(&self) -> &str {
        self.pattern.as_str()
    }
}

impl FromStr for NodeSelector {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_pattern(s.into())
    }
}

impl<'p> IntoIterator for &'p NodeSelector {
    type Item = Part<'p>;
    type IntoIter = SelectorPatternIter<'p, slice::Iter<'p, Segment>>;

    fn into_iter(self) -> Self::IntoIter {
        let NodeSelector {
            pattern,
            scheme,
            segments,
        } = self;

        SelectorPatternIter::new(
            pattern.as_str(),
            scheme.as_ref().map(|StaticBound { start, end }| {
                Part::Static(&self.pattern.as_str()[*start..*end])
            }),
            segments.iter(),
        )
    }
}

fn parse_pattern(span: Span) -> Result<NodeSelector, ParseError> {
    parse_consume(span, Parser::and(parse_scheme, parse_path)).map(|(scheme, segments)| {
        NodeSelector {
            pattern: span.to_string(),
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
            tag("$topic").map(|_| Segment::Selector(Selector::Topic)),
            // Anything prefixed by $ will be an escaped character. E.g, an input of /$key$_value
            // will yield a key selector and then a static segment of '_value'.
            preceded(opt(one_of("$")), parse_static).map(|span| {
                Segment::Static(StaticBound {
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

fn parse_scheme(input: Span) -> IResult<Span, Option<StaticBound>> {
    let (input, scheme) = opt(terminated(
        take_while(|c: char| c.is_ascii_alphabetic()),
        char(':'),
    ))(input)?;
    let indices = scheme.map(|s| {
        let start = s.location_offset();
        let end = start + s.fragment().len();
        StaticBound { start, end }
    });
    Ok((input, indices))
}

impl Display for NodeSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(StaticBound { start, end }) = self.scheme {
            write!(f, "{}:", &self.pattern[start..end])?;
        }
        for part in &self.segments {
            part.fmt_with_input(f, &self.pattern)?;
        }
        Ok(())
    }
}
