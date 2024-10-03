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

use crate::relay::selector::common::segment_to_part;
use crate::relay::selector::{
    common::{
        parse_consume, parse_payload_selector, parse_static_expression, Segment, Selector, Span,
        StaticBound,
    },
    ParseError, Part, SelectorPatternIter,
};
use crate::selector::Deferred;
use crate::LaneSelectorError;
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{anychar, one_of},
    combinator::{eof, map, opt, peek},
    sequence::preceded,
    IResult, Parser,
};
use std::iter::{once, Once};
use std::str::FromStr;
use swimos_model::Value;

#[cfg(test)]
mod tests;

/// A selector for deriving a lane URI to send a command to, possibly using a record's contents.
///
/// Lane selectors may select from the topic's name, a record's key or value and any subcomponents
/// that it contains. Alternatively, they may be a static lane URI.
///
/// # Example
/// ```
/// # use std::error::Error;
/// use swimos_connector::relay::LaneSelector;
/// use std::str::FromStr;
///
/// # fn main() -> Result<(), Box<dyn Error + 'static>> {
/// let selector = LaneSelector::from_str("$key.name")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct LaneSelector {
    pattern: String,
    segment: Segment,
}

impl LaneSelector {
    pub(crate) fn select<K, V>(
        &self,
        key: &mut K,
        value: &mut V,
        topic: &Value,
    ) -> Result<String, LaneSelectorError>
    where
        K: Deferred,
        V: Deferred,
    {
        let LaneSelector { pattern, segment } = self;

        match segment_to_part(pattern, segment) {
            Part::Static(p) => Ok(p.to_string()),
            Part::Selector(selector) => {
                let value = selector
                    .select(topic, key, value)?
                    .ok_or(LaneSelectorError::InvalidRecord("".to_string()))?;
                match value {
                    Value::BooleanValue(v) => {
                        if *v {
                            Ok("true".to_string())
                        } else {
                            Ok("false".to_string())
                        }
                    }
                    Value::Int32Value(v) => Ok(v.to_string()),
                    Value::Int64Value(v) => Ok(v.to_string()),
                    Value::UInt32Value(v) => Ok(v.to_string()),
                    Value::UInt64Value(v) => Ok(v.to_string()),
                    Value::BigInt(v) => Ok(v.to_string()),
                    Value::BigUint(v) => Ok(v.to_string()),
                    Value::Text(v) => Ok(v.to_string()),
                    _ => Err(LaneSelectorError::InvalidRecord(self.pattern.to_string())),
                }
            }
        }
    }
}

impl AsRef<str> for LaneSelector {
    fn as_ref(&self) -> &str {
        self.pattern.as_str()
    }
}

impl FromStr for LaneSelector {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_pattern(s.into())
    }
}

impl<'p> IntoIterator for &'p LaneSelector {
    type Item = Part<'p>;
    type IntoIter = SelectorPatternIter<'p, Once<&'p Segment>>;

    fn into_iter(self) -> Self::IntoIter {
        let LaneSelector { pattern, segment } = self;
        SelectorPatternIter::new(pattern.as_str(), None, once(segment))
    }
}

fn parse_pattern(span: Span) -> Result<LaneSelector, ParseError> {
    parse_consume(span, parse).map(|segment| LaneSelector {
        pattern: span.to_string(),
        segment,
    })
}

fn parse(span: Span) -> IResult<Span, Segment> {
    alt((
        parse_payload_selector.map(Segment::Selector),
        tag("$topic").map(|_| Segment::Selector(Selector::Topic)),
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
            Segment::Static(StaticBound {
                start: span.location_offset(),
                end: span.location_offset() + span.fragment().len(),
            })
        }),
    ))(span)
}
