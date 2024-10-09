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

use crate::relay::selector::{
    common::{
        parse_consume, parse_payload_selector, parse_static_expression, segment_to_part, Segment,
        Selector as SelectorModel, Span, StaticBound,
    },
    ParseError, Part,
};
use crate::selector::{PubSubSelectorArgs, Selector};
use crate::SelectorError;
use frunk::Coprod;
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{anychar, one_of},
    combinator::{eof, map, opt, peek},
    sequence::preceded,
    IResult, Parser,
};
use swimos_agent::event_handler::{Discard, SendCommand};
use swimos_agent_protocol::MapMessage;
use swimos_api::address::Address;
use swimos_model::Value;

#[cfg(test)]
mod tests;

type SendCommandOp =
    Coprod!(SendCommand<String, Value>, SendCommand<String,MapMessage<Value, Value>>);
pub type GenericSendCommandOp = Discard<Option<SendCommandOp>>;

#[derive(Debug)]
enum Inner {
    Value {
        input: String,
        segment: Segment,
    },
    Map {
        key_pattern: String,
        value_pattern: String,
        remove_when_no_value: bool,
        key: Segment,
        value: Segment,
    },
}

/// A selector for extracting either a Value or Map payload from a record.
#[derive(Debug)]
pub struct PayloadSelector {
    inner: Inner,
    required: bool,
}

impl PayloadSelector {
    /// Builds a new Value [`PayloadSelector`].
    ///
    /// # Arguments
    /// * `pattern` - the payload selector pattern.
    /// * `required` - whether the selector must succeed. If this is true and the selector fails, then
    ///   the connector will terminate.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::error::Error;
    /// use swimos_connector::PayloadSelector;
    ///
    /// # fn main() -> Result<(), Box<dyn Error + 'static>> {
    /// let selector = PayloadSelector::value("$key.name", true)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn value(pattern: &str, required: bool) -> Result<PayloadSelector, ParseError> {
        parse_pattern(pattern.into()).map(|segment| PayloadSelector {
            inner: Inner::Value {
                input: pattern.to_string(),
                segment,
            },
            required,
        })
    }

    /// Builds a new Value [`PayloadSelector`].
    ///
    /// # Arguments
    /// * `key_pattern` - the key selector pattern.
    /// * `value_pattern` - the value selector pattern.
    /// * `required` - whether the selector must succeed. If this is true and the selector fails, then
    ///   the connector will terminate.
    /// * `remove_when_no_value` - if the value selector fails to select, then it will emit a map
    ///   remove command to remove the corresponding entry.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::error::Error;
    /// use swimos_connector::PayloadSelector;
    ///
    /// # fn main() -> Result<(), Box<dyn Error + 'static>> {
    /// let selector = PayloadSelector::map("$key", "$value", true, true)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn map(
        key_pattern: &str,
        value_pattern: &str,
        required: bool,
        remove_when_no_value: bool,
    ) -> Result<PayloadSelector, ParseError> {
        let key = parse_pattern(key_pattern.into())?;
        let value = parse_pattern(value_pattern.into())?;
        Ok(PayloadSelector {
            inner: Inner::Map {
                key_pattern: key_pattern.to_string(),
                value_pattern: value_pattern.to_string(),
                remove_when_no_value,
                key,
                value,
            },
            required,
        })
    }

    pub(crate) fn select<'a>(
        &self,
        node_uri: String,
        lane_uri: String,
        args: &PubSubSelectorArgs<'a>,
    ) -> Result<GenericSendCommandOp, SelectorError> {
        let PayloadSelector { inner, required } = self;

        let op = match inner {
            Inner::Value { input, segment } => {
                build_value(*required, input.as_str(), segment, args)?.map(|payload| {
                    SendCommandOp::inject(SendCommand::new(
                        Address::new(None, node_uri, lane_uri),
                        payload,
                        false,
                    ))
                })
            }
            Inner::Map {
                key_pattern,
                value_pattern,
                remove_when_no_value,
                key,
                value,
            } => {
                let key = build_value(*required, key_pattern.as_str(), key, args)?;
                let value = build_value(*required, value_pattern.as_str(), value, args)?;

                match (key, value) {
                    (Some(key_payload), Some(value_payload)) => {
                        let op = SendCommandOp::inject(SendCommand::new(
                            Address::new(None, node_uri, lane_uri),
                            MapMessage::Update {
                                key: key_payload,
                                value: value_payload,
                            },
                            false,
                        ));
                        Some(op)
                    }
                    (Some(key_payload), None) if *remove_when_no_value => {
                        let op = SendCommandOp::inject(SendCommand::new(
                            Address::new(None, node_uri, lane_uri),
                            MapMessage::Remove { key: key_payload },
                            false,
                        ));
                        Some(op)
                    }
                    _ => None,
                }
            }
        };

        Ok(Discard::<Option<SendCommandOp>>::new(op))
    }
}

fn build_value<'a>(
    required: bool,
    pattern: &str,
    segment: &Segment,
    args: &PubSubSelectorArgs<'a>,
) -> Result<Option<Value>, SelectorError> {
    let payload = match segment_to_part(pattern, segment) {
        Part::Static(path) => Ok(Some(swimos_model::Value::from(path.to_string()))),
        Part::Selector(selector) => selector.select(args).map_err(SelectorError::from),
    };

    match payload {
        Ok(Some(payload)) => Ok(Some(payload)),
        Ok(None) => {
            if required {
                Err(SelectorError::Selector(pattern.to_string()))
            } else {
                Ok(None)
            }
        }
        Err(e) => {
            if required {
                Err(e)
            } else {
                Ok(None)
            }
        }
    }
}

fn parse_pattern(span: Span) -> Result<Segment, ParseError> {
    parse_consume(span, parse)
}

fn parse(span: Span) -> IResult<Span, Segment> {
    alt((
        parse_payload_selector.map(Segment::Selector),
        tag("$topic").map(|_| Segment::Selector(SelectorModel::Topic)),
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
