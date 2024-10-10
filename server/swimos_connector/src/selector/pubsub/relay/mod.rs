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

use crate::selector::{PubSubSelectorArgs, Selector, SelectorError};
use frunk::Coprod;
use swimos_model::Value;

use crate::selector::{parse_selector, BadSelector, PubSubSelector};
use regex::Regex;
use std::{slice::Iter, str::FromStr, sync::OnceLock};
use swimos_agent::event_handler::{Discard, SendCommand};
use swimos_agent_protocol::MapMessage;
use swimos_api::address::Address;
use swimos_recon::parser::{parse_recognize, ParseError};

type SendCommandOp =
    Coprod!(SendCommand<String, Value>, SendCommand<String,MapMessage<Value, Value>>);
pub type GenericSendCommandOp = Discard<Option<SendCommandOp>>;

static STATIC_REGEX: OnceLock<Regex> = OnceLock::new();
static STATIC_PATH_REGEX: OnceLock<Regex> = OnceLock::new();

fn static_regex() -> &'static Regex {
    STATIC_REGEX.get_or_init(|| create_static_regex().expect("Invalid regex."))
}

fn create_static_regex() -> Result<Regex, regex::Error> {
    Regex::new(r"^[a-zA-Z0-9_]+$")
}

fn static_path_regex() -> &'static Regex {
    STATIC_PATH_REGEX.get_or_init(|| create_static_path_regex().expect("Invalid regex."))
}

fn create_static_path_regex() -> Result<Regex, regex::Error> {
    Regex::new(r"^\/?[a-zA-Z0-9_]+(\/[a-zA-Z0-9_]+)*(\/|$)")
}

#[derive(Debug, Clone, PartialEq)]
pub enum Segment {
    Static(String),
    Selector(PubSubSelector),
}

fn parse_segment(pattern: &str) -> Result<Segment, BadSelector> {
    let mut iter = pattern.chars();
    match iter.next() {
        Some('$') => Ok(Segment::Selector(parse_selector(pattern)?.into())),
        Some(_) => {
            if static_regex().is_match(pattern) {
                Ok(Segment::Static(pattern.to_string()))
            } else {
                Err(BadSelector::InvalidPath)
            }
        }
        _ => Err(BadSelector::InvalidPath),
    }
}

#[derive(Debug, Clone, PartialEq)]
struct LaneSelector {
    segment: Segment,
    pattern: String,
}

impl FromStr for LaneSelector {
    type Err = BadSelector;

    fn from_str(pattern: &str) -> Result<Self, Self::Err> {
        Ok(LaneSelector {
            segment: parse_segment(pattern)?,
            pattern: pattern.to_string(),
        })
    }
}

impl LaneSelector {
    fn select(&self, args: &PubSubSelectorArgs<'_>) -> Result<String, SelectorError> {
        let LaneSelector { pattern, segment } = self;

        match segment {
            Segment::Static(p) => Ok(p.to_string()),
            Segment::Selector(selector) => {
                let value = selector
                    .select(args)?
                    .ok_or(SelectorError::Selector(pattern.to_string()))?;
                match value {
                    Value::BooleanValue(v) => {
                        if v {
                            Ok("true".to_string())
                        } else {
                            Ok("false".to_string())
                        }
                    }
                    Value::Int32Value(v) => Ok(v.to_string()),
                    Value::Int64Value(v) => Ok(v.to_string()),
                    Value::UInt32Value(v) => Ok(v.to_string()),
                    Value::UInt64Value(v) => Ok(v.to_string()),
                    Value::BigUint(v) => Ok(v.to_string()),
                    Value::Text(v) => Ok(v.to_string()),
                    _ => Err(SelectorError::InvalidRecord(self.pattern.to_string())),
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeSelector {
    pattern: String,
    segments: Vec<Segment>,
}

impl<'a> IntoIterator for &'a NodeSelector {
    type Item = &'a Segment;
    type IntoIter = Iter<'a, Segment>;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.iter()
    }
}

impl FromStr for NodeSelector {
    type Err = BadSelector;

    fn from_str(mut pattern: &str) -> Result<Self, Self::Err> {
        let input = pattern.to_string();
        if !pattern.starts_with('/') || pattern.len() < 2 {
            return Err(BadSelector::InvalidPath);
        }

        let mut segments = Vec::new();

        loop {
            let mut iter = pattern.chars();
            match iter.next() {
                Some('$') => match pattern.split_once('/') {
                    Some((head, tail)) => {
                        segments.push(Segment::Selector(parse_selector(head)?.into()));
                        pattern = tail;
                    }
                    None => {
                        segments.push(Segment::Selector(parse_selector(pattern)?.into()));
                        break;
                    }
                },
                Some(_) => match static_path_regex().find(pattern) {
                    Some(matched) => {
                        segments.push(Segment::Static(matched.as_str().to_string()));
                        pattern = &pattern[matched.end()..];
                        if pattern.is_empty() {
                            break;
                        } else if pattern.starts_with('/') {
                            // Pattern will capture up to a trailing slash but not a double one, so
                            // guard against the next static segment starting with a slash.
                            return Err(BadSelector::InvalidPath);
                        }
                    }
                    None => return Err(BadSelector::InvalidPath),
                },
                _ => return Err(BadSelector::InvalidPath),
            }
        }

        Ok(NodeSelector {
            segments,
            pattern: input,
        })
    }
}

impl NodeSelector {
    fn select(&self, args: &PubSubSelectorArgs<'_>) -> Result<String, SelectorError> {
        let mut node_uri = String::new();

        for elem in self.into_iter() {
            match elem {
                Segment::Static(p) => node_uri.push_str(p),
                Segment::Selector(selector) => {
                    let value = selector
                        .select(args)?
                        .ok_or(SelectorError::Selector(self.pattern.to_string()))?;
                    match value {
                        Value::BooleanValue(v) => {
                            node_uri.push_str(if v { "true" } else { "false" })
                        }
                        Value::Int32Value(v) => node_uri.push_str(&v.to_string()),
                        Value::Int64Value(v) => node_uri.push_str(&v.to_string()),
                        Value::UInt32Value(v) => node_uri.push_str(&v.to_string()),
                        Value::UInt64Value(v) => node_uri.push_str(&v.to_string()),
                        Value::BigInt(v) => node_uri.push_str(&v.to_string()),
                        Value::BigUint(v) => node_uri.push_str(&v.to_string()),
                        Value::Text(v) => node_uri.push_str(v.as_str()),
                        _ => return Err(SelectorError::InvalidRecord(self.pattern.to_string())),
                    }
                }
            }
        }

        Ok(node_uri)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct PayloadSelector {
    inner: Inner,
    required: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PayloadSegment {
    Value(Value),
    Selector(PubSubSelector),
}

impl TryFrom<Segment> for PayloadSegment {
    type Error = ParseError;

    fn try_from(value: Segment) -> Result<Self, Self::Error> {
        match value {
            Segment::Static(path) => Ok(PayloadSegment::Value(parse_recognize::<Value>(
                path.as_str(),
                false,
            )?)),
            Segment::Selector(s) => Ok(PayloadSegment::Selector(s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Inner {
    Value {
        pattern: String,
        segment: PayloadSegment,
    },
    Map {
        key_pattern: String,
        value_pattern: String,
        remove_when_no_value: bool,
        key: PayloadSegment,
        value: PayloadSegment,
    },
}

impl PayloadSelector {
    pub fn value(pattern: &str, required: bool) -> Result<PayloadSelector, BadSelector> {
        Ok(PayloadSelector {
            inner: Inner::Value {
                pattern: pattern.to_string(),
                segment: parse_segment(pattern)?.try_into()?,
            },
            required,
        })
    }

    pub fn map(
        key_pattern: &str,
        value_pattern: &str,
        required: bool,
        remove_when_no_value: bool,
    ) -> Result<PayloadSelector, BadSelector> {
        let key = parse_segment(key_pattern)?.try_into()?;
        let value = parse_segment(value_pattern)?.try_into()?;
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

    fn select(
        &self,
        node_uri: String,
        lane_uri: String,
        args: &PubSubSelectorArgs<'_>,
    ) -> Result<GenericSendCommandOp, SelectorError> {
        let PayloadSelector { inner, required } = self;
        let op = match inner {
            Inner::Value { pattern, segment } => {
                build_value(*required, pattern.as_str(), segment, args)?.map(|payload| {
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

fn build_value(
    required: bool,
    pattern: &str,
    segment: &PayloadSegment,
    args: &PubSubSelectorArgs<'_>,
) -> Result<Option<Value>, SelectorError> {
    let payload = match segment {
        PayloadSegment::Value(value) => Ok(Some(value.clone())),
        PayloadSegment::Selector(selector) => selector.select(args).map_err(SelectorError::from),
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

/// A collection of relays which are used to derive the commands to send to lanes on agents.
#[derive(Debug, Clone, Default)]
pub struct Relays {
    chain: Vec<Relay>,
}

impl Relays {
    pub fn new<I>(chain: I) -> Relays
    where
        I: IntoIterator<Item = Relay>,
    {
        Relays {
            chain: chain.into_iter().collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.chain.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chain.is_empty()
    }
}

impl From<Relay> for Relays {
    fn from(relays: Relay) -> Relays {
        Relays {
            chain: vec![relays],
        }
    }
}

impl<'s> IntoIterator for &'s Relays {
    type Item = &'s Relay;
    type IntoIter = Iter<'s, Relay>;

    fn into_iter(self) -> Self::IntoIter {
        self.chain.as_slice().iter()
    }
}

/// A relay which is used to build a command to send to a lane on an agent.
#[derive(Debug, Clone)]
pub struct Relay {
    node: NodeSelector,
    lane: LaneSelector,
    payload: PayloadSelector,
}

impl Relay {
    /// Builds a new Value [`Relay`].
    ///
    /// # Arguments
    /// * `node` - node URI selector.
    /// * `lane` - node URI selector.
    /// * `pattern` - the payload selector pattern.
    /// * `required` - whether the selector must succeed. If this is true and the selector fails, then
    ///   the connector will terminate.
    pub fn value(
        node: &str,
        lane: &str,
        payload: &str,
        required: bool,
    ) -> Result<Relay, BadSelector> {
        Ok(Relay {
            node: NodeSelector::from_str(node)?,
            lane: LaneSelector::from_str(lane)?,
            payload: PayloadSelector::value(payload, required)?,
        })
    }

    /// Builds a new Map [`Relay`].
    ///
    /// # Arguments
    /// * `node` - node URI selector.
    /// * `lane` - node URI selector.
    /// * `key_pattern` - the key selector pattern.
    /// * `value_pattern` - the value selector pattern.
    /// * `required` - whether the selector must succeed. If this is true and the selector fails, then
    ///   the connector will terminate.
    /// * `remove_when_no_value` - if the value selector fails to select, then it will emit a map
    ///   remove command to remove the corresponding entry.
    pub fn map(
        node: &str,
        lane: &str,
        key_pattern: &str,
        value_pattern: &str,
        required: bool,
        remove_when_no_value: bool,
    ) -> Result<Relay, BadSelector> {
        Ok(Relay {
            node: NodeSelector::from_str(node)?,
            lane: LaneSelector::from_str(lane)?,
            payload: PayloadSelector::map(
                key_pattern,
                value_pattern,
                required,
                remove_when_no_value,
            )?,
        })
    }

    pub fn select_handler<'a>(
        &self,
        args: &PubSubSelectorArgs<'a>,
    ) -> Result<GenericSendCommandOp, SelectorError> {
        let Relay {
            node,
            lane,
            payload,
        } = self;

        let node_uri = node.select(args)?;
        let lane_uri = lane.select(args)?;
        payload.select(node_uri, lane_uri, args)
    }
}
