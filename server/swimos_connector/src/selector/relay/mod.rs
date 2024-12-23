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

use crate::{selector::Selector, BadSelector, SelectorError};
use frunk::Coprod;
use regex::Regex;
use swimos_model::Value;
use swimos_recon::parser::{parse_recognize, ParseError};

use std::{slice::Iter, sync::OnceLock};
use swimos_agent::event_handler::{Discard, SendCommand};
use swimos_agent_protocol::MapMessage;
use swimos_api::address::Address;

use super::{InterpretableSelector, RawSelectorDescriptor};

#[cfg(test)]
mod tests;

type SendCommandOp =
    Coprod!(SendCommand<String, Value>, SendCommand<String,MapMessage<Value, Value>>);
pub type GenericSendCommandOp = Discard<Option<SendCommandOp>>;

/// A segment selector in a URI. A segment is the part between two consecutive '/' in a URI. This
/// may delegate to a selector to derive the segment from a key, payload or topic, or yield a static
/// segment.
#[derive(Debug, Clone, PartialEq)]
pub enum Segment<S> {
    /// Yield a static segment for the URI.
    Static(String),
    /// Build the URI segment from a selector.
    Selector(S),
}

/// A lane URI selector.
#[derive(Debug, Clone, PartialEq)]
pub struct LaneSelector<S> {
    segment: Segment<S>,
    pattern: String,
}

impl<S> LaneSelector<S> {
    /// Builds a new [`LaneSelector`].
    ///
    /// # Arguments
    /// * `segment` - the selector for the lane.
    /// * `pattern` - the pattern which the selector represents. Used to build an error message if
    ///   the selector fails.
    pub fn new(segment: Segment<S>, pattern: String) -> LaneSelector<S> {
        LaneSelector { segment, pattern }
    }

    fn select<A>(&self, args: &mut A) -> Result<String, SelectorError>
    where
        S: Selector<A>,
    {
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

/// A URI selector for an agent node. [`NodeSelector`] takes a Vec of segments, where each segment
/// represents the portion of the node's URI between consecutive slashes. Each segment can either be
/// a fixed string or a value, determined by the selector type used by this NodeSelector.
///
/// This type is generally built using the connector's message type implementation. For publish-subscribe
/// type connectors see [`crate::selector::parse_node_selector`].
#[derive(Debug, Clone, PartialEq)]
pub struct NodeSelector<S> {
    pattern: String,
    segments: Vec<Segment<S>>,
}

impl<'a, S> IntoIterator for &'a NodeSelector<S> {
    type Item = &'a Segment<S>;
    type IntoIter = Iter<'a, Segment<S>>;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.iter()
    }
}

impl<S> NodeSelector<S> {
    /// Builds a new [`NodeSelector`].
    ///
    /// # Arguments
    /// * `segment` - the selector for the node.
    /// * `pattern` - the pattern which the selector represents. Used to build an error message if
    ///   the selector fails.
    pub fn new(pattern: String, segments: Vec<Segment<S>>) -> NodeSelector<S> {
        NodeSelector { pattern, segments }
    }

    fn select<A>(&self, args: &mut A) -> Result<String, SelectorError>
    where
        S: Selector<A>,
    {
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

/// A command message's payload selector.
#[derive(Debug, Clone, PartialEq)]
pub struct RelayPayloadSelector<S> {
    /// Abstraction over a value or map command selector.
    inner: Inner<S>,
    /// Whether the selector must yield a value. If a selector fails to yield a value then an error
    /// will be returned.
    required: bool,
}

/// A payload selector model. When called, will either yield a [`Value`] or delegate to `S`.
#[derive(Debug, Clone, PartialEq)]
pub enum PayloadSegment<S> {
    /// Yield a value.
    Value(Value),
    /// Delegate to a [`Selector`].
    Selector(S),
}

#[derive(Debug, Clone, PartialEq)]
enum Inner<S> {
    Value {
        pattern: String,
        segment: PayloadSegment<S>,
    },
    Map {
        key_pattern: String,
        value_pattern: String,
        remove_when_no_value: bool,
        key: PayloadSegment<S>,
        value: PayloadSegment<S>,
    },
}

impl<S> RelayPayloadSelector<S> {
    /// Builds a new Value [`RelayPayloadSelector`].
    ///
    /// # Arguments
    /// * `segment` - the selector for the payload.
    /// * `pattern` - the pattern that this selector represents. Used to build an error
    ///   message if the selector fails.
    /// * `required` - whether the selector must succeed. If this is true and the selector fails, then
    ///   the connector will terminate.
    pub fn value(
        segment: PayloadSegment<S>,
        pattern: String,
        required: bool,
    ) -> RelayPayloadSelector<S> {
        RelayPayloadSelector {
            inner: Inner::Value { pattern, segment },
            required,
        }
    }

    /// Builds a new Map [`RelayPayloadSelector`].
    ///
    /// # Arguments
    /// * `key_segment` - the key selector for the payload.
    /// * `value_segment` - the value selector for the payload.
    /// * `key_pattern` - the key pattern that this selector represents. Used to build an error message
    ///   if the key selector fails.
    /// * `value_segment` - the value pattern that this selector represents. Used to build an error
    ///   message if the value selector fails.
    /// * `remove_when_no_value` - if the value selector fails to select, then it will emit a map
    ///   remove command to remove the corresponding entry.
    /// * `required` - whether the selector must succeed. If this is true and the selector fails, then
    ///   the connector will terminate.
    pub fn map(
        key_segment: PayloadSegment<S>,
        value_segment: PayloadSegment<S>,
        key_pattern: String,
        value_pattern: String,
        remove_when_no_value: bool,
        required: bool,
    ) -> RelayPayloadSelector<S> {
        RelayPayloadSelector {
            inner: Inner::Map {
                key_pattern,
                value_pattern,
                remove_when_no_value,
                key: key_segment,
                value: value_segment,
            },
            required,
        }
    }

    fn select<A>(
        &self,
        node_uri: String,
        lane_uri: String,
        args: &mut A,
    ) -> Result<GenericSendCommandOp, SelectorError>
    where
        S: Selector<A>,
    {
        let RelayPayloadSelector { inner, required } = self;
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

fn build_value<S, A>(
    required: bool,
    pattern: &str,
    segment: &PayloadSegment<S>,
    args: &mut A,
) -> Result<Option<Value>, SelectorError>
where
    S: Selector<A>,
{
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
#[derive(Debug, Clone)]
pub struct Relays<S> {
    chain: Vec<Relay<S>>,
}

impl<S> Default for Relays<S> {
    fn default() -> Self {
        Relays { chain: vec![] }
    }
}

impl<S> Relays<S> {
    pub fn new<I>(chain: I) -> Relays<S>
    where
        I: IntoIterator<Item = Relay<S>>,
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

impl<S> From<Relay<S>> for Relays<S> {
    fn from(relays: Relay<S>) -> Relays<S> {
        Relays {
            chain: vec![relays],
        }
    }
}

impl<'s, S> IntoIterator for &'s Relays<S> {
    type Item = &'s Relay<S>;
    type IntoIter = Iter<'s, Relay<S>>;

    fn into_iter(self) -> Self::IntoIter {
        self.chain.as_slice().iter()
    }
}

/// A relay which is used to build a command to send to a lane on an agent.
#[derive(Debug, Clone)]
pub struct Relay<S> {
    node: NodeSelector<S>,
    lane: LaneSelector<S>,
    payload: RelayPayloadSelector<S>,
}

impl<S> Relay<S> {
    pub fn new(
        node: NodeSelector<S>,
        lane: LaneSelector<S>,
        payload: RelayPayloadSelector<S>,
    ) -> Relay<S> {
        Relay {
            node,
            lane,
            payload,
        }
    }

    pub fn select_handler<A>(&self, args: &mut A) -> Result<GenericSendCommandOp, SelectorError>
    where
        S: Selector<A>,
    {
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

fn parse_segment<S>(pattern: &str) -> Result<Segment<S>, BadSelector>
where
    S: InterpretableSelector,
{
    let mut iter = pattern.chars();
    match iter.next() {
        Some('$') => Ok(Segment::Selector(interp(pattern)?)),
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

/// Parses a publish-subscribe [`LaneSelector`].
///
/// Publish-subscribe node URI selectors define three selector keywords which may be used at any
/// part of the pattern.
/// * `$topic` - selects the segment from the connector's topic. E.g, "/agents/$topic".
/// * `$key` - selects the segment from the message's key. E.g, "/agents/$key".
/// * `$payload` - selects the segment from the message's value. E.g, "/agents/$payload".
///
/// `$key` and `$payload` selectors yield Recon [`Value`]'s and allow for selecting attributes and
/// items from the key or value of the message.
///
/// # Static Example
/// "lights".
///
/// # Dynamic Example
/// "$payload.id".
pub fn parse_lane_selector<S>(pattern: &str) -> Result<LaneSelector<S>, BadSelector>
where
    S: InterpretableSelector,
{
    Ok(LaneSelector::new(
        parse_segment(pattern)?,
        pattern.to_string(),
    ))
}

fn interp<S: InterpretableSelector>(rep: &str) -> Result<S, BadSelector> {
    let desc = RawSelectorDescriptor::try_from(rep)?;
    let s = S::try_interp(&desc)?;
    if let Some(s) = s {
        Ok(s)
    } else {
        Err(BadSelector::InvalidRoot)
    }
}

/// Parses a publish-subscribe [`NodeSelector`].
///
/// Publish-subscribe node URI selectors define three selector keywords which may be used at any
/// part of the pattern.
/// * `$topic` - selects the segment from the connector's topic. E.g, "/agents/$topic".
/// * `$key` - selects the segment from the message's key. E.g, "/agents/$key".
/// * `$payload` - selects the segment from the message's value. E.g, "/agents/$payload".
///
/// `$key` and `$payload` selectors yield Recon [`Value`]'s and allow for selecting attributes and
/// items from the key or value of the message.
///
/// # Static Example
/// "/agents/lights".
///
/// # Dynamic Example
/// "/$topic/$key/$payload.id".
pub fn parse_node_selector<S>(mut pattern: &str) -> Result<NodeSelector<S>, BadSelector>
where
    S: InterpretableSelector,
{
    let input = pattern.to_string();
    if !pattern.starts_with('/') || pattern.len() < 2 {
        return Err(BadSelector::InvalidPath);
    }

    let mut segments: Vec<Segment<S>> = Vec::new();

    loop {
        let mut iter = pattern.chars();
        match iter.next() {
            Some('$') => match pattern.split_once('/') {
                Some((head, tail)) => {
                    segments.push(Segment::Selector(interp(head)?));
                    pattern = tail;
                }
                None => {
                    segments.push(Segment::Selector(interp(pattern)?));
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

    Ok(NodeSelector::new(input, segments))
}

impl<S> TryFrom<Segment<S>> for PayloadSegment<S> {
    type Error = ParseError;

    fn try_from(value: Segment<S>) -> Result<Self, Self::Error> {
        match value {
            Segment::Static(path) => Ok(PayloadSegment::Value(parse_recognize::<Value>(
                path.as_str(),
                false,
            )?)),
            Segment::Selector(s) => Ok(PayloadSegment::Selector(s)),
        }
    }
}

/// Parses a Value Lane selector.
///
/// # Arguments
/// * `pattern` - the selector pattern to parse. This may be defined as a Recon [`Value`] which may
///   be used as the key or value for the message to send to the lane.
/// * `required` - whether the selector must succeed. If this is true and the selector fails, then
///   the connector will terminate.
///
/// Both key and value selectors may define a Recon [`Value`] which may be used as the key or value
/// for the message to send to the lane.
pub fn parse_value_selector<S>(
    pattern: &str,
    required: bool,
) -> Result<RelayPayloadSelector<S>, BadSelector>
where
    S: InterpretableSelector,
{
    Ok(RelayPayloadSelector::value(
        parse_segment(pattern)?.try_into()?,
        pattern.to_string(),
        required,
    ))
}

/// Parses a Value Lane selector.
///
/// # Arguments
/// * `key_pattern` - the key selector pattern to parse.
/// * `value_pattern` - the key selector pattern to parse.
/// * `required` - whether the selector must succeed. If this is true and the selector fails, then
///   the connector will terminate.
/// * `remove_when_no_value` - if the value selector fails to select, then it will emit a map
///   remove command to remove the corresponding entry.
///
/// Both key and value selectors may define a Recon [`Value`] which may be used as the key or value
/// for the message to send to the lane.
pub fn parse_map_selector<S>(
    key_pattern: &str,
    value_pattern: &str,
    required: bool,
    remove_when_no_value: bool,
) -> Result<RelayPayloadSelector<S>, BadSelector>
where
    S: InterpretableSelector,
{
    let key = parse_segment(key_pattern)?.try_into()?;
    let value = parse_segment(value_pattern)?.try_into()?;
    Ok(RelayPayloadSelector::map(
        key,
        value,
        key_pattern.to_string(),
        value_pattern.to_string(),
        required,
        remove_when_no_value,
    ))
}
