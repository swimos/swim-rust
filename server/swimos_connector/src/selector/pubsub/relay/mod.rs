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

use crate::BadSelector;
use swimos_model::Value;

use crate::selector::relay::{
    LaneSelector, NodeSelector, PayloadSegment, RelayPayloadSelector, Segment,
};
use crate::selector::{parse_selector, PubSubSelector};
use regex::Regex;
use std::sync::OnceLock;
use swimos_recon::parser::{parse_recognize, ParseError};

use super::{InterpretableSelector, RawSelectorDescriptor};

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
