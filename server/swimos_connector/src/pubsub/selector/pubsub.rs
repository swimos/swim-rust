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

use crate::deser::MessagePart;
use crate::pubsub::selector::base::{
    ChainSelector, Deferred, GenericMapLaneOp, MapLaneSelector, Selector,
};
use crate::{BadSelector, DeserializationError, SelectorError};
use regex::Regex;
use std::{fmt::Debug, sync::OnceLock};
use swimos_agent::event_handler::HandlerActionExt;
use swimos_model::Value;

/// A value selector attempts to extract a value from a message to use as a new value for a value,
/// lane an update for a map lane or for use in deriving a node or lane URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueSelector {
    /// Use a selector to select a sub-component of the key of the message.
    Key(ChainSelector),
    /// Use a selector to select a sub-component of the payload of the message.
    Payload(ChainSelector),
}

impl ValueSelector {
    /// Attempt to select a sub-component from a message.
    ///
    /// # Arguments
    /// * `key` - Lazily deserialized message key.
    /// * `payload` - Lazily deserialized message payload.
    pub fn select<'a, K, V>(
        &self,
        key: &'a mut K,
        payload: &'a mut V,
    ) -> Result<Option<&'a Value>, DeserializationError>
    where
        K: Deferred + 'a,
        V: Deferred + 'a,
    {
        Ok(match self {
            ValueSelector::Key(selector) => selector.select(key.get()?),
            ValueSelector::Payload(selector) => selector.select(payload.get()?),
        })
    }
}

/// Enumeration of the components of a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageField {
    Key,
    Payload,
    Topic,
}

impl From<MessagePart> for MessageField {
    fn from(value: MessagePart) -> Self {
        match value {
            MessagePart::Key => MessageField::Key,
            MessagePart::Payload => MessageField::Payload,
        }
    }
}

static INIT_REGEX: OnceLock<Regex> = OnceLock::new();
static FIELD_REGEX: OnceLock<Regex> = OnceLock::new();

/// Regular expression matching the base selectors for topic, key and payload components.
fn init_regex() -> &'static Regex {
    INIT_REGEX.get_or_init(|| create_init_regex().expect("Invalid regex."))
}

/// Regular expression matching the description of a [`BasicSelector`].
fn field_regex() -> &'static Regex {
    FIELD_REGEX.get_or_init(|| create_field_regex().expect("Invalid regex."))
}

fn create_init_regex() -> Result<Regex, regex::Error> {
    Regex::new("\\A(\\$(?:[a-z]+))(?:\\[(\\d+)])?\\z")
}

fn create_field_regex() -> Result<Regex, regex::Error> {
    Regex::new("\\A(\\@?(?:\\w+))(?:\\[(\\d+)])?\\z")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectorComponent<'a> {
    pub is_attr: bool,
    pub name: &'a str,
    pub index: Option<usize>,
}

impl<'a> SelectorComponent<'a> {
    pub fn new(is_attr: bool, name: &'a str, index: Option<usize>) -> Self {
        SelectorComponent {
            is_attr,
            name,
            index,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawSelectorDescriptor<'a> {
    // todo remove pub vis after moving everything else
    pub part: &'a str,
    pub index: Option<usize>,
    pub components: Vec<SelectorComponent<'a>>,
}

// can't use FromStr as it doesn't have a lifetime associated with it.
impl<'a> TryFrom<&'a str> for RawSelectorDescriptor<'a> {
    type Error = BadSelector;

    fn try_from(descriptor: &'a str) -> Result<Self, Self::Error> {
        if descriptor.is_empty() {
            return Err(BadSelector::EmptySelector);
        }
        let mut it = descriptor.split('.');
        let (field, index) = match it.next() {
            Some(root) if !root.is_empty() => {
                if let Some(captures) = init_regex().captures(root) {
                    let field = match captures.get(1) {
                        Some(kind) => kind.as_str(),
                        _ => return Err(BadSelector::InvalidRoot),
                    };
                    let index = if let Some(index_match) = captures.get(2) {
                        Some(index_match.as_str().parse::<usize>()?)
                    } else {
                        None
                    };
                    (field, index)
                } else {
                    return Err(BadSelector::InvalidRoot);
                }
            }
            _ => return Err(BadSelector::EmptyComponent),
        };

        let mut components = vec![];
        for part in it {
            if part.is_empty() {
                return Err(BadSelector::EmptyComponent);
            }
            if let Some(captures) = field_regex().captures(part) {
                let (is_attr, name) = match captures.get(1) {
                    Some(name) if name.as_str().starts_with('@') => (true, &name.as_str()[1..]),
                    Some(name) => (false, name.as_str()),
                    _ => return Err(BadSelector::InvalidComponent),
                };
                let index = if let Some(index_match) = captures.get(2) {
                    Some(index_match.as_str().parse::<usize>()?)
                } else {
                    None
                };
                components.push(SelectorComponent::new(is_attr, name, index));
            } else {
                return Err(BadSelector::InvalidRoot);
            }
        }

        Ok(RawSelectorDescriptor {
            part: field,
            index,
            components,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SelectorDescriptor<'a> {
    Part {
        part: MessagePart,
        index: Option<usize>,
        components: Vec<SelectorComponent<'a>>,
    },
    Topic,
}

impl<'a> TryFrom<RawSelectorDescriptor<'a>> for SelectorDescriptor<'a> {
    type Error = BadSelector;

    fn try_from(value: RawSelectorDescriptor<'a>) -> Result<Self, Self::Error> {
        let RawSelectorDescriptor {
            part,
            index,
            components,
        } = value;
        match part {
            "$topic" => {
                if index.is_none() && components.is_empty() {
                    Ok(SelectorDescriptor::Topic)
                } else {
                    Err(BadSelector::TopicWithComponent)
                }
            }
            "$key" => Ok(SelectorDescriptor::Part {
                part: MessagePart::Key,
                index,
                components,
            }),
            "$payload" => Ok(SelectorDescriptor::Part {
                part: MessagePart::Payload,
                index,
                components,
            }),
            _ => Err(BadSelector::InvalidRoot),
        }
    }
}

impl<'a> SelectorDescriptor<'a> {
    pub fn field(&self) -> MessageField {
        match self {
            SelectorDescriptor::Part { part, .. } => (*part).into(),
            SelectorDescriptor::Topic => MessageField::Topic,
        }
    }

    pub fn suggested_name(&self) -> Option<&'a str> {
        match self {
            SelectorDescriptor::Part {
                part,
                index,
                components,
            } => {
                if let Some(SelectorComponent { name, index, .. }) = components.last() {
                    if index.is_none() {
                        Some(*name)
                    } else {
                        None
                    }
                } else if index.is_none() {
                    Some(match part {
                        MessagePart::Key => "key",
                        MessagePart::Payload => "payload",
                    })
                } else {
                    None
                }
            }
            SelectorDescriptor::Topic => Some("topic"),
        }
    }

    pub fn selector(&self) -> Option<ChainSelector> {
        match self {
            SelectorDescriptor::Part {
                index, components, ..
            } => Some(ChainSelector::new(*index, components)),
            SelectorDescriptor::Topic => None,
        }
    }
}

/// Attempt to parse a descriptor for a selector from a string.
fn parse_selector(descriptor: &str) -> Result<SelectorDescriptor<'_>, BadSelector> {
    RawSelectorDescriptor::try_from(descriptor)?.try_into()
}

/// A value lane selector generates event handlers from messages to update the state of a map lane.
#[derive(Debug, Clone)]
pub struct PubSubMapLaneSelector {
    inner: MapLaneSelector,
}

impl PubSubMapLaneSelector {
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `key_selector` - Selects a component from the message for the map key.
    /// * `value_selector` - Selects a component from the message for the map value.
    /// * `required` - If this is required and the selectors do not return a result, an error will be generated.
    /// * `remove_when_no_value` - If a key is selected but no value is selected, the corresponding entry will be
    ///   removed from the map.
    pub fn new(
        name: String,
        key_selector: ChainSelector,
        value_selector: ChainSelector,
        required: bool,
        remove_when_no_value: bool,
    ) -> Self {
        PubSubMapLaneSelector {
            inner: MapLaneSelector::new(
                name,
                key_selector,
                value_selector,
                required,
                remove_when_no_value,
            ),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name()
    }

    pub fn select_handler<K, V>(
        &self,
        topic: &Value,
        key: &mut K,
        value: &mut V,
    ) -> Result<GenericMapLaneOp, SelectorError>
    where
        K: Deferred,
        V: Deferred,
    {
        unimplemented!()
    }
}
