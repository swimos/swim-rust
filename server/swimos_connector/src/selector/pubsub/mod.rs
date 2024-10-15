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

mod error;
mod relay;
#[cfg(test)]
mod tests;

use crate::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use crate::deser::{Deferred, MessagePart};
use crate::selector::{MapLaneSelector, SelectorComponent, ValueLaneSelector};
use crate::BadSelector;
use crate::{
    selector::{ChainSelector, Selector, ValueSelector},
    DeserializationError,
};
pub use error::*;
use frunk::{Coprod, HList};
use regex::Regex;
pub use relay::{Relay, Relays};
use std::sync::OnceLock;
use swimos_model::Value;

/// Canonical selector for pub-sub type connectors.
pub type PubSubSelector = Coprod!(TopicSelector, KeySelector, PayloadSelector);
pub type PubSubSelectorArgs<'a> = HList!(
    <TopicSelector as Selector<'a>>::Arg,
    <KeySelector as Selector<'a>>::Arg,
    <PayloadSelector as Selector<'a>>::Arg
);
/// Selector type for Value Lanes.
pub type PubSubValueLaneSelector = ValueLaneSelector<PubSubSelector>;
/// Selector type for Map Lanes.
pub type PubSubMapLaneSelector = MapLaneSelector<PubSubSelector, PubSubSelector>;

#[derive(Debug, PartialEq, Clone, Eq, Default)]
pub struct TopicSelector;

impl<'a> Selector<'a> for TopicSelector {
    type Arg = Value;

    fn select(&self, from: &'a Self::Arg) -> Result<Option<Value>, DeserializationError> {
        Ok(Some(from.clone()))
    }
}

#[derive(Debug, PartialEq, Clone, Eq, Default)]
pub struct KeySelector(ChainSelector);

impl KeySelector {
    pub fn new(inner: ChainSelector) -> KeySelector {
        KeySelector(inner)
    }
}

impl<I> From<I> for KeySelector
where
    I: Into<ChainSelector>,
{
    fn from(value: I) -> Self {
        KeySelector(value.into())
    }
}

impl<'a> Selector<'a> for KeySelector {
    type Arg = Deferred<'a>;

    fn select(&self, from: &'a Self::Arg) -> Result<Option<Value>, DeserializationError> {
        let KeySelector(chain) = self;
        from.with(|val| ValueSelector::select_value(chain, val).cloned())
    }
}

#[derive(Default, Debug, PartialEq, Clone, Eq)]
pub struct PayloadSelector(ChainSelector);

impl PayloadSelector {
    pub fn new(inner: ChainSelector) -> PayloadSelector {
        PayloadSelector(inner)
    }
}

impl<I> From<I> for PayloadSelector
where
    I: Into<ChainSelector>,
{
    fn from(value: I) -> Self {
        PayloadSelector::new(value.into())
    }
}

impl<'a> Selector<'a> for PayloadSelector {
    type Arg = Deferred<'a>;

    fn select(&self, from: &'a Self::Arg) -> Result<Option<Value>, DeserializationError> {
        let PayloadSelector(chain) = self;
        from.with(|val| ValueSelector::select_value(chain, val).cloned())
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawSelectorDescriptor<'a> {
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
pub enum SelectorDescriptor<'a> {
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
pub fn parse_selector(descriptor: &str) -> Result<SelectorDescriptor<'_>, BadSelector> {
    RawSelectorDescriptor::try_from(descriptor)?.try_into()
}

impl TryFrom<&IngressValueLaneSpec> for PubSubValueLaneSelector {
    type Error = InvalidLaneSpec;

    fn try_from(value: &IngressValueLaneSpec) -> Result<Self, Self::Error> {
        let IngressValueLaneSpec {
            name,
            selector,
            required,
        } = value;
        let parsed = parse_selector(selector.as_str())?;
        if let Some(lane_name) = name
            .as_ref()
            .cloned()
            .or_else(|| parsed.suggested_name().map(|s| s.to_owned()))
        {
            Ok(ValueLaneSelector::new(lane_name, parsed.into(), *required))
        } else {
            Err(InvalidLaneSpec::NameCannotBeInferred)
        }
    }
}

impl<'a> From<SelectorDescriptor<'a>> for PubSubSelector {
    fn from(parsed: SelectorDescriptor<'a>) -> Self {
        match (parsed.field(), parsed.selector()) {
            (MessageField::Key, Some(selector)) => PubSubSelector::inject(KeySelector(selector)),
            (MessageField::Payload, Some(selector)) => {
                PubSubSelector::inject(PayloadSelector(selector))
            }
            _ => PubSubSelector::inject(TopicSelector),
        }
    }
}

impl TryFrom<&IngressMapLaneSpec> for PubSubMapLaneSelector {
    type Error = InvalidLaneSpec;

    fn try_from(value: &IngressMapLaneSpec) -> Result<Self, Self::Error> {
        let IngressMapLaneSpec {
            name,
            key_selector,
            value_selector,
            remove_when_no_value,
            required,
        } = value;
        Ok(PubSubMapLaneSelector::new(
            name.clone(),
            parse_selector(key_selector.as_str())?.into(),
            parse_selector(value_selector.as_str())?.into(),
            *required,
            *remove_when_no_value,
        ))
    }
}
