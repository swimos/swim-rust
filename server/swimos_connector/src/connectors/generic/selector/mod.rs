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

use std::{fmt::Debug, sync::OnceLock};

use frunk::Coprod;
use regex::Regex;
use swimos_agent::event_handler::{Discard, HandlerActionExt};
use swimos_model::Value;
use tracing::{error, trace};

use crate::connectors::generic::config::{MapLaneSpec, ValueLaneSpec};
use crate::deserialization::{Deferred, MessagePart};
use crate::generic::{
    BadSelector, GenericConnectorAgent, InvalidLaneSpec, LaneSelectorError, MapLaneSelectorFn,
    ValueLaneSelectorFn,
};
use crate::selector::{
    AttrSelector, BasicSelector, ChainSelector, IndexSelector, SlotSelector, ValueSelector,
};
use swimos_agent::lanes::{MapLaneSelectRemove, MapLaneSelectUpdate, ValueLaneSelectSet};

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
    Regex::new("\\A(\\$(?:key|payload|topic))(?:\\[(\\d+)])?\\z")
}

fn create_field_regex() -> Result<Regex, regex::Error> {
    Regex::new("\\A(\\@?(?:\\w+))(?:\\[(\\d+)])?\\z")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectorComponent<'a> {
    is_attr: bool,
    name: &'a str,
    index: Option<usize>,
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
enum SelectorDescriptor<'a> {
    Part {
        part: MessagePart,
        index: Option<usize>,
        components: Vec<SelectorComponent<'a>>,
    },
    Topic,
}

impl<'a> From<SelectorDescriptor<'a>> for ValueSelector {
    fn from(value: SelectorDescriptor<'a>) -> Self {
        match (value.field(), value.selector()) {
            (MessageField::Key, Some(selector)) => ValueSelector::Key(selector),
            (MessageField::Payload, Some(selector)) => ValueSelector::Payload(selector),
            _ => ValueSelector::Topic,
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
            } => {
                let mut links = vec![];
                if let Some(n) = index {
                    links.push(BasicSelector::Index(IndexSelector::new(*n)));
                }
                for SelectorComponent {
                    is_attr,
                    name,
                    index,
                } in components
                {
                    links.push(if *is_attr {
                        BasicSelector::Attr(AttrSelector::new(name.to_string()))
                    } else {
                        BasicSelector::Slot(SlotSelector::for_field(*name))
                    });
                    if let Some(n) = index {
                        links.push(BasicSelector::Index(IndexSelector::new(*n)));
                    }
                }
                Some(ChainSelector::new(links))
            }
            SelectorDescriptor::Topic => None,
        }
    }
}

/// Attempt to parse a descriptor for a selector from a string.
fn parse_selector(descriptor: &str) -> Result<SelectorDescriptor<'_>, BadSelector> {
    if descriptor.is_empty() {
        return Err(BadSelector::EmptySelector);
    }
    let mut it = descriptor.split('.');
    let (field, index) = match it.next() {
        Some(root) if !root.is_empty() => {
            if let Some(captures) = init_regex().captures(root) {
                let field = match captures.get(1) {
                    Some(kind) if kind.as_str() == "$key" => MessageField::Key,
                    Some(kind) if kind.as_str() == "$payload" => MessageField::Payload,
                    Some(kind) if kind.as_str() == "$topic" => MessageField::Topic,
                    _ => return Err(BadSelector::InvalidRoot),
                };
                let index = if let Some(index_match) = captures.get(2) {
                    if field != MessageField::Topic {
                        Some(index_match.as_str().parse::<usize>()?)
                    } else {
                        return Err(BadSelector::InvalidRoot);
                    }
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

    let part = match field {
        MessageField::Key => MessagePart::Key,
        MessageField::Payload => MessagePart::Payload,
        MessageField::Topic => {
            if components.is_empty() {
                return Ok(SelectorDescriptor::Topic);
            } else {
                return Err(BadSelector::TopicWithComponent);
            }
        }
    };

    Ok(SelectorDescriptor::Part {
        part,
        index,
        components,
    })
}

/// A value lane selector generates event handlers from messages to update the state of a value lane.
#[derive(Debug, Clone)]
pub struct ValueLaneSelector {
    name: String,
    selector: ValueSelector,
    required: bool,
}

impl ValueLaneSelector {
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `selector` - Selects a component from the message.
    /// * `required` - If this is required and the selector does not return a result, an error will be generated.
    pub fn new(name: String, selector: ValueSelector, required: bool) -> Self {
        ValueLaneSelector {
            name,
            selector,
            required,
        }
    }
}

/// A value lane selector generates event handlers from messages to update the state of a map lane.
#[derive(Debug, Clone)]
pub struct MapLaneSelector {
    name: String,
    key_selector: ValueSelector,
    value_selector: ValueSelector,
    required: bool,
    remove_when_no_value: bool,
}

impl MapLaneSelector {
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `key_selector` - Selects a component from the message for the map key.
    /// * `value_selector` - Selects a component from the message for the map value.
    /// * `required` - If this is required and the selectors do not return a result, an error will be generated.
    /// * `remove_when_no_value` - If a key is selected but no value is selected, the corresponding entry will be
    ///   removed from the map.
    pub fn new(
        name: String,
        key_selector: ValueSelector,
        value_selector: ValueSelector,
        required: bool,
        remove_when_no_value: bool,
    ) -> Self {
        MapLaneSelector {
            name,
            key_selector,
            value_selector,
            required,
            remove_when_no_value,
        }
    }
}

impl TryFrom<&ValueLaneSpec> for ValueLaneSelector {
    type Error = InvalidLaneSpec;

    fn try_from(value: &ValueLaneSpec) -> Result<Self, Self::Error> {
        let ValueLaneSpec {
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

impl TryFrom<&MapLaneSpec> for MapLaneSelector {
    type Error = InvalidLaneSpec;

    fn try_from(value: &MapLaneSpec) -> Result<Self, Self::Error> {
        let MapLaneSpec {
            name,
            key_selector,
            value_selector,
            remove_when_no_value,
            required,
        } = value;
        let key = ValueSelector::from(parse_selector(key_selector.as_str())?);
        let value = ValueSelector::from(parse_selector(value_selector.as_str())?);
        Ok(MapLaneSelector::new(
            name.clone(),
            key,
            value,
            *required,
            *remove_when_no_value,
        ))
    }
}

pub type GenericValueLaneSet =
    Discard<Option<ValueLaneSelectSet<GenericConnectorAgent, Value, ValueLaneSelectorFn>>>;

impl ValueLaneSelector {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn select_handler<K, V>(
        &self,
        topic: &Value,
        key: &mut K,
        value: &mut V,
    ) -> Result<GenericValueLaneSet, LaneSelectorError>
    where
        K: Deferred,
        V: Deferred,
    {
        let ValueLaneSelector {
            name,
            selector,
            required,
        } = self;
        let maybe_value = selector.select(topic, key, value)?;
        let handler = match maybe_value {
            Some(value) => {
                trace!(name, value = %value, "Setting a value extracted from a message to a value lane.");
                let select_lane = ValueLaneSelectorFn::new(name.clone());
                Some(ValueLaneSelectSet::new(select_lane, value.clone()))
            }
            None => {
                if *required {
                    error!(name, "A message did not contain a required value.");
                    return Err(LaneSelectorError::MissingRequiredLane(name.clone()));
                } else {
                    None
                }
            }
        };
        Ok(handler.discard())
    }
}

type MapLaneUpdate = MapLaneSelectUpdate<GenericConnectorAgent, Value, Value, MapLaneSelectorFn>;
type MapLaneRemove = MapLaneSelectRemove<GenericConnectorAgent, Value, Value, MapLaneSelectorFn>;
type MapLaneOp = Coprod!(MapLaneUpdate, MapLaneRemove);

pub type GenericMapLaneOp = Discard<Option<MapLaneOp>>;

impl MapLaneSelector {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn select_handler<K, V>(
        &self,
        topic: &Value,
        key: &mut K,
        value: &mut V,
    ) -> Result<GenericMapLaneOp, LaneSelectorError>
    where
        K: Deferred,
        V: Deferred,
    {
        let MapLaneSelector {
            name,
            key_selector,
            value_selector,
            required,
            remove_when_no_value,
        } = self;
        let maybe_key: Option<Value> = key_selector.select(topic, key, value)?.cloned();
        let maybe_value = value_selector.select(topic, key, value)?;
        let select_lane = MapLaneSelectorFn::new(name.clone());
        let handler: Option<MapLaneOp> = match (maybe_key, maybe_value) {
            (None, _) if *required => {
                error!(
                    name,
                    "A message did not contain a required map lane update/removal."
                );
                return Err(LaneSelectorError::MissingRequiredLane(name.clone()));
            }
            (Some(key), None) if *remove_when_no_value => {
                trace!(name, key = %key, "Removing an entry from a map lane with a key extracted from a message.");
                let remove = MapLaneSelectRemove::new(select_lane, key);
                Some(MapLaneOp::inject(remove))
            }
            (Some(key), Some(value)) => {
                trace!(name, key = %key, value = %value, "Updating a map lane with an entry extracted from a message.");
                let update = MapLaneSelectUpdate::new(select_lane, key, value.clone());
                Some(MapLaneOp::inject(update))
            }
            _ => None,
        };
        Ok(handler.discard())
    }
}
