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

mod message;
#[cfg(test)]
mod tests;

use std::{fmt::Debug, sync::OnceLock};

use frunk::Coprod;
use regex::Regex;
use swimos_agent::event_handler::{Discard, HandlerActionExt};
use swimos_connector::{ConnectorAgent, MapLaneSelectorFn, ValueLaneSelectorFn};
use swimos_model::{Attr, Item, Text, Value};
use tracing::{error, trace};

use crate::{
    config::{IngressMapLaneSpec, IngressValueLaneSpec},
    deser::MessagePart,
    error::{DeserializationError, LaneSelectorError},
    BadSelector, InvalidLaneSpec,
};
use swimos_agent::lanes::{MapLaneSelectRemove, MapLaneSelectUpdate, ValueLaneSelectSet};

pub use message::{KeyOrValue, MessageSelector, FieldSelector, TopicSelector};

/// Enumeration of the components of a Kafka message.
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

/// A lazy loader for a component of a Kafka messages. This ensures that deserializers are only run if a selector
/// refers to a component.
pub trait Deferred {
    /// Get the deserialized component (computing it on the first call).
    fn get(&mut self) -> Result<&Value, DeserializationError>;
}

/// A selector attempts to choose some sub-component of a [`Value`], matching against a pattern, returning
/// nothing if the pattern does not match.
pub trait Selector: Debug {
    /// Attempt to select some sub-component of the provided [`Value`].
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value>;
}

/// Canonical implementation of [`Deferred`].
pub struct Computed<F> {
    inner: Option<Value>,
    f: F,
}

impl<F> Computed<F>
where
    F: Fn() -> Result<Value, DeserializationError>,
{
    pub fn new(f: F) -> Self {
        Computed { inner: None, f }
    }
}

impl<F> Deferred for Computed<F>
where
    F: Fn() -> Result<Value, DeserializationError>,
{
    fn get(&mut self) -> Result<&Value, DeserializationError> {
        let Computed { inner, f } = self;
        if let Some(v) = inner {
            Ok(v)
        } else {
            Ok(inner.insert(f()?))
        }
    }
}

/// A lane selector attempts to extract a value from a Kafka message to use as a new value for a value lane
/// or an update for a map lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaneSelector {
    /// Select the topic from the message.
    Topic,
    /// Use a selector to select a sub-component of the key of the message.
    Key(ChainSelector),
    /// Use a selector to select a sub-component of the payload of the message.
    Payload(ChainSelector),
}

impl<'a> From<SelectorDescriptor<'a>> for LaneSelector {
    fn from(value: SelectorDescriptor<'a>) -> Self {
        match (value.field(), value.selector()) {
            (MessageField::Key, Some(selector)) => LaneSelector::Key(selector),
            (MessageField::Payload, Some(selector)) => LaneSelector::Payload(selector),
            _ => LaneSelector::Topic,
        }
    }
}

impl LaneSelector {
    /// Attempt to select a sub-component from a Kafka message.
    ///
    /// # Arguments
    /// * `topic` - The topic of the message.
    /// * `key` - Lazily deserialized message key.
    /// * `payload` - Lazily deserialized message payload.
    pub fn select<'a, K, V>(
        &self,
        topic: &'a Value,
        key: &'a mut K,
        payload: &'a mut V,
    ) -> Result<Option<&'a Value>, DeserializationError>
    where
        K: Deferred + 'a,
        V: Deferred + 'a,
    {
        Ok(match self {
            LaneSelector::Topic => Some(topic),
            LaneSelector::Key(selector) => selector.select(key.get()?),
            LaneSelector::Payload(selector) => selector.select(payload.get()?),
        })
    }
}

/// Trivial selector that chooses the entire input value.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentitySelector;

impl Selector for IdentitySelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        Some(value)
    }
}

/// A selector that chooses the value of a named attribute if the value is a record and that attribute exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttrSelector {
    select_name: String,
}

impl AttrSelector {
    /// # Arguments
    /// * `name` - The name of the attribute.
    fn new(name: String) -> Self {
        AttrSelector { select_name: name }
    }
}

impl Selector for AttrSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let AttrSelector { select_name } = self;
        match value {
            Value::Record(attrs, _) => attrs.iter().find_map(|Attr { name, value }: &Attr| {
                if name.as_str() == select_name.as_str() {
                    Some(value)
                } else {
                    None
                }
            }),
            _ => None,
        }
    }
}

/// A selector that chooses the value of a slot if the value is a record and that slot exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSelector {
    select_key: Value,
}

impl SlotSelector {
    /// Construct a slot selector for a named field.
    ///
    /// # Arguments
    /// * `name` - The name of the field.
    pub fn for_field(name: impl Into<Text>) -> Self {
        SlotSelector {
            select_key: Value::text(name),
        }
    }
}

impl Selector for SlotSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let SlotSelector { select_key } = self;
        match value {
            Value::Record(_, items) => items.iter().find_map(|item: &Item| match item {
                Item::Slot(key, value) if key == select_key => Some(value),
                _ => None,
            }),
            _ => None,
        }
    }
}

/// A selector that chooses an item by index if the value is a record and has a sufficient number of items. If
/// the selected item is a slot, its value is selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexSelector {
    index: usize,
}

impl IndexSelector {
    /// # Arguments
    /// * `index` - The index in the record to select.
    pub fn new(index: usize) -> Self {
        IndexSelector { index }
    }
}

impl Selector for IndexSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let IndexSelector { index } = self;
        match value {
            Value::Record(_, items) => items.get(*index).map(|item| match item {
                Item::ValueItem(v) => v,
                Item::Slot(_, v) => v,
            }),
            _ => None,
        }
    }
}

/// One of an [`AttrSelector`], [`SlotSelector`] or [`IndexSelector`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BasicSelector {
    Attr(AttrSelector),
    Slot(SlotSelector),
    Index(IndexSelector),
}

impl From<AttrSelector> for BasicSelector {
    fn from(value: AttrSelector) -> Self {
        BasicSelector::Attr(value)
    }
}

impl From<SlotSelector> for BasicSelector {
    fn from(value: SlotSelector) -> Self {
        BasicSelector::Slot(value)
    }
}

impl From<IndexSelector> for BasicSelector {
    fn from(value: IndexSelector) -> Self {
        BasicSelector::Index(value)
    }
}

impl Selector for BasicSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        match self {
            BasicSelector::Attr(s) => s.select(value),
            BasicSelector::Slot(s) => s.select(value),
            BasicSelector::Index(s) => s.select(value),
        }
    }
}

/// A selector that applies a sequence of simpler selectors, in order, passing the result of one selector to the next.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct ChainSelector(Vec<BasicSelector>);

impl ChainSelector {
    /// # Arguments
    /// * `selector` - A chain of simpler selectors.
    pub fn new(selectors: Vec<BasicSelector>) -> Self {
        ChainSelector(selectors)
    }
}

impl Selector for ChainSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let mut v = Some(value);
        let ChainSelector(selectors) = self;
        for s in selectors {
            let selected = if let Some(v) = v {
                s.select(v)
            } else {
                break;
            };
            v = selected;
        }
        v
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
struct RawSelectorDescriptor<'a> {
    part: &'a str,
    index: Option<usize>,
    components: Vec<SelectorComponent<'a>>,
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

fn make_chain_selector(
    index: Option<usize>,
    components: &[SelectorComponent<'_>],
) -> ChainSelector {
    let mut links = vec![];
    if let Some(n) = index {
        links.push(BasicSelector::Index(IndexSelector::new(n)));
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
    ChainSelector::new(links)
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
                Some(make_chain_selector(*index, components))
            }
            SelectorDescriptor::Topic => None,
        }
    }
}

/// Attempt to parse a descriptor for a selector from a string.
fn parse_selector(descriptor: &str) -> Result<SelectorDescriptor<'_>, BadSelector> {
    parse_raw_selector(descriptor)?.try_into()
}

/// Attempt to parse a descriptor for a selector from a string.
fn parse_raw_selector(descriptor: &str) -> Result<RawSelectorDescriptor<'_>, BadSelector> {
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

/// A value lane selector generates event handlers from Kafka messages to update the state of a value lane.
#[derive(Debug, Clone)]
pub struct ValueLaneSelector {
    name: String,
    selector: LaneSelector,
    required: bool,
}

impl ValueLaneSelector {
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `selector` - Selects a component from the message.
    /// * `required` - If this is required and the selector does not return a result, an error will be generated.
    pub fn new(name: String, selector: LaneSelector, required: bool) -> Self {
        ValueLaneSelector {
            name,
            selector,
            required,
        }
    }
}

/// A value lane selector generates event handlers from Kafka messages to update the state of a map lane.
#[derive(Debug, Clone)]
pub struct MapLaneSelector {
    name: String,
    key_selector: LaneSelector,
    value_selector: LaneSelector,
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
        key_selector: LaneSelector,
        value_selector: LaneSelector,
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

impl TryFrom<&IngressValueLaneSpec> for ValueLaneSelector {
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

impl TryFrom<&IngressMapLaneSpec> for MapLaneSelector {
    type Error = InvalidLaneSpec;

    fn try_from(value: &IngressMapLaneSpec) -> Result<Self, Self::Error> {
        let IngressMapLaneSpec {
            name,
            key_selector,
            value_selector,
            remove_when_no_value,
            required,
        } = value;
        let key = LaneSelector::from(parse_selector(key_selector.as_str())?);
        let value = LaneSelector::from(parse_selector(value_selector.as_str())?);
        Ok(MapLaneSelector::new(
            name.clone(),
            key,
            value,
            *required,
            *remove_when_no_value,
        ))
    }
}

type GenericValueLaneSet =
    Discard<Option<ValueLaneSelectSet<ConnectorAgent, Value, ValueLaneSelectorFn>>>;

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
                trace!(name, value = %value, "Setting a value extracted from a Kafka message to a value lane.");
                let select_lane = ValueLaneSelectorFn::new(name.clone());
                Some(ValueLaneSelectSet::new(select_lane, value.clone()))
            }
            None => {
                if *required {
                    error!(name, "A Kafka message did not contain a required value.");
                    return Err(LaneSelectorError::MissingRequiredLane(name.clone()));
                } else {
                    None
                }
            }
        };
        Ok(handler.discard())
    }
}

type MapLaneUpdate = MapLaneSelectUpdate<ConnectorAgent, Value, Value, MapLaneSelectorFn>;
type MapLaneRemove = MapLaneSelectRemove<ConnectorAgent, Value, Value, MapLaneSelectorFn>;
type MapLaneOp = Coprod!(MapLaneUpdate, MapLaneRemove);

type GenericMapLaneOp = Discard<Option<MapLaneOp>>;

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
                    "A Kafka message did not contain a required map lane update/removal."
                );
                return Err(LaneSelectorError::MissingRequiredLane(name.clone()));
            }
            (Some(key), None) if *remove_when_no_value => {
                trace!(name, key = %key, "Removing an entry from a map lane with a key extracted from a Kafka message.");
                let remove = MapLaneSelectRemove::new(select_lane, key);
                Some(MapLaneOp::inject(remove))
            }
            (Some(key), Some(value)) => {
                trace!(name, key = %key, value = %value, "Updating a map lane with an entry extracted from a Kafka message.");
                let update = MapLaneSelectUpdate::new(select_lane, key, value.clone());
                Some(MapLaneOp::inject(update))
            }
            _ => None,
        };
        Ok(handler.discard())
    }
}
