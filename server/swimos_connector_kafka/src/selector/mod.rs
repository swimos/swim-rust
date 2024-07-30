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

use std::{fmt::Debug, num::ParseIntError, sync::OnceLock};

use frunk::Coprod;
use regex::Regex;
use swimos_agent::{
    agent_lifecycle::ConnectorContext,
    event_handler::{Discard, HandlerActionExt},
};
use swimos_connector::{ConnectorAgent, MapLaneSelectorFn, ValueLaneSelectorFn};
use swimos_model::{Attr, Item, Text, Value};
use thiserror::Error;

use crate::{
    config::{MapLaneSpec, ValueLaneSpec},
    connector::MessagePart,
    error::{DeserializationError, LaneSelectorError},
};
use swimos_agent::lanes::{MapLaneSelectRemove, MapLaneSelectUpdate, ValueLaneSelectSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageField {
    Key,
    Value,
    Topic,
}

impl From<MessagePart> for MessageField {
    fn from(value: MessagePart) -> Self {
        match value {
            MessagePart::Key => MessageField::Key,
            MessagePart::Payload => MessageField::Value,
        }
    }
}

pub trait Deferred {
    fn get(&mut self) -> Result<&Value, DeserializationError>;
}

pub trait Selector: Debug {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value>;
}

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
            *inner = Some(f()?);
            Ok(inner.as_ref().expect("Should be defined."))
        }
    }
}

#[derive(Debug)]
pub enum LaneSelector {
    Topic,
    Key(BoxSelector),
    Payload(BoxSelector),
}

impl<'a> From<SelectorDescriptor<'a>> for LaneSelector {
    fn from(value: SelectorDescriptor<'a>) -> Self {
        match (value.field(), value.selector()) {
            (MessageField::Key, Some(selector)) => LaneSelector::Key(Box::new(selector)),
            (MessageField::Value, Some(selector)) => LaneSelector::Payload(Box::new(selector)),
            _ => LaneSelector::Topic,
        }
    }
}

impl LaneSelector {
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

#[derive(Debug, Clone, Copy, Default)]
pub struct IdentitySelector;

impl Selector for IdentitySelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        Some(value)
    }
}

#[derive(Debug, Clone)]
pub struct AttrSelector {
    select_name: String,
}

impl AttrSelector {
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

#[derive(Debug, Clone)]
pub struct SlotSelector {
    select_key: Value,
}

impl SlotSelector {
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

#[derive(Debug, Clone, Copy)]
pub struct IndexSelector {
    index: usize,
}

impl IndexSelector {
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct ChainSelector(Vec<BasicSelector>);

impl ChainSelector {
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

pub type BoxSelector = Box<dyn Selector + Send + Sync + 'static>;

impl Selector for BoxSelector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        (**self).select(value)
    }
}

static INIT_REGEX: OnceLock<Regex> = OnceLock::new();
static FIELD_REGEX: OnceLock<Regex> = OnceLock::new();

fn init_regex() -> &'static Regex {
    INIT_REGEX.get_or_init(|| create_init_regex().expect("Invalid regex."))
}

fn field_regex() -> &'static Regex {
    FIELD_REGEX.get_or_init(|| create_field_regex().expect("Invalid regex."))
}

fn create_init_regex() -> Result<Regex, regex::Error> {
    Regex::new("\\A(\\$(?:key|value|topic))(?:\\[(\\d+)])?\\z")
}

fn create_field_regex() -> Result<Regex, regex::Error> {
    Regex::new("\\A(\\@?(?:\\w+))(?:\\[(\\d+)])?\\z")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectorComponent<'a> {
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
pub enum SelectorDescriptor<'a> {
    Part {
        part: MessagePart,
        index: Option<usize>,
        components: Vec<SelectorComponent<'a>>,
    },
    Topic,
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
                        MessagePart::Payload => "value",
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

#[derive(Clone, Copy, Error, Debug, PartialEq, Eq)]
pub enum BadSelector {
    #[error("Selector strings cannot be empty.")]
    EmptySelector,
    #[error("Selector components cannot be empty.")]
    EmptyComponent,
    #[error("Invalid root selector (must be one of '$key' or '$value' with an optional index or '$topic').")]
    InvalidRoot,
    #[error(
        "Invalid component selector (must be an attribute or slot name with an optional index)."
    )]
    InvalidComponent,
    #[error("An index specified was not a valid usize.")]
    IndexOutOfRange,
    #[error("The topic does not have components.")]
    TopicWithComponent,
}

impl From<ParseIntError> for BadSelector {
    fn from(_value: ParseIntError) -> Self {
        BadSelector::IndexOutOfRange
    }
}

pub fn parse_selector(descriptor: &str) -> Result<SelectorDescriptor<'_>, BadSelector> {
    if descriptor.is_empty() {
        return Err(BadSelector::EmptySelector);
    }
    let mut it = descriptor.split('.');
    let (field, index) = match it.next() {
        Some(root) if !root.is_empty() => {
            if let Some(captures) = init_regex().captures(root) {
                let field = match captures.get(1) {
                    Some(kind) if kind.as_str() == "$key" => MessageField::Key,
                    Some(kind) if kind.as_str() == "$value" => MessageField::Value,
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
        MessageField::Value => MessagePart::Payload,
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

#[derive(Debug)]
pub struct ValueLaneSelector {
    name: String,
    selector: LaneSelector,
    required: bool,
}

#[derive(Debug)]
pub struct MapLaneSelector {
    name: String,
    key_selector: LaneSelector,
    value_selector: LaneSelector,
    required: bool,
    remove_when_no_value: bool,
}

#[derive(Clone, Copy, Debug, Error)]
pub enum InvalidLaneSpec {
    #[error(transparent)]
    Selector(#[from] BadSelector),
    #[error("No name provided and it cannot be inferred from the selector.")]
    NameCannotBeInferred,
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
            Ok(ValueLaneSelector {
                name: lane_name,
                selector: parsed.into(),
                required: *required,
            })
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
        let key = LaneSelector::from(parse_selector(key_selector.as_str())?);
        let value = LaneSelector::from(parse_selector(value_selector.as_str())?);
        Ok(MapLaneSelector {
            name: name.clone(),
            key_selector: key,
            value_selector: value,
            required: *required,
            remove_when_no_value: *remove_when_no_value,
        })
    }
}

pub type GenericValueLaneSet =
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
                let select_lane = ValueLaneSelectorFn::new(name.clone());
                Some(ValueLaneSelectSet::new(select_lane, value.clone()))
            }
            None => {
                if *required {
                    return Err(LaneSelectorError::MissingRequiredField(name.clone()));
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
        let context: ConnectorContext<ConnectorAgent> = Default::default();
        let maybe_key: Option<Value> = key_selector.select(topic, key, value)?.cloned();
        let maybe_value = value_selector.select(topic, key, value)?;
        let select_lane = MapLaneSelectorFn::new(name.clone());
        let handler: Option<MapLaneOp> = match (maybe_key, maybe_value) {
            (None, _) if *required => {
                return Err(LaneSelectorError::MissingRequiredField(name.clone()))
            }
            (Some(key), None) if *remove_when_no_value => {
                Some(MapLaneOp::inject(context.remove(select_lane, key)))
            }
            (Some(key), Some(value)) => Some(MapLaneOp::inject(context.update(
                select_lane,
                key,
                value.clone(),
            ))),
            _ => None,
        };
        Ok(handler.discard())
    }
}
