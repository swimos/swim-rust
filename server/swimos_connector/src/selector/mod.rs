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

mod base;
mod model;

use base::SelectorComponent;
pub use base::{
    AttrSelector, BasicSelector, ChainSelector, IdentitySelector, IndexSelector, MapLaneSelector,
    SlotSelector, ValueLaneSelector,
};
mod relay;
pub use model::*;
pub use relay::*;

#[cfg(test)]
mod tests;

pub trait InterpretableSelector: Sized {
    fn try_interp(descriptor: &RawSelectorDescriptor<'_>) -> Result<Option<Self>, BadSelector>;
}

impl InterpretableSelector for CNil {
    fn try_interp(_descriptor: &RawSelectorDescriptor<'_>) -> Result<Option<Self>, BadSelector> {
        Ok(None)
    }
}

impl<Head, Tail> InterpretableSelector for Coproduct<Head, Tail>
where
    Head: InterpretableSelector,
    Tail: InterpretableSelector,
{
    fn try_interp(descriptor: &RawSelectorDescriptor<'_>) -> Result<Option<Self>, BadSelector> {
        if let Some(tail) = Tail::try_interp(descriptor)? {
            Ok(Some(Coproduct::Inr(tail)))
        } else {
            Ok(Head::try_interp(descriptor)?.map(Coproduct::Inl))
        }
    }
}

use crate::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use crate::deser::Deferred;
use crate::error::InvalidLaneSpec;
use crate::BadSelector;
use crate::DeserializationError;
use frunk::coproduct::{CNil, Coproduct};
use frunk::{Coprod, HList};
use regex::Regex;
pub use relay::{
    parse_lane_selector, parse_map_selector, parse_node_selector, parse_value_selector,
};
use std::borrow::Cow;
use std::sync::OnceLock;
use swimos_model::Value;

/// Canonical selector for pub-sub type connectors.
pub type PubSubSelector = Coprod!(TopicSelector, KeySelector, PayloadSelector);
pub type PubSubSelectorArgs<'a> = HList!(Value, Deferred<'a>, Deferred<'a>);
/// Selector type for Value Lanes.
pub type PubSubValueLaneSelector = ValueLaneSelector<PubSubSelector>;
/// Selector type for Map Lanes.
pub type PubSubMapLaneSelector = MapLaneSelector<PubSubSelector, PubSubSelector>;

#[derive(Debug, PartialEq, Clone, Eq, Default)]
pub struct TopicSelector;

impl Selector<Value> for TopicSelector {
    fn select(&self, from: &mut Value) -> Result<Option<Value>, DeserializationError> {
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

impl Selector<Deferred<'_>> for KeySelector {
    fn select(&self, from: &mut Deferred<'_>) -> Result<Option<Value>, DeserializationError> {
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

    pub fn from_chain<I: IntoIterator<Item = BasicSelector>>(it: I) -> Self {
        PayloadSelector::new(ChainSelector::from(it))
    }
}

impl Selector<Deferred<'_>> for PayloadSelector {
    fn select(&self, from: &mut Deferred<'_>) -> Result<Option<Value>, DeserializationError> {
        let PayloadSelector(chain) = self;
        from.with(|val| ValueSelector::select_value(chain, val).cloned())
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

impl<'a> RawSelectorDescriptor<'a> {
    pub fn suggested_name(&self) -> Option<&'a str> {
        let RawSelectorDescriptor {
            part,
            index,
            components,
        } = self;
        if let Some(SelectorComponent { name, index, .. }) = components.last() {
            if index.is_none() {
                Some(*name)
            } else {
                None
            }
        } else if index.is_none() {
            let p = part.strip_prefix('$').unwrap_or(part);
            if !p.is_empty() {
                Some(p)
            } else {
                None
            }
        } else {
            None
        }
    }
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

impl<S> TryFrom<&IngressValueLaneSpec> for ValueLaneSelector<S>
where
    S: InterpretableSelector,
{
    type Error = InvalidLaneSpec;

    fn try_from(value: &IngressValueLaneSpec) -> Result<Self, Self::Error> {
        let IngressValueLaneSpec {
            name,
            selector,
            required,
        } = value;
        let parsed = RawSelectorDescriptor::try_from(selector.as_str())?;
        if let Some(lane_name) = name
            .as_ref()
            .cloned()
            .or_else(|| parsed.suggested_name().map(|s| s.to_owned()))
        {
            if let Some(selector) = S::try_interp(&parsed)? {
                Ok(ValueLaneSelector::new(lane_name, selector, *required))
            } else {
                Err(InvalidLaneSpec::Selector(BadSelector::InvalidRoot))
            }
        } else {
            Err(InvalidLaneSpec::NameCannotBeInferred)
        }
    }
}

impl InterpretableSelector for PayloadSelector {
    fn try_interp(descriptor: &RawSelectorDescriptor<'_>) -> Result<Option<Self>, BadSelector> {
        let RawSelectorDescriptor {
            part,
            index,
            components,
        } = descriptor;
        if *part == "$payload" {
            let selector = ChainSelector::new(*index, components);
            Ok(Some(PayloadSelector::new(selector)))
        } else {
            Ok(None)
        }
    }
}

impl InterpretableSelector for KeySelector {
    fn try_interp(descriptor: &RawSelectorDescriptor<'_>) -> Result<Option<Self>, BadSelector> {
        let RawSelectorDescriptor {
            part,
            index,
            components,
        } = descriptor;
        if *part == "$key" {
            let selector = ChainSelector::new(*index, components);
            Ok(Some(KeySelector::new(selector)))
        } else {
            Ok(None)
        }
    }
}

impl InterpretableSelector for TopicSelector {
    fn try_interp(descriptor: &RawSelectorDescriptor<'_>) -> Result<Option<Self>, BadSelector> {
        let RawSelectorDescriptor {
            part,
            index,
            components,
        } = descriptor;
        if *part == "$topic" {
            if index.is_none() && components.is_empty() {
                Ok(Some(TopicSelector))
            } else {
                Err(BadSelector::TopicWithComponent)
            }
        } else {
            Ok(None)
        }
    }
}

impl<K, V> TryFrom<&IngressMapLaneSpec> for MapLaneSelector<K, V>
where
    K: InterpretableSelector,
    V: InterpretableSelector,
{
    type Error = InvalidLaneSpec;

    fn try_from(value: &IngressMapLaneSpec) -> Result<Self, Self::Error> {
        let IngressMapLaneSpec {
            name,
            key_selector,
            value_selector,
            remove_when_no_value,
            required,
        } = value;
        let key = K::try_interp(&RawSelectorDescriptor::try_from(key_selector.as_str())?)?;
        let value = V::try_interp(&RawSelectorDescriptor::try_from(value_selector.as_str())?)?;
        match (key, value) {
            (Some(key), Some(value)) => Ok(MapLaneSelector::new(
                name.clone(),
                key,
                value,
                *required,
                *remove_when_no_value,
            )),
            _ => Err(InvalidLaneSpec::Selector(BadSelector::InvalidRoot)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyOrValue {
    Key,
    Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldExtractor {
    part: KeyOrValue,
    selector: ChainSelector,
}

impl FieldExtractor {
    pub fn new(part: KeyOrValue, selector: ChainSelector) -> Self {
        FieldExtractor { part, selector }
    }

    pub fn select<'a>(&self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a Value> {
        let FieldExtractor { part, selector } = self;
        match part {
            KeyOrValue::Key => key.and_then(|k| selector.select_value(k)),
            KeyOrValue::Value => selector.select_value(value),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicExtractor {
    Fixed(String),
    Selector(FieldExtractor),
}

impl TopicExtractor {
    pub fn select<'a>(&'a self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a str> {
        match self {
            TopicExtractor::Fixed(s) => Some(s.as_str()),
            TopicExtractor::Selector(sel) => match sel.select(key, value) {
                Some(Value::Text(s)) => Some(s.as_str()),
                _ => None,
            },
        }
    }
}

pub enum TopicExtractorSpec<'a> {
    Fixed(Cow<'a, str>),
    Selector(FieldExtractorSpec<'a>),
}

impl<'a> From<FieldExtractorSpec<'a>> for FieldExtractor {
    fn from(value: FieldExtractorSpec<'a>) -> Self {
        let FieldExtractorSpec {
            part,
            index,
            components,
        } = value;
        FieldExtractor::new(part, ChainSelector::new(index, &components))
    }
}

pub struct FieldExtractorSpec<'a> {
    part: KeyOrValue,
    index: Option<usize>,
    components: Vec<SelectorComponent<'a>>,
}

impl<'a> TryFrom<RawSelectorDescriptor<'a>> for FieldExtractorSpec<'a> {
    type Error = BadSelector;

    fn try_from(value: RawSelectorDescriptor<'a>) -> Result<Self, Self::Error> {
        let RawSelectorDescriptor {
            part,
            index,
            components,
        } = value;
        match part {
            "$key" => Ok(FieldExtractorSpec {
                part: KeyOrValue::Key,
                index,
                components,
            }),
            "$value" => Ok(FieldExtractorSpec {
                part: KeyOrValue::Value,
                index,
                components,
            }),
            _ => Err(BadSelector::InvalidRoot),
        }
    }
}

/// Attempt to parse a field selector from a string.
pub fn parse_field_selector(descriptor: &str) -> Result<FieldExtractorSpec<'_>, BadSelector> {
    RawSelectorDescriptor::try_from(descriptor)?.try_into()
}
