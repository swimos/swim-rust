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

use crate::deserialization::{Deferred, DeserializationError};
use swimos_model::{Attr, Item, Text, Value};

pub trait Selector {
    fn select<'a>(&self, value: &'a Value) -> Option<&'a Value>;
}

/// A lane selector attempts to extract a value from a message to use as a new value for a value lane
/// or an update for a map lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueSelector {
    /// Select the topic from the message.
    Topic,
    /// Use a selector to select a subcomponent of the key of the message.
    Key(ChainSelector),
    /// Use a selector to select a subcomponent of the payload of the message.
    Payload(ChainSelector),
}

impl ValueSelector {
    /// Attempt to select a subcomponent from a message.
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
            ValueSelector::Topic => Some(topic),
            ValueSelector::Key(selector) => selector.select(key.get()?),
            ValueSelector::Payload(selector) => selector.select(payload.get()?),
        })
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
    pub fn new(name: String) -> Self {
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
