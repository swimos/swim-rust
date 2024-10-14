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

use crate::selector::{SelectHandler, Selector, SelectorError, ValueSelector};
use crate::{ConnectorAgent, MapLaneSelectorFn, ValueLaneSelectorFn};
use frunk::Coprod;
use swimos_agent::event_handler::{Discard, HandlerActionExt};
use swimos_agent::lanes::{MapLaneSelectRemove, MapLaneSelectUpdate, ValueLaneSelectSet};
use swimos_model::{Attr, Item, Text, Value};
use tracing::{error, trace};

type MapLaneUpdate = MapLaneSelectUpdate<ConnectorAgent, Value, Value, MapLaneSelectorFn>;
type MapLaneRemove = MapLaneSelectRemove<ConnectorAgent, Value, Value, MapLaneSelectorFn>;
type MapLaneOp = Coprod!(MapLaneUpdate, MapLaneRemove);
pub type GenericMapLaneOp = Discard<Option<MapLaneOp>>;
pub type GenericValueLaneSet =
    Discard<Option<ValueLaneSelectSet<ConnectorAgent, Value, ValueLaneSelectorFn>>>;

// /// A lazy loader for a component of a messages. This ensures that deserializers are only run if a
// /// selector refers to a component.
// pub trait Deferred {
//     /// Get the deserialized component (computing it on the first call).
//     fn get(&mut self) -> Result<&Value, DeserializationError>;
// }

#[derive(Debug, PartialEq)]
pub struct ValueLaneSelector<S> {
    name: String,
    selector: S,
    required: bool,
}

impl<S> Clone for ValueLaneSelector<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        ValueLaneSelector {
            name: self.name.clone(),
            selector: self.selector.clone(),
            required: self.required,
        }
    }
}

impl<S> ValueLaneSelector<S> {
    pub fn new(name: String, selector: S, required: bool) -> ValueLaneSelector<S> {
        ValueLaneSelector {
            name,
            selector,
            required,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn is_required(&self) -> bool {
        self.required
    }

    pub fn selector(&self) -> &S {
        &self.selector
    }

    pub fn into_selector(self) -> S {
        self.selector
    }

    pub fn try_map_selector<S2, E, F>(self, f: F) -> Result<ValueLaneSelector<S2>, E>
    where
        F: FnOnce(S) -> Result<S2, E>,
    {
        let ValueLaneSelector {
            name,
            selector,
            required,
        } = self;
        let transformed = f(selector)?;
        Ok(ValueLaneSelector {
            name,
            selector: transformed,
            required,
        })
    }
}

impl<'a, S, A> SelectHandler<'a, A> for ValueLaneSelector<S>
where
    S: Selector<'a, Arg = A>,
{
    type Handler = GenericValueLaneSet;

    fn select_handler(&self, args: &'a A) -> Result<Self::Handler, SelectorError> {
        let ValueLaneSelector {
            name,
            selector,
            required,
        } = self;

        let maybe_value = selector.select(args)?;
        let handler = match maybe_value {
            Some(value) => {
                trace!(name, value = %value, "Setting a value extracted from a message to a value lane.");
                let select_lane = ValueLaneSelectorFn::new(name.clone());
                Some(ValueLaneSelectSet::new(select_lane, value))
            }
            None => {
                if *required {
                    error!(name, "A message did not contain a required value.");
                    return Err(SelectorError::MissingRequiredLane(name.clone()));
                } else {
                    None
                }
            }
        };
        Ok(handler.discard())
    }
}

/// Trivial selector that chooses the entire input value.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentitySelector;

impl ValueSelector for IdentitySelector {
    fn select_value<'a>(&self, value: &'a Value) -> Option<&'a Value> {
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
    pub fn new(name: String) -> Self {
        AttrSelector { select_name: name }
    }
}

impl ValueSelector for AttrSelector {
    fn select_value<'a>(&self, value: &'a Value) -> Option<&'a Value> {
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

impl ValueSelector for SlotSelector {
    fn select_value<'a>(&self, value: &'a Value) -> Option<&'a Value> {
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

impl ValueSelector for IndexSelector {
    fn select_value<'a>(&self, value: &'a Value) -> Option<&'a Value> {
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

impl ValueSelector for BasicSelector {
    fn select_value<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        match self {
            BasicSelector::Attr(s) => ValueSelector::select_value(s, value),
            BasicSelector::Slot(s) => ValueSelector::select_value(s, value),
            BasicSelector::Index(s) => ValueSelector::select_value(s, value),
        }
    }
}

/// A selector that applies a sequence of simpler selectors, in order, passing the result of one selector to the next.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct ChainSelector(Vec<BasicSelector>);

impl<I> From<I> for ChainSelector
where
    I: IntoIterator<Item = BasicSelector>,
{
    fn from(value: I) -> Self {
        ChainSelector(value.into_iter().collect())
    }
}

impl ChainSelector {
    pub fn new(index: Option<usize>, components: &[SelectorComponent<'_>]) -> ChainSelector {
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
        ChainSelector::from(links)
    }
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

impl ValueSelector for ChainSelector {
    fn select_value<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let mut v = Some(value);
        let ChainSelector(selectors) = self;
        for s in selectors {
            let selected = if let Some(v) = v {
                ValueSelector::select_value(s, v)
            } else {
                break;
            };
            v = selected;
        }
        v
    }
}

/// A value lane selector generates event handlers from messages to update the state of a map lane.
#[derive(Debug)]
pub struct MapLaneSelector<K, V> {
    name: String,
    key_selector: K,
    value_selector: V,
    required: bool,
    remove_when_no_value: bool,
}

impl<K, V> Clone for MapLaneSelector<K, V>
where
    K: Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        MapLaneSelector {
            name: self.name.clone(),
            key_selector: self.key_selector.clone(),
            value_selector: self.value_selector.clone(),
            required: self.required,
            remove_when_no_value: self.remove_when_no_value,
        }
    }
}

impl<K, V> MapLaneSelector<K, V> {
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `key_selector` - Selects a component from the message for the map key.
    /// * `value_selector` - Selects a component from the message for the map value.
    /// * `required` - If this is required and the selectors do not return a result, an error will be generated.
    /// * `remove_when_no_value` - If a key is selected but no value is selected, the corresponding entry will be
    ///   removed from the map.
    pub fn new(
        name: String,
        key_selector: K,
        value_selector: V,
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_required(&self) -> bool {
        self.required
    }

    pub fn remove_when_no_value(&self) -> bool {
        self.remove_when_no_value
    }

    pub fn into_selectors(self) -> (K, V) {
        let MapLaneSelector {
            key_selector,
            value_selector,
            ..
        } = self;
        (key_selector, value_selector)
    }

    pub fn selectors(&self) -> (&K, &V) {
        let MapLaneSelector {
            key_selector,
            value_selector,
            ..
        } = self;
        (key_selector, value_selector)
    }

    pub fn try_map_selectors<K2, V2, E, F1, F2>(
        self,
        fk: F1,
        fv: F2,
    ) -> Result<MapLaneSelector<K2, V2>, E>
    where
        F1: FnOnce(K) -> Result<K2, E>,
        F2: FnOnce(V) -> Result<V2, E>,
    {
        let MapLaneSelector {
            name,
            key_selector,
            value_selector,
            required,
            remove_when_no_value,
        } = self;
        let key_transformed = fk(key_selector)?;
        let value_transformed = fv(value_selector)?;
        Ok(MapLaneSelector {
            name,
            key_selector: key_transformed,
            value_selector: value_transformed,
            required,
            remove_when_no_value,
        })
    }
}

impl<'a, K, V, A> SelectHandler<'a, A> for MapLaneSelector<K, V>
where
    K: Selector<'a, Arg = A>,
    V: Selector<'a, Arg = A>,
{
    type Handler = GenericMapLaneOp;

    fn select_handler(&self, from: &'a A) -> Result<Self::Handler, SelectorError> {
        let MapLaneSelector {
            name,
            key_selector,
            value_selector,
            required,
            remove_when_no_value,
        } = self;

        let maybe_key: Option<Value> = key_selector.select(from)?;
        let maybe_value = value_selector.select(from)?;
        let select_lane = MapLaneSelectorFn::new(name.clone());

        let handler: Option<MapLaneOp> = match (maybe_key, maybe_value) {
            (None, _) if *required => {
                error!(
                    name,
                    "A message did not contain a required map lane update/removal."
                );
                return Err(SelectorError::MissingRequiredLane(name.clone()));
            }
            (Some(key), None) if *remove_when_no_value => {
                trace!(name, key = %key, "Removing an entry from a map lane with a key extracted from a message.");
                let remove = MapLaneSelectRemove::new(select_lane, key);
                Some(MapLaneOp::inject(remove))
            }
            (Some(key), Some(value)) => {
                trace!(name, key = %key, value = %value, "Updating a map lane with an entry extracted from a message.");
                let update = MapLaneSelectUpdate::new(select_lane, key, value);
                Some(MapLaneOp::inject(update))
            }
            _ => None,
        };
        Ok(handler.discard())
    }
}
