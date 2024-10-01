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

use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use swimos_api::address::Address;
use swimos_model::Value;

use crate::{
    config::{
        EgressDownlinkSpec, EgressLaneSpec, ExtractionSpec, KafkaEgressConfiguration,
        TopicSpecifier,
    },
    selector::make_chain_selector,
    BadSelector, InvalidExtractor, InvalidExtractors,
};

use super::{
    parse_raw_selector, ChainSelector, RawSelectorDescriptor, Selector, SelectorComponent,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSelector {
    topic: TopicSelector,
    key: Option<FieldSelector>,
    payload: Option<FieldSelector>,
}

impl MessageSelector {
    pub fn new(
        topic: TopicSelector,
        key: Option<FieldSelector>,
        payload: Option<FieldSelector>,
    ) -> Self {
        MessageSelector {
            topic,
            key,
            payload,
        }
    }

    pub fn select_topic<'a>(&'a self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a str> {
        let MessageSelector { topic, .. } = self;
        topic.select(key, value)
    }

    pub fn select_key<'a>(&self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a Value> {
        let MessageSelector { key: key_sel, .. } = self;
        match key_sel {
            Some(sel) => sel.select(key, value),
            None => None,
        }
    }

    pub fn select_payload<'a>(
        &self,
        key: Option<&'a Value>,
        value: &'a Value,
    ) -> Option<&'a Value> {
        let MessageSelector { payload, .. } = self;
        match payload {
            Some(sel) => sel.select(key, value),
            None => Some(value),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldSelector {
    part: KeyOrValue,
    selector: ChainSelector,
}

impl FieldSelector {
    pub fn new(part: KeyOrValue, selector: ChainSelector) -> Self {
        FieldSelector { part, selector }
    }

    pub fn select<'a>(&self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a Value> {
        let FieldSelector { part, selector } = self;
        match part {
            KeyOrValue::Key => key.and_then(|k| selector.select(k)),
            KeyOrValue::Value => selector.select(value),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyOrValue {
    Key,
    Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicSelector {
    Fixed(String),
    Selector(FieldSelector),
}

impl TopicSelector {
    fn select<'a>(&'a self, key: Option<&'a Value>, value: &'a Value) -> Option<&'a str> {
        match self {
            TopicSelector::Fixed(s) => Some(s.as_str()),
            TopicSelector::Selector(sel) => match sel.select(key, value) {
                Some(Value::Text(s)) => Some(s.as_str()),
                _ => None,
            },
        }
    }
}

enum TopicSelectorSpec<'a> {
    Fixed(Cow<'a, str>),
    Selector(FieldSelectorSpec<'a>),
}

impl<'a> From<FieldSelectorSpec<'a>> for FieldSelector {
    fn from(value: FieldSelectorSpec<'a>) -> Self {
        let FieldSelectorSpec {
            part,
            index,
            components,
        } = value;
        FieldSelector::new(part, make_chain_selector(index, &components))
    }
}

impl<'a> From<MessageSelectorSpec<'a>> for MessageSelector {
    fn from(value: MessageSelectorSpec<'a>) -> Self {
        let MessageSelectorSpec {
            topic,
            key,
            payload,
        } = value;
        let topic = match topic {
            TopicSelectorSpec::Fixed(s) => TopicSelector::Fixed(s.to_string()),
            TopicSelectorSpec::Selector(spec) => TopicSelector::Selector(spec.into()),
        };
        MessageSelector::new(topic, key.map(Into::into), payload.map(Into::into))
    }
}

struct MessageSelectorSpec<'a> {
    topic: TopicSelectorSpec<'a>,
    key: Option<FieldSelectorSpec<'a>>,
    payload: Option<FieldSelectorSpec<'a>>,
}

struct FieldSelectorSpec<'a> {
    part: KeyOrValue,
    index: Option<usize>,
    components: Vec<SelectorComponent<'a>>,
}

impl<'a> TryFrom<RawSelectorDescriptor<'a>> for FieldSelectorSpec<'a> {
    type Error = BadSelector;

    fn try_from(value: RawSelectorDescriptor<'a>) -> Result<Self, Self::Error> {
        let RawSelectorDescriptor {
            part,
            index,
            components,
        } = value;
        match part {
            "$key" => Ok(FieldSelectorSpec {
                part: KeyOrValue::Key,
                index,
                components,
            }),
            "$value" => Ok(FieldSelectorSpec {
                part: KeyOrValue::Value,
                index,
                components,
            }),
            _ => Err(BadSelector::InvalidRoot),
        }
    }
}

/// Attempt to parse a field selector from a string.
fn parse_field_selector(descriptor: &str) -> Result<FieldSelectorSpec<'_>, BadSelector> {
    parse_raw_selector(descriptor)?.try_into()
}

impl MessageSelector {
    pub fn try_from_ext_spec(
        spec: &ExtractionSpec,
        fixed_topic: Option<&str>,
    ) -> Result<Self, InvalidExtractor> {
        let spec = MessageSelectorSpec::try_from_ext_spec(spec, fixed_topic)?;
        Ok(spec.into())
    }
}

impl<'a> MessageSelectorSpec<'a> {
    fn try_from_ext_spec(
        spec: &'a ExtractionSpec,
        fixed_topic: Option<&str>,
    ) -> Result<Self, InvalidExtractor> {
        let ExtractionSpec {
            topic_specifier,
            key_selector,
            payload_selector,
        } = spec;
        let topic = match topic_specifier {
            TopicSpecifier::Fixed => {
                if let Some(top) = fixed_topic {
                    TopicSelectorSpec::Fixed(Cow::Owned(top.to_string()))
                } else {
                    return Err(InvalidExtractor::NoTopic);
                }
            }
            TopicSpecifier::Specified(t) => TopicSelectorSpec::Fixed(Cow::Borrowed(t.as_str())),
            TopicSpecifier::Selector(s) => {
                TopicSelectorSpec::Selector(parse_field_selector(s.as_str())?)
            }
        };
        let key = key_selector
            .as_ref()
            .map(|s| parse_field_selector(s))
            .transpose()?;
        let payload = payload_selector
            .as_ref()
            .map(|s| parse_field_selector(s))
            .transpose()?;
        Ok(MessageSelectorSpec {
            topic,
            key,
            payload,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSelectors {
    value_lanes: HashMap<String, MessageSelector>,
    map_lanes: HashMap<String, MessageSelector>,
    value_downlinks: HashMap<Address<String>, MessageSelector>,
    map_downlinks: HashMap<Address<String>, MessageSelector>,
    total_lanes: u32,
}

impl MessageSelectors {
    pub fn value_lanes(&self) -> &HashMap<String, MessageSelector> {
        &self.value_lanes
    }

    pub fn map_lanes(&self) -> &HashMap<String, MessageSelector> {
        &self.map_lanes
    }

    pub fn value_downlinks(&self) -> &HashMap<Address<String>, MessageSelector> {
        &self.value_downlinks
    }

    pub fn map_downlinks(&self) -> &HashMap<Address<String>, MessageSelector> {
        &self.map_downlinks
    }

    pub fn total_lanes(&self) -> u32 {
        self.total_lanes
    }
}

impl TryFrom<&KafkaEgressConfiguration> for MessageSelectors {
    type Error = InvalidExtractors;

    fn try_from(value: &KafkaEgressConfiguration) -> Result<Self, Self::Error> {
        let KafkaEgressConfiguration {
            fixed_topic,
            value_lanes,
            map_lanes,
            value_downlinks,
            map_downlinks,
            ..
        } = value;
        let top = fixed_topic.as_ref().map(|s| s.as_str());
        let mut value_lane_selectors = HashMap::new();
        let mut map_lane_selectors = HashMap::new();
        let mut value_dl_selectors = HashMap::new();
        let mut map_dl_selectors = HashMap::new();
        for EgressLaneSpec { name, extractor } in value_lanes {
            match value_lane_selectors.entry(name.clone()) {
                Entry::Occupied(entry) => {
                    let (name, _) = entry.remove_entry();
                    return Err(InvalidExtractors::NameCollision(name));
                }
                Entry::Vacant(entry) => {
                    let selector = MessageSelector::try_from_ext_spec(extractor, top)?;
                    entry.insert(selector);
                }
            }
        }
        for EgressLaneSpec { name, extractor } in map_lanes {
            if value_lane_selectors.contains_key(name) {
                return Err(InvalidExtractors::NameCollision(name.clone()));
            }
            match map_lane_selectors.entry(name.clone()) {
                Entry::Occupied(entry) => {
                    let (name, _) = entry.remove_entry();
                    return Err(InvalidExtractors::NameCollision(name));
                }
                Entry::Vacant(entry) => {
                    let selector = MessageSelector::try_from_ext_spec(extractor, top)?;
                    entry.insert(selector);
                }
            }
        }
        for EgressDownlinkSpec { address, extractor } in value_downlinks {
            let address = Address::<String>::from(address);
            match value_dl_selectors.entry(address.clone()) {
                Entry::Occupied(entry) => {
                    let (address, _) = entry.remove_entry();
                    return Err(InvalidExtractors::AddressCollision(address));
                }
                Entry::Vacant(entry) => {
                    let selector = MessageSelector::try_from_ext_spec(extractor, top)?;
                    entry.insert(selector);
                }
            }
        }
        for EgressDownlinkSpec { address, extractor } in map_downlinks {
            let address = Address::<String>::from(address);
            if value_dl_selectors.contains_key(&address) {
                return Err(InvalidExtractors::AddressCollision(address.clone()));
            }
            match map_dl_selectors.entry(address.clone()) {
                Entry::Occupied(entry) => {
                    let (address, _) = entry.remove_entry();
                    return Err(InvalidExtractors::AddressCollision(address));
                }
                Entry::Vacant(entry) => {
                    let selector = MessageSelector::try_from_ext_spec(extractor, top)?;
                    entry.insert(selector);
                }
            }
        }
        let total = value_lanes.len() + map_lanes.len();
        let total_lanes = if let Ok(n) = u32::try_from(total) {
            if n == u32::MAX {
                return Err(InvalidExtractors::TooManyLanes(total));
            }
            n
        } else {
            return Err(InvalidExtractors::TooManyLanes(total));
        };
        Ok(MessageSelectors {
            value_lanes: value_lane_selectors,
            map_lanes: map_lane_selectors,
            value_downlinks: value_dl_selectors,
            map_downlinks: map_dl_selectors,
            total_lanes,
        })
    }
}
