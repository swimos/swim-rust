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
use swimos_connector::{
    selector::{
        parse_field_selector, FieldExtractor, FieldExtractorSpec, TopicExtractor,
        TopicExtractorSpec,
    },
    InvalidExtractor, InvalidExtractors, MessageSource,
};
use swimos_model::Value;

use crate::config::{
    EgressDownlinkSpec, EgressLaneSpec, ExtractionSpec, KafkaEgressConfiguration, TopicSpecifier,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSelector {
    topic: TopicExtractor,
    key: Option<FieldExtractor>,
    payload: Option<FieldExtractor>,
}

impl MessageSelector {
    pub fn new(
        topic: TopicExtractor,
        key: Option<FieldExtractor>,
        payload: Option<FieldExtractor>,
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

impl<'a> From<MessageSelectorSpec<'a>> for MessageSelector {
    fn from(value: MessageSelectorSpec<'a>) -> Self {
        let MessageSelectorSpec {
            topic,
            key,
            payload,
        } = value;
        let topic = match topic {
            TopicExtractorSpec::Fixed(s) => TopicExtractor::Fixed(s.to_string()),
            TopicExtractorSpec::Selector(spec) => TopicExtractor::Selector(spec.into()),
        };
        MessageSelector::new(topic, key.map(Into::into), payload.map(Into::into))
    }
}

struct MessageSelectorSpec<'a> {
    topic: TopicExtractorSpec<'a>,
    key: Option<FieldExtractorSpec<'a>>,
    payload: Option<FieldExtractorSpec<'a>>,
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
                    TopicExtractorSpec::Fixed(Cow::Owned(top.to_string()))
                } else {
                    return Err(InvalidExtractor::NoTopic);
                }
            }
            TopicSpecifier::Specified(t) => TopicExtractorSpec::Fixed(Cow::Borrowed(t.as_str())),
            TopicSpecifier::Selector(s) => {
                TopicExtractorSpec::Selector(parse_field_selector(s.as_str())?)
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
    pub(crate) value_lanes: HashMap<String, MessageSelector>,
    pub(crate) map_lanes: HashMap<String, MessageSelector>,
    pub(crate) event_downlinks: HashMap<Address<String>, MessageSelector>,
    pub(crate) map_event_downlinks: HashMap<Address<String>, MessageSelector>,
}

impl MessageSelectors {
    pub fn select_source(&self, source: MessageSource<'_>) -> Option<&MessageSelector> {
        match source {
            MessageSource::Lane(name) => self
                .value_lanes
                .get(name)
                .or_else(|| self.map_lanes.get(name)),
            MessageSource::Downlink(addr) => self
                .event_downlinks
                .get(addr)
                .or_else(|| self.map_event_downlinks.get(addr)),
        }
    }
}

impl TryFrom<&KafkaEgressConfiguration> for MessageSelectors {
    type Error = InvalidExtractors;

    fn try_from(value: &KafkaEgressConfiguration) -> Result<Self, Self::Error> {
        let KafkaEgressConfiguration {
            fixed_topic,
            value_lanes,
            map_lanes,
            event_downlinks,
            map_event_downlinks,
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
        for EgressDownlinkSpec { address, extractor } in event_downlinks {
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
        for EgressDownlinkSpec { address, extractor } in map_event_downlinks {
            if value_dl_selectors.contains_key(address) {
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
        Ok(MessageSelectors {
            value_lanes: value_lane_selectors,
            map_lanes: map_lane_selectors,
            event_downlinks: value_dl_selectors,
            map_event_downlinks: map_dl_selectors,
        })
    }
}
