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

use crate::{
    config::{ExtractionSpec, TopicSpecifier},
    EgressDownlinkSpec, EgressLaneSpec, MqttEgressConfiguration,
};

#[cfg(test)]
mod tests;

/// Extracts MQTT messages from an Swim lane/downlink event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageExtractor {
    topic: TopicExtractor,
    payload: Option<FieldExtractor>,
}

impl MessageExtractor {
    pub fn new(topic: TopicExtractor, payload: Option<FieldExtractor>) -> Self {
        MessageExtractor { topic, payload }
    }

    /// Attempt to extract the MQTT topic from the event.
    ///
    /// # Arguments
    /// * `key` - The map lane/downlink key (absent if not a map event).
    /// * `value` - The value associated with the event.
    pub fn extract_topic<'a>(
        &'a self,
        key: Option<&'a Value>,
        value: &'a Value,
    ) -> Option<&'a str> {
        let MessageExtractor { topic, .. } = self;
        topic.select(key, value)
    }

    /// Attempt to extract the MQTT payload from the event.
    ///
    /// # Arguments
    /// * `key` - The map lane/downlink key (absent if not a map event).
    /// * `value` - The value associated with the event.
    pub fn extract_payload<'a>(
        &self,
        key: Option<&'a Value>,
        value: &'a Value,
    ) -> Option<&'a Value> {
        let MessageExtractor { payload, .. } = self;
        match payload {
            Some(sel) => sel.select(key, value),
            None => Some(value),
        }
    }
}

/// Container for message extractors for all lanes and downlinks associated with an MQTT egress
/// agent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageExtractors {
    value_lanes: HashMap<String, MessageExtractor>,
    map_lanes: HashMap<String, MessageExtractor>,
    value_downlinks: HashMap<Address<String>, MessageExtractor>,
    map_downlinks: HashMap<Address<String>, MessageExtractor>,
}

impl MessageExtractors {
    /// Get the extractor for the specified source.
    ///
    /// # Arguments
    /// * `source` - The lane or downlink that produced an event.
    pub fn select_source(&self, source: MessageSource<'_>) -> Option<&MessageExtractor> {
        match source {
            MessageSource::Lane(name) => self
                .value_lanes
                .get(name)
                .or_else(|| self.map_lanes.get(name)),
            MessageSource::Downlink(addr) => self
                .value_downlinks
                .get(addr)
                .or_else(|| self.map_downlinks.get(addr)),
        }
    }
}

impl<'a> From<MessageExtractorSpec<'a>> for MessageExtractor {
    fn from(value: MessageExtractorSpec<'a>) -> Self {
        let MessageExtractorSpec { topic, payload } = value;
        let topic = match topic {
            TopicExtractorSpec::Fixed(s) => TopicExtractor::Fixed(s.to_string()),
            TopicExtractorSpec::Selector(spec) => TopicExtractor::Selector(spec.into()),
        };
        MessageExtractor::new(topic, payload.map(Into::into))
    }
}

struct MessageExtractorSpec<'a> {
    topic: TopicExtractorSpec<'a>,
    payload: Option<FieldExtractorSpec<'a>>,
}

impl MessageExtractor {
    pub fn try_from_ext_spec(
        spec: &ExtractionSpec,
        fixed_topic: Option<&str>,
    ) -> Result<Self, InvalidExtractor> {
        let spec = MessageExtractorSpec::try_from_ext_spec(spec, fixed_topic)?;
        Ok(spec.into())
    }
}

impl<'a> MessageExtractorSpec<'a> {
    fn try_from_ext_spec(
        spec: &'a ExtractionSpec,
        fixed_topic: Option<&str>,
    ) -> Result<Self, InvalidExtractor> {
        let ExtractionSpec {
            topic_specifier,
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
        let payload = payload_selector
            .as_ref()
            .map(|s| parse_field_selector(s))
            .transpose()?;
        Ok(MessageExtractorSpec { topic, payload })
    }
}

impl TryFrom<&MqttEgressConfiguration> for MessageExtractors {
    type Error = InvalidExtractors;

    fn try_from(value: &MqttEgressConfiguration) -> Result<Self, Self::Error> {
        let MqttEgressConfiguration {
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
                    let selector = MessageExtractor::try_from_ext_spec(extractor, top)?;
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
                    let selector = MessageExtractor::try_from_ext_spec(extractor, top)?;
                    entry.insert(selector);
                }
            }
        }
        for EgressDownlinkSpec { address, extractor } in value_downlinks {
            match value_dl_selectors.entry(address.clone()) {
                Entry::Occupied(entry) => {
                    let (address, _) = entry.remove_entry();
                    return Err(InvalidExtractors::AddressCollision(address));
                }
                Entry::Vacant(entry) => {
                    let selector = MessageExtractor::try_from_ext_spec(extractor, top)?;
                    entry.insert(selector);
                }
            }
        }
        for EgressDownlinkSpec { address, extractor } in map_downlinks {
            if value_dl_selectors.contains_key(address) {
                return Err(InvalidExtractors::AddressCollision(address.clone()));
            }
            match map_dl_selectors.entry(address.clone()) {
                Entry::Occupied(entry) => {
                    let (address, _) = entry.remove_entry();
                    return Err(InvalidExtractors::AddressCollision(address));
                }
                Entry::Vacant(entry) => {
                    let selector = MessageExtractor::try_from_ext_spec(extractor, top)?;
                    entry.insert(selector);
                }
            }
        }
        Ok(MessageExtractors {
            value_lanes: value_lane_selectors,
            map_lanes: map_lane_selectors,
            value_downlinks: value_dl_selectors,
            map_downlinks: map_dl_selectors,
        })
    }
}
