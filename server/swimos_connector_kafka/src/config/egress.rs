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

use std::collections::HashMap;

use swimos_form::Form;

use super::{DataFormat, KafkaLogLevel};

/// Configuration parameters for the Kafka connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "kafka")]
pub struct KafkaEgressConfiguration {
    /// Properties to configure the Kafka consumer.
    pub properties: HashMap<String, String>,
    /// Log level for the Kafka producer.
    pub log_level: KafkaLogLevel,
    /// Serialization format to use when writing keys.
    pub key_serializer: DataFormat,
    /// Serialization format to use when writing payloads.
    pub payload_serializer: DataFormat,
    /// The topic to publish to the record does not specify it.
    pub fixed_topic: Option<String>,
    pub value_lanes: Vec<EgressValueLaneSpec>,
    pub map_lanes: Vec<EgressMapLaneSpec>,
    pub value_downlinks: Vec<ValueDownlinkSpec>,
    pub map_downlinks: Vec<MapDownlinkSpec>,
}

/// Instructions to derive the topic for a Kafka message from a value posted to a lane.
#[derive(Clone, Form, Debug, Default, PartialEq, Eq)]
pub enum TopicSpecifier {
    /// Use the fixed topic specified by the parent configuration.
    #[default]
    Fixed,
    /// Use a single, specified topic.
    Specified(#[form(header_body)] String),
    /// Use a selector to choose the topic from the value.
    Selector(#[form(header_body)] String),
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "ValueLaneSpec")]
pub struct EgressValueLaneSpec {
    /// A name to use for the lane.
    pub name: String,
    /// Chooses the topic for a value set to this lane.
    pub topic: TopicSpecifier,
    /// A selector for a key to associate with the message. If not specified, the key will be empty.
    pub key_selector: Option<String>,
    /// A selector for for the payload of the message. If not specified, the entire value will be used.
    pub payload_selector: Option<String>,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "MapLaneSpec")]
pub struct EgressMapLaneSpec {
    /// A name to use for the lane.
    pub name: String,
    /// Chooses the topic for a value set to this lane.
    pub topic: TopicSpecifier,
    /// A selector for a key to associate with the message. If not specified, the key will be empty.
    pub key_selector: Option<String>,
    /// A selector for for the payload of the message. If not specified, the entire value will be used.
    pub payload_selector: Option<String>,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct DownlinkAddress {
    pub host: Option<String>,
    pub node: String,
    pub lane: String,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct ValueDownlinkSpec {
    /// The address of the lane to link from.
    pub address: DownlinkAddress,
    /// Chooses the topic for a value set to this lane.
    pub topic: TopicSpecifier,
    /// A selector for a key to associate with the message. If not specified, the key will be empty.
    pub key_selector: Option<String>,
    /// A selector for for the payload of the message. If not specified, the entire value will be used.
    pub payload_selector: Option<String>,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct MapDownlinkSpec {
    /// The address of the lane to link from.
    pub address: DownlinkAddress,
    /// Chooses the topic for a value set to this lane.
    pub topic: TopicSpecifier,
    /// A selector for a key to associate with the message. If not specified, the key will be empty.
    pub key_selector: Option<String>,
    /// A selector for for the payload of the message. If not specified, the entire value will be used.
    pub payload_selector: Option<String>,
}
