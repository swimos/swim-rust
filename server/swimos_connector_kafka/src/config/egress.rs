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
use swimos_api::address::Address;
use swimos_form::Form;

use super::{DataFormat, KafkaLogLevel};

/// Configuration parameters for the Kafka egress connector.
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
    /// Descriptors for the value lanes of the connector agent and how to extract messages
    /// from them to send to the egress sink.
    pub value_lanes: Vec<EgressLaneSpec>,
    /// Descriptors for the map lanes of the connector agent and how to extract messages
    /// from them to send to the egress sink.
    pub map_lanes: Vec<EgressLaneSpec>,
    /// Descriptors for the value downlinks (to remote lanes) of the connector agent and how to extract messages
    /// from the received events to send to the egress sink.
    pub value_downlinks: Vec<EgressDownlinkSpec>,
    /// Descriptors for the map downlinks (to remote lanes) of the connector agent and how to extract messages
    /// from the received events to send to the egress sink.
    pub map_downlinks: Vec<EgressDownlinkSpec>,
    /// Time to wait before retrying a message if the connector is initially busy, in milliseconds.
    pub retry_timeout_ms: u64,
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

#[derive(Clone, Debug, Default, Form, PartialEq, Eq)]
pub struct ExtractionSpec {
    /// Chooses the topic for a value set to this lane.
    pub topic_specifier: TopicSpecifier,
    /// A selector for a key to associate with the message. If not specified, the key will be empty.
    pub key_selector: Option<String>,
    /// A selector for for the payload of the message. If not specified, the entire value will be used.
    pub payload_selector: Option<String>,
}

/// Specification of a lane for the connector agent.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "LaneSpec")]
pub struct EgressLaneSpec {
    /// A name to use for the lane.
    pub name: String,
    /// Specification for extracting the Kafka message from the lane events.
    pub extractor: ExtractionSpec,
}

/// Target lane for a remote downlink.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct DownlinkAddress {
    pub host: Option<String>,
    pub node: String,
    pub lane: String,
}

impl From<&DownlinkAddress> for Address<String> {
    fn from(value: &DownlinkAddress) -> Self {
        let DownlinkAddress { host, node, lane } = value;
        Address::new(host.clone(), node.clone(), lane.clone())
    }
}

impl DownlinkAddress {
    pub fn borrow_as_addr(&self) -> Address<&str> {
        let DownlinkAddress { host, node, lane } = self;
        Address {
            host: host.as_ref().map(|s| s.as_str()),
            node,
            lane,
        }
    }
}

/// Specification of a downlink (to a remote lane) for the connector agent.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "DownlinkSpec")]
pub struct EgressDownlinkSpec {
    /// The address of the lane to link from.
    pub address: DownlinkAddress,
    /// Specification for extracting the Kafka message from the downlink events.
    pub extractor: ExtractionSpec,
}
