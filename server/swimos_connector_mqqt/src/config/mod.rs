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

use swimos_api::address::Address;
use swimos_connector::config::{
    format::DataFormat, IngressMapLaneSpec, IngressValueLaneSpec, RelaySpecification,
};
use swimos_form::Form;

/// Configuration parameters for the MQTT ingress connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "mqtt")]
pub struct MqttIngressConfiguration {
    /// The MQTT connection Url.
    pub url: String,
    /// Specifications for the value lanes to define for the connector. This includes a pattern to define a selector
    /// that will pick out values to set to that lane, from a MQTT message.
    pub value_lanes: Vec<IngressValueLaneSpec>,
    /// Specifications for the map lanes to define for the connector. This includes a pattern to define a selector
    /// that will pick out updates to apply to that lane, from a MQTT message.
    pub map_lanes: Vec<IngressMapLaneSpec>,
    pub relays: Vec<RelaySpecification>,
    /// Deserialization format to use to interpret the contents of the payloads of the MQTT messages.
    pub payload_deserializer: DataFormat,
    /// The MQTT topics to subscribe to.
    pub subscription: Subscription,
    pub keep_alive_secs: Option<u64>,
    pub max_packet_size: Option<usize>,
    pub channel_size: Option<usize>,
    pub credentials: Option<Credentials>,
}

/// Configuration parameters for the MQTT egress connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "mqtt")]
pub struct MqttEgressConfiguration {
    /// The MQTT connection Url.
    pub url: String,
    /// The fixed MQTT topic for messages that do not specify one.
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
    /// Serialization format to use when writing payloads.
    pub payload_serializer: DataFormat,
    pub keep_alive_secs: Option<u64>,
    pub max_packet_size: Option<usize>,
    pub max_inflight: Option<u32>,
    pub channel_size: Option<usize>,
    pub credentials: Option<Credentials>,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
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
    /// A selector for for the payload of the message. If not specified, the entire value will be used.
    pub payload_selector: Option<String>,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct EgressLaneSpec {
    /// A name to use for the lane.
    pub name: String,
    /// Specification for extracting the Kafka message from the lane events.
    pub extractor: ExtractionSpec,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct EgressDownlinkSpec {
    pub address: Address<String>,
    /// Specification for extracting the Kafka message from the downlink events.
    pub extractor: ExtractionSpec,
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub enum Subscription {
    #[form(tag = "topic")]
    Topic(#[form(header_body)] String),
    #[form(tag = "topics")]
    Topics(#[form(header_body)] Vec<String>),
    #[form(tag = "filter")]
    Filters(#[form(header_body)] Vec<String>),
}
