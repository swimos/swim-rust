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

use super::{DataFormat, KafkaLogLevel};
use std::collections::HashMap;
use std::str::FromStr;
use swimos_connector::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use swimos_connector::{
    LaneSelector, MapRelaySpecification, NodeSelector, ParseError, PayloadSelector, Relay,
    RelaySpecification, Relays, ValueRelaySpecification,
};
use swimos_form::Form;

/// Configuration parameters for the Kafka ingress connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "kafka")]
pub struct KafkaIngressSpecification {
    /// Properties to configure the Kafka consumer.
    pub properties: HashMap<String, String>,
    /// Log level for the Kafka consumer.
    pub log_level: KafkaLogLevel,
    /// Specifications for the value lanes to define for the connector. This includes a pattern to define a selector
    /// that will pick out values to set to that lane, from a Kafka message.
    pub value_lanes: Vec<IngressValueLaneSpec>,
    /// Specifications for the map lanes to define for the connector. This includes a pattern to define a selector
    /// that will pick out updates to apply to that lane, from a Kafka message.
    pub map_lanes: Vec<IngressMapLaneSpec>,
    /// Deserialization format to use to interpret the contents of the keys of the Kafka messages.
    pub key_deserializer: DataFormat,
    /// Deserialization format to use to interpret the contents of the payloads of the Kafka messages.
    pub payload_deserializer: DataFormat,
    /// A list of Kafka topics to subscribe to.
    pub topics: Vec<String>,
    /// Collection of relays used for forwarding messages to lanes on agents.
    pub relays: Vec<RelaySpecification>,
}

impl KafkaIngressSpecification {
    pub fn build(self) -> Result<KafkaIngressConfiguration, ParseError> {
        let KafkaIngressSpecification {
            properties,
            log_level,
            value_lanes,
            map_lanes,
            key_deserializer,
            payload_deserializer,
            topics,
            relays,
        } = self;

        let mut chain = Vec::with_capacity(relays.len());

        for spec in relays {
            match spec {
                RelaySpecification::Value(ValueRelaySpecification {
                    node,
                    lane,
                    payload,
                    required,
                }) => {
                    let node = NodeSelector::from_str(node.as_str())?;
                    let lane = LaneSelector::from_str(lane.as_str())?;
                    let payload = PayloadSelector::value(payload.as_str(), required)?;

                    chain.push(Relay::new(node, lane, payload));
                }
                RelaySpecification::Map(MapRelaySpecification {
                    node,
                    lane,
                    key,
                    value,
                    required,
                    remove_when_no_value,
                }) => {
                    let node = NodeSelector::from_str(node.as_str())?;
                    let lane = LaneSelector::from_str(lane.as_str())?;
                    let payload = PayloadSelector::map(
                        key.as_str(),
                        value.as_str(),
                        required,
                        remove_when_no_value,
                    )?;

                    chain.push(Relay::new(node, lane, payload));
                }
            }
        }

        Ok(KafkaIngressConfiguration {
            properties,
            log_level,
            value_lanes,
            map_lanes,
            key_deserializer,
            payload_deserializer,
            topics,
            relays: Relays::from(chain),
        })
    }
}

#[derive(Clone, Debug)]
pub struct KafkaIngressConfiguration {
    pub properties: HashMap<String, String>,
    pub log_level: KafkaLogLevel,
    pub value_lanes: Vec<IngressValueLaneSpec>,
    pub map_lanes: Vec<IngressMapLaneSpec>,
    pub key_deserializer: DataFormat,
    pub payload_deserializer: DataFormat,
    pub topics: Vec<String>,
    pub relays: Relays,
}
