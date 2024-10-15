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
use std::path::Path;
use std::str::FromStr;
use swimos_connector::config::{IngressMapLaneSpec, IngressValueLaneSpec, RelaySpecification};
use swimos_connector::selector::Relays;
use swimos_connector::BadSelector;
use swimos_form::Form;
use swimos_recon::parser::parse_recognize;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Configuration parameters for the Kafka ingress connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "kafka")]
struct KafkaIngressSpecification {
    properties: HashMap<String, String>,
    log_level: KafkaLogLevel,
    value_lanes: Vec<IngressValueLaneSpec>,
    map_lanes: Vec<IngressMapLaneSpec>,
    key_deserializer: DataFormat,
    payload_deserializer: DataFormat,
    topics: Vec<String>,
    relays: Vec<RelaySpecification>,
}

impl KafkaIngressSpecification {
    pub fn build(self) -> Result<KafkaIngressConfiguration, BadSelector> {
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

        Ok(KafkaIngressConfiguration {
            properties,
            log_level,
            value_lanes,
            map_lanes,
            key_deserializer,
            payload_deserializer,
            topics,
            relays: Relays::try_from(relays)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct KafkaIngressConfiguration {
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
    pub relays: Relays,
}

impl KafkaIngressConfiguration {
    pub async fn from_file(path: impl AsRef<Path>) -> Result<KafkaIngressConfiguration, BoxError> {
        let content = tokio::fs::read_to_string(path).await?;
        KafkaIngressConfiguration::from_str(&content)
    }
}

impl FromStr for KafkaIngressConfiguration {
    type Err = BoxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let config = parse_recognize::<KafkaIngressSpecification>(s, true)?.build()?;
        Ok(config)
    }
}
