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

mod connector;
mod facade;

pub use connector::KafkaConnector;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use std::collections::HashMap;
use swimos_connector::generic::{
    DerserializerLoadError, DeserializationFormat, InvalidLanes, LaneSelectorError, Lanes,
    MapLaneSpec, ValueLaneSpec,
};
use swimos_form::Form;
use thiserror::Error;

/// Errors that can be produced by the Kafka connector.
#[derive(Debug, Error)]
pub enum KafkaConnectorError {
    /// Failed to load the deserializers required to interpret the Kafka messages.
    #[error(transparent)]
    Configuration(#[from] DerserializerLoadError),
    /// The Kafka consumer failed.
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    /// Attempting to select the required components of a Kafka message failed.
    #[error(transparent)]
    Lane(#[from] LaneSelectorError),
    #[error("A message was not handled properly and so could not be committed.")]
    MessageNotHandled,
}

/// Enumeration of logging levels supported by the underlying Kafka consumer.
#[derive(Clone, Copy, Debug, Form, PartialEq, Eq)]
pub enum KafkaLogLevel {
    Emerg,
    Alert,
    Critical,
    Error,
    Warning,
    Notice,
    Info,
    Debug,
}

impl From<KafkaLogLevel> for RDKafkaLogLevel {
    fn from(value: KafkaLogLevel) -> Self {
        match value {
            KafkaLogLevel::Emerg => RDKafkaLogLevel::Emerg,
            KafkaLogLevel::Alert => RDKafkaLogLevel::Alert,
            KafkaLogLevel::Critical => RDKafkaLogLevel::Critical,
            KafkaLogLevel::Error => RDKafkaLogLevel::Error,
            KafkaLogLevel::Warning => RDKafkaLogLevel::Warning,
            KafkaLogLevel::Notice => RDKafkaLogLevel::Notice,
            KafkaLogLevel::Info => RDKafkaLogLevel::Info,
            KafkaLogLevel::Debug => RDKafkaLogLevel::Debug,
        }
    }
}

/// Configuration parameters for the Kafka connector.
#[derive(Clone, Debug, Form)]
#[form(tag = "kafka")]
pub struct KafkaConnectorConfiguration {
    /// Properties to configure the Kafka consumer.
    pub properties: HashMap<String, String>,
    /// Log level for the Kafka consumer.
    pub log_level: KafkaLogLevel,
    /// Specifications for the value lanes to define for the connector. This includes a pattern to define a selector
    /// that will pick out values to set to that lane, from a Kafka message.
    pub value_lanes: Vec<ValueLaneSpec>,
    /// Specifications for the map lanes to define for the connector. This includes a pattern to define a selector
    /// that will pick out updates to apply to that lane, from a Kafka message.
    pub map_lanes: Vec<MapLaneSpec>,
    /// Deserialization format to use to interpret the contents of the keys of the Kafka messages.
    pub key_deserializer: DeserializationFormat,
    /// Deserialization format to use to interpret the contents of the payloads of the Kafka messages.
    pub payload_deserializer: DeserializationFormat,
    /// A list of Kafka topics to subscribe to.
    pub topics: Vec<String>,
}

impl TryFrom<&KafkaConnectorConfiguration> for Lanes {
    type Error = InvalidLanes;

    fn try_from(value: &KafkaConnectorConfiguration) -> Result<Self, Self::Error> {
        let KafkaConnectorConfiguration {
            value_lanes,
            map_lanes,
            ..
        } = value;
        Lanes::try_from_lane_specs(value_lanes, map_lanes)
    }
}
