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

use rumqttc::{ClientError, ConnectionError, OptionError};
use swimos_connector::{
    BadSelector, InvalidExtractors, InvalidLaneSpec, InvalidLanes, LoadError, SelectorError,
    SerializationError,
};
use thiserror::Error;

/// Errors that can be produced by the Kafka connector.
#[derive(Debug, Error)]
pub enum MqttConnectorError {
    /// The initialization phase of the connector was not completed.
    #[error("The connector was not initialized correctly.")]
    NotInitialized,
    /// An error occurred attempting ot load or interpret the configuration.
    #[error(transparent)]
    Configuration(#[from] LoadError),
    /// The extractors specified for an egress connector were not valid.
    #[error(transparent)]
    Extractors(#[from] InvalidExtractors),
    /// The lanes specified for an ingress connector were not valid.
    #[error(transparent)]
    Lanes(#[from] InvalidLanes),
    /// The MQTT client options were invalid.
    #[error(transparent)]
    Options(#[from] OptionError),
    /// An error occurred in the MQTT cline runtime loop.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// A connection to the MQTT broker failed.
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    /// A selector failed to extract a required component from a message.
    #[error(transparent)]
    Selection(#[from] SelectorError),
    /// An error occurred serializing deserializing the payload of an MQTT message.
    #[error(transparent)]
    Serialization(#[from] SerializationError),
    /// An extractor failed to select a topic for an outgoing message.
    #[error("A topic was not selected for an outgoing message.")]
    MissingTopic,
}

impl From<BadSelector> for MqttConnectorError {
    fn from(err: BadSelector) -> Self {
        MqttConnectorError::Lanes(InvalidLanes::Spec(InvalidLaneSpec::Selector(err)))
    }
}
