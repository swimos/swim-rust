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

use rdkafka::error::KafkaError;
use swimos_api::address::Address;
use swimos_connector::{BadSelector, LoadError, SelectorError, SerializationError};
use thiserror::Error;

/// Errors that can be produced by the Kafka connector.
#[derive(Debug, Error)]
pub enum KafkaConnectorError {
    /// Failed to load the deserializers required to interpret the Kafka messages.
    #[error(transparent)]
    Configuration(#[from] LoadError),
    /// The specification of at least one lane is invalid.
    #[error(transparent)]
    Lanes(#[from] InvalidLanes),
    /// The Kafka consumer failed.
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    /// Attempting to select the required components of a Kafka message failed.
    #[error(transparent)]
    Lane(#[from] SelectorError),
    #[error("A message was not handled properly and so could not be committed.")]
    MessageNotHandled,
}

/// Error type for an invalid egress extractor specification.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum InvalidExtractor {
    /// A string describing a selector was invalid.
    #[error(transparent)]
    Selector(#[from] BadSelector),
    /// No topic specified for an extractor.
    #[error("An extractor did not provide a topic and no global topic was specified.")]
    NoTopic,
}

/// Error type produced for invalid egress extractors.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum InvalidExtractors {
    /// The specification of an extractor was not valid.
    #[error(transparent)]
    Spec(#[from] InvalidExtractor),
    /// There are lane extractors with the same name.
    #[error("The lane name {0} occurs more than once.")]
    NameCollision(String),
    /// There are downlink extractors with the same address.
    #[error("The downlink address {0} occurs more than once.")]
    AddressCollision(Address<String>),
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Error)]
#[error("Connector agent initialized twice.")]
pub struct DoubleInitialization;

#[derive(Debug, Error)]
pub enum KafkaSenderError {
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error(transparent)]
    Serialization(#[from] SerializationError),
    #[error("The connector was not initialized.")]
    NotInitialized,
}
