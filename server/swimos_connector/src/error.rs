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

use std::num::ParseIntError;

use swimos_api::address::Address;
use swimos_model::{Value, ValueKind};
use swimos_recon::parser::ParseError;
use thiserror::Error;

/// An error type that is produced by the [`crate::IngressConnectorLifecycle`] if the [`crate::IngressConnector`] that it wraps
/// fails to complete the initialization phase correctly.
#[derive(Clone, Copy, Default, Debug, Error)]
#[error("The connector initialization failed to complete.")]
pub struct ConnectorInitError;

/// An error type that boxes any type of error that could be returned by a message deserializer.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct DeserializationError(Box<dyn std::error::Error + Send + 'static>);

impl DeserializationError {
    pub fn new<E>(error: E) -> Self
    where
        E: std::error::Error + Send + 'static,
    {
        DeserializationError(Box::new(error))
    }
}

/// An error type that boxes any type of error that could be returned by a message serializer.
#[derive(Debug, Error)]
pub enum SerializationError {
    /// The value is not supported by the serialization format.
    #[error("The serializations scheme does not support values of kind: {0}")]
    InvalidKind(ValueKind),
    /// An integer in a value was out of range for the serialization format.
    #[error("Integer value {0} out of range for the serialization scheme.")]
    IntegerOutOfRange(Value),
    /// An floating point number in a value was out of range for the serialization format.
    #[error("Float value {0} out of range for the serialization scheme.")]
    FloatOutOfRange(f64),
    /// The serializer failed with an error.
    #[error("A value serializer failed: {0}")]
    SerializerFailed(Box<dyn std::error::Error + Send>),
}

/// An error type that can be produced when attempting to load a serializer or deserializer.
#[derive(Debug, Error)]
pub enum LoadError {
    /// Attempting to read a required resource (for example, a file) failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A required resource was invalid.
    #[error(transparent)]
    InvalidDescriptor(#[from] Box<dyn std::error::Error + Send + 'static>),
    #[error("The configuration provided for the serializer or deserializer is invalid: {0}")]
    InvalidConfiguration(String),
    #[error("Loading of the configuration was cancelled.")]
    Cancelled,
}

/// Error type for an invalid egress extractor specification.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
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

/// Error type for an invalid selector descriptor.
#[derive(Clone, Error, Debug, PartialEq, Eq)]
pub enum BadSelector {
    /// An empty string does not describe a valid selector.
    #[error("Selector strings cannot be empty.")]
    EmptySelector,
    /// A selector component may not be empty.
    #[error("Selector components cannot be empty.")]
    EmptyComponent,
    /// The root of a selector must be a valid component of a message.
    #[error("Invalid root selector (must be one of '$key' or '$payload' with an optional index or '$topic').")]
    InvalidRoot,
    /// A component of the descriptor did not describe a valid selector.
    #[error(
        "Invalid component selector (must be an attribute or slot name with an optional index)."
    )]
    InvalidComponent,
    /// The index for an index selector was too large for usize.
    #[error("An index specified was not a valid usize.")]
    IndexOutOfRange,
    /// The topic root cannot have any other components.
    #[error("The topic does not have components.")]
    TopicWithComponent,
    /// A provided lane/node/payload selector was malformed.
    #[error("The selector formed an invalid path.")]
    InvalidPath,
    /// Invalid Recon was specified for a payload selector.
    #[error("Failed to parse Recon value for selector payload")]
    InvalidRecon(#[from] ParseError),
}

impl From<ParseIntError> for BadSelector {
    fn from(_value: ParseIntError) -> Self {
        BadSelector::IndexOutOfRange
    }
}
