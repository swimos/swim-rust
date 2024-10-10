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

use swimos_model::{Value, ValueKind};
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
