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
use thiserror::Error;

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

#[derive(Debug, Error)]
pub enum DerserializerLoadError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    InvalidDescriptor(#[from] Box<dyn std::error::Error + Send + 'static>),
}

#[derive(Debug, Error)]
pub enum KafkaConnectorError {
    #[error(transparent)]
    Configuration(#[from] DerserializerLoadError),
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error(transparent)]
    Lane(#[from] LaneSelectorError),
}

#[derive(Debug, Error)]
pub enum LaneSelectorError {
    #[error("The field '{0}' is required but did not occur in a message.")]
    MissingRequiredField(String),
    #[error("Deserializing the content of a Kafka message failed: {0}")]
    DeserializationFailed(#[from] DeserializationError),
}
