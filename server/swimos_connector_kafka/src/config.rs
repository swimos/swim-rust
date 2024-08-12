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

use rdkafka::config::RDKafkaLogLevel;
use swimos_form::Form;

use crate::{
    deser::{
        BoxMessageDeserializer, BytesDeserializer, Endianness, F32Deserializer, F64Deserializer,
        I32Deserializer, I64Deserializer, MessageDeserializer, ReconDeserializer,
        StringDeserializer, U32Deserializer, U64Deserializer, UuidDeserializer,
    },
    error::DerserializerLoadError,
};

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

/// Specification of a value lane for the Kafka connector.
#[derive(Clone, Debug, Form)]
pub struct ValueLaneSpec {
    /// A name to use for the lane. If not specified, the connector will attempt to infer one from the selector.
    pub name: Option<String>,
    /// String representation of a selector to extract values for the lane from Kafka messages.
    pub selector: String,
    /// Whether the lane is required. If this is `true` and the selector returns nothing for a Kafka Message, the
    /// connector will fail with an error.
    pub required: bool,
}

impl ValueLaneSpec {
    /// # Arguments
    /// * `name` - A name to use for the lane. If not specified the connector will attempt to infer a name from the selector.
    /// * `selector` - String representation of the selector to extract values from the Kafka message.
    /// * `required` - Whether the lane is required. If this is `true` and the selector returns nothing for a Kafka Message, the
    ///   connector will fail with an error.
    pub fn new<S: Into<String>>(name: Option<S>, selector: S, required: bool) -> Self {
        ValueLaneSpec {
            name: name.map(Into::into),
            selector: selector.into(),
            required,
        }
    }
}

/// Specification of a value lane for the Kafka connector.
#[derive(Clone, Debug, Form)]
pub struct MapLaneSpec {
    /// The name of the lane.
    pub name: String,
    /// String representation of a selector to extract the map keys from the Kafka messages.
    pub key_selector: String,
    /// String representation of a selector to extract the map values from the Kafka messages.
    pub value_selector: String,
    /// Whether to remove an entry from the map if the value selector does not return a value. Otherwise, missing
    /// values will be treated as a failed extraction from the message.
    pub remove_when_no_value: bool,
    /// Whether the lane is required. If this is `true` and the selector returns nothing for a Kafka Message, the
    /// connector will fail with an error.
    pub required: bool,
}

impl MapLaneSpec {
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `key_selector` - String representation of a selector to extract the map keys from the Kafka messages.
    /// * `value_selector` - String representation of a selector to extract the map values from the Kafka messages.
    /// * `remove_when_no_value` - Whether to remove an entry from the map if the value selector does not return a value. Otherwise, missing
    ///   values will be treated as a failed extraction from the message.
    /// * `required` - Whether the lane is required. If this is `true` and the selector returns nothing for a Kafka Message, the
    ///   connector will fail with an error.
    pub fn new<S: Into<String>>(
        name: S,
        key_selector: S,
        value_selector: S,
        remove_when_no_value: bool,
        required: bool,
    ) -> Self {
        MapLaneSpec {
            name: name.into(),
            key_selector: key_selector.into(),
            value_selector: value_selector.into(),
            remove_when_no_value,
            required,
        }
    }
}

/// Supported deserialization formats to use to interpret a component of a Kafka message.
#[derive(Clone, Form, Debug, Default)]
pub enum DeserializationFormat {
    #[default]
    Bytes,
    String,
    Int32(Endianness),
    Int64(Endianness),
    UInt32(Endianness),
    UInt64(Endianness),
    Float32(Endianness),
    Float64(Endianness),
    Uuid,
    Recon,
    #[cfg(feature = "json")]
    Json,
    #[cfg(feature = "avro")]
    Avro {
        /// If this is specified, loading the deserializer will attempt to load an Avro schema from a file at this path.
        schema_path: Option<String>,
    },
}

impl DeserializationFormat {
    /// Attempt to load a deserializer based on the format descriptor.
    pub async fn load(&self) -> Result<BoxMessageDeserializer, DerserializerLoadError> {
        match self {
            DeserializationFormat::Bytes => Ok(BytesDeserializer.boxed()),
            DeserializationFormat::String => Ok(StringDeserializer.boxed()),
            DeserializationFormat::Int32(endianness) => {
                Ok(I32Deserializer::new(*endianness).boxed())
            }
            DeserializationFormat::Int64(endianness) => {
                Ok(I64Deserializer::new(*endianness).boxed())
            }
            DeserializationFormat::UInt32(endianness) => {
                Ok(U32Deserializer::new(*endianness).boxed())
            }
            DeserializationFormat::UInt64(endianness) => {
                Ok(U64Deserializer::new(*endianness).boxed())
            }
            DeserializationFormat::Float32(endianness) => {
                Ok(F32Deserializer::new(*endianness).boxed())
            }
            DeserializationFormat::Float64(endianness) => {
                Ok(F64Deserializer::new(*endianness).boxed())
            }
            DeserializationFormat::Uuid => Ok(UuidDeserializer.boxed()),
            DeserializationFormat::Recon => Ok(ReconDeserializer.boxed()),
            #[cfg(feature = "json")]
            DeserializationFormat::Json => Ok(crate::deser::JsonDeserializer.boxed()),
            #[cfg(feature = "avro")]
            DeserializationFormat::Avro { schema_path } => {
                use tokio::{fs::File, io::AsyncReadExt};
                if let Some(path) = schema_path {
                    let mut file = File::open(path).await?;
                    let mut contents = String::new();
                    file.read_to_string(&mut contents).await?;
                    let schema = apache_avro::Schema::parse_str(&contents)
                        .map_err(|e| DerserializerLoadError::InvalidDescriptor(Box::new(e)))?;
                    Ok(crate::deser::AvroDeserializer::new(schema).boxed())
                } else {
                    Ok(crate::deser::AvroDeserializer::default().boxed())
                }
            }
        }
    }
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
