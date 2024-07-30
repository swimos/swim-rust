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

#[derive(Clone, Debug, Form)]
#[form(tag = "kafka")]
pub struct KafkaConnectorConfiguration {
    pub properties: HashMap<String, String>,
    pub log_level: KafkaLogLevel,
    pub value_lanes: Vec<ValueLaneSpec>,
    pub map_lanes: Vec<MapLaneSpec>,
    pub key_deserializer: DeserializationFormat,
    pub value_deserializer: DeserializationFormat,
}

#[derive(Clone, Debug, Form)]
pub struct ValueLaneSpec {
    pub name: Option<String>,
    pub selector: String,
    pub required: bool,
}

impl ValueLaneSpec {
    pub fn new(name: Option<String>, selector: String, required: bool) -> Self {
        ValueLaneSpec {
            name,
            selector,
            required,
        }
    }
}

#[derive(Clone, Debug, Form)]
pub struct MapLaneSpec {
    pub name: String,
    pub key_selector: String,
    pub value_selector: String,
    pub remove_when_no_value: bool,
    pub required: bool,
}

impl MapLaneSpec {
    pub fn new(
        name: String,
        key_selector: String,
        value_selector: String,
        remove_when_no_value: bool,
        required: bool,
    ) -> Self {
        MapLaneSpec {
            name,
            key_selector,
            value_selector,
            remove_when_no_value,
            required,
        }
    }
}

#[derive(Clone, Form, Debug)]
pub enum DeserializationFormat {
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
        schema_path: Option<String>,
    },
}

impl DeserializationFormat {
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

#[derive(Clone, Copy, Debug, Form)]
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
