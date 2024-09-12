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

use swimos_form::Form;

use crate::{
    deser::{
        BoxMessageDeserializer, BytesDeserializer, F32Deserializer, F64Deserializer,
        I32Deserializer, I64Deserializer, MessageDeserializer, ReconDeserializer,
        StringDeserializer, U32Deserializer, U64Deserializer, UuidDeserializer,
    },
    ser::{
        BytesSerializer, F32Serializer, F64Serializer, I32Serializer, I64Serializer,
        MessageSerializer, ReconSerializer, SharedMessageSerializer, StringSerializer,
        U32Serializer, U64Serializer, UuidSerializer,
    },
    Endianness, LoadError,
};

/// Supported deserialization formats to use to interpret a component of a Kafka message.
#[derive(Clone, Form, Debug, Default, PartialEq, Eq)]
pub enum DataFormat {
    #[default]
    Bytes,
    String,
    Int32(#[form(header_body)] Endianness),
    Int64(#[form(header_body)] Endianness),
    UInt32(#[form(header_body)] Endianness),
    UInt64(#[form(header_body)] Endianness),
    Float32(#[form(header_body)] Endianness),
    Float64(#[form(header_body)] Endianness),
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

impl DataFormat {
    /// Attempt to load a deserializer based on the format descriptor.
    pub async fn load_deserializer(&self) -> Result<BoxMessageDeserializer, LoadError> {
        match self {
            DataFormat::Bytes => Ok(BytesDeserializer.boxed()),
            DataFormat::String => Ok(StringDeserializer.boxed()),
            DataFormat::Int32(endianness) => Ok(I32Deserializer::new(*endianness).boxed()),
            DataFormat::Int64(endianness) => Ok(I64Deserializer::new(*endianness).boxed()),
            DataFormat::UInt32(endianness) => Ok(U32Deserializer::new(*endianness).boxed()),
            DataFormat::UInt64(endianness) => Ok(U64Deserializer::new(*endianness).boxed()),
            DataFormat::Float32(endianness) => Ok(F32Deserializer::new(*endianness).boxed()),
            DataFormat::Float64(endianness) => Ok(F64Deserializer::new(*endianness).boxed()),
            DataFormat::Uuid => Ok(UuidDeserializer.boxed()),
            DataFormat::Recon => Ok(ReconDeserializer.boxed()),
            #[cfg(feature = "json")]
            DataFormat::Json => Ok(crate::deser::JsonDeserializer.boxed()),
            #[cfg(feature = "avro")]
            DataFormat::Avro { schema_path } => {
                if let Some(path) = schema_path {
                    let schema = load_schema(path).await?;
                    Ok(crate::deser::AvroDeserializer::new(schema).boxed())
                } else {
                    Ok(crate::deser::AvroDeserializer::default().boxed())
                }
            }
        }
    }

    pub async fn load_serializer(&self) -> Result<SharedMessageSerializer, LoadError> {
        match self {
            DataFormat::Bytes => Ok(BytesSerializer.shared()),
            DataFormat::String => Ok(StringSerializer.shared()),
            DataFormat::Int32(endianness) => Ok(I32Serializer::new(*endianness).shared()),
            DataFormat::Int64(endianness) => Ok(I64Serializer::new(*endianness).shared()),
            DataFormat::UInt32(endianness) => Ok(U32Serializer::new(*endianness).shared()),
            DataFormat::UInt64(endianness) => Ok(U64Serializer::new(*endianness).shared()),
            DataFormat::Float32(endianness) => Ok(F32Serializer::new(*endianness).shared()),
            DataFormat::Float64(endianness) => Ok(F64Serializer::new(*endianness).shared()),
            DataFormat::Uuid => Ok(UuidSerializer.shared()),
            DataFormat::Recon => Ok(ReconSerializer.shared()),
            #[cfg(feature = "json")]
            DataFormat::Json => Ok(crate::ser::JsonSerializer.shared()),
            #[cfg(feature = "avro")]
            DataFormat::Avro { schema_path } => {
                let schema = if let Some(path) = schema_path {
                    load_schema(path).await?
                } else {
                    return Err(LoadError::InvalidConfiguration(
                        "A schema is required.".to_string(),
                    ));
                };
                Ok(crate::ser::AvroSerializer::new(schema).shared())
            }
        }
    }
}

#[cfg(feature = "avro")]
async fn load_schema(path: &str) -> Result<apache_avro::Schema, LoadError> {
    use tokio::{fs::File, io::AsyncReadExt};
    let mut file = File::open(path).await?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;
    let schema = apache_avro::Schema::parse_str(&contents)
        .map_err(|e| LoadError::InvalidDescriptor(Box::new(e)))?;
    Ok(schema)
}
