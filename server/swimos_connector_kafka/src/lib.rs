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

mod config;
mod connector;
mod deser;
mod error;
mod facade;
mod selector;

pub use config::{
    DeserializationFormat, KafkaConnectorConfiguration, KafkaLogLevel, MapLaneSpec, ValueLaneSpec,
};
pub use connector::{KafkaConnector, MessagePart};
#[cfg(feature = "avro")]
pub use deser::AvroDeserializer;
#[cfg(feature = "json")]
pub use deser::JsonDeserializer;
pub use deser::{
    BoxErrorDeserializer, BoxMessageDeserializer, BytesDeserializer, Endianness, F32Deserializer,
    F64Deserializer, I32Deserializer, I64Deserializer, MessageDeserializer, MessageView,
    ReconDeserializer, StringDeserializer, U32Deserializer, U64Deserializer, UuidDeserializer,
};
pub use error::KafkaConnectorError;
