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
mod error;
mod facade;
mod selector;

pub use config::{
    DataFormat, DownlinkAddress, EgressDownlinkSpec, EgressLaneSpec, ExtractionSpec,
    KafkaEgressConfiguration, KafkaIngressConfiguration, KafkaIngressSpecification, KafkaLogLevel,
    TopicSpecifier,
};
pub use connector::{KafkaEgressConnector, KafkaIngressConnector};
pub use error::{
    DoubleInitialization, InvalidExtractor, InvalidExtractors, KafkaConnectorError,
    KafkaSenderError,
};
pub use swimos_connector::{
    config::{IngressMapLaneSpec, IngressValueLaneSpec},
    deser::Endianness,
    BadSelector, DeserializationError, InvalidLaneSpec, InvalidLanes, LoadError, SelectorError,
    SerializationError,
};
