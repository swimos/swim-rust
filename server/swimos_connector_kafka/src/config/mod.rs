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

use rdkafka::config::RDKafkaLogLevel;

mod egress;
mod format;
mod ingress;

pub use format::DataFormat;
pub use ingress::{KafkaIngressConfiguration, MapLaneSpec, ValueLaneSpec};
use swimos_form::Form;

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
