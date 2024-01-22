// Copyright 2015-2023 Swim Inc.
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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use url::Url;

#[derive(Debug)]
pub enum ConnectorKind {
    Kafka,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "properties")]
pub enum ConnectorSpec {
    Kafka(KafkaConnectorSpec),
}

impl ConnectorSpec {
    pub fn kind(&self) -> ConnectorKind {
        match self {
            ConnectorSpec::Kafka(_) => ConnectorKind::Kafka,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "config")]
pub enum ConnectorDef {
    Kafka(KafkaConnectorDef),
}

#[derive(Serialize, Deserialize)]
pub struct KafkaConnectorSpec {
    pub broker: Url,
    pub topic: String,
    pub group: String,
    pub module: Vec<u8>,
    pub properties: ConnectorProperties,
}

impl Debug for KafkaConnectorSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let KafkaConnectorSpec {
            broker,
            topic,
            group,
            properties,
            ..
        } = self;
        f.debug_struct("KafkaConnectorSpec")
            .field("broker", broker)
            .field("topic", topic)
            .field("group", group)
            .field("module", &"..")
            .field("properties", properties)
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KafkaConnectorDef {
    pub broker: Url,
    pub topic: String,
    pub group: String,
    pub module: String,
    pub properties: ConnectorProperties,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ConnectorProperties(HashMap<String, String>);
