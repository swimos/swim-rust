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
use thiserror::Error;

use crate::selector::{parse_selector, BadSelector, LaneSelector};

#[derive(Clone, Debug)]
pub struct KafkaConnectorConfiguration {
    pub properties: HashMap<String, String>,
    pub log_level: RDKafkaLogLevel,
    pub value_lanes: Vec<ValueLaneSpec>,
    pub map_lanes: Vec<MapLaneSpec>,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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
