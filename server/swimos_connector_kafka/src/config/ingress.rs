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

use super::{DataFormat, KafkaLogLevel};
use swimos_form::Form;

/// Configuration parameters for the Kafka connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "kafka")]
pub struct KafkaIngressConfiguration {
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
    pub key_deserializer: DataFormat,
    /// Deserialization format to use to interpret the contents of the payloads of the Kafka messages.
    pub payload_deserializer: DataFormat,
    /// A list of Kafka topics to subscribe to.
    pub topics: Vec<String>,
}

/// Specification of a value lane for the Kafka connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
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
#[derive(Clone, Debug, Form, PartialEq, Eq)]
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
