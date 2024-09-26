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

use crate::{
    config::{EgressMapLaneSpec, EgressValueLaneSpec, KafkaEgressConfiguration, TopicSpecifier},
    DataFormat, ExtractionSpec, KafkaLogLevel,
};

const FIXED_TOPIC: &str = "fixed";

fn empty_config() -> KafkaEgressConfiguration {
    KafkaEgressConfiguration {
        properties: HashMap::new(),
        log_level: KafkaLogLevel::Warning,
        key_serializer: DataFormat::Recon,
        payload_serializer: DataFormat::Recon,
        fixed_topic: Some(FIXED_TOPIC.to_string()),
        value_lanes: vec![],
        map_lanes: vec![],
        value_downlinks: vec![],
        map_downlinks: vec![],
        retry_timeout_ms: 5000,
    }
}

const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";

fn lanes_config() -> KafkaEgressConfiguration {
    KafkaEgressConfiguration {
        value_lanes: vec![EgressValueLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                key_selector: None,
                payload_selector: None,
            },
        }],
        map_lanes: vec![EgressMapLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                key_selector: Some("$key".to_string()),
                payload_selector: None,
            },
        }],
        ..empty_config()
    }
}

#[tokio::test]
async fn open_lanes() {}
