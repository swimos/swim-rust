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

use swimos_api::address::Address;
use swimos_connector::{
    config::format::DataFormat,
    selector::{
        BasicSelector, ChainSelector, FieldExtractor, KeyOrValue, SlotSelector, TopicExtractor,
    },
};
use swimos_model::{Item, Value};

use crate::{
    config::{ExtractionSpec, TopicSpecifier},
    EgressDownlinkSpec, EgressLaneSpec, MqttEgressConfiguration,
};

use super::{MessageExtractor, MessageExtractors};

fn mk_record() -> Value {
    Value::record(vec![
        Item::slot("topic", "target"),
        Item::slot("value", 434),
    ])
}

#[test]
fn simple_message_extractor() {
    let extractor = MessageExtractor::new(TopicExtractor::Fixed("topic".to_string()), None);

    let value = Value::from(434);
    assert_eq!(extractor.extract_topic(None, &value), Some("topic"));
    assert_eq!(extractor.extract_payload(None, &value), Some(&value));
}

#[test]
fn complex_message_extractor() {
    let topic_selector =
        ChainSelector::from([BasicSelector::Slot(SlotSelector::for_field("topic"))]);
    let value_selector =
        ChainSelector::from([BasicSelector::Slot(SlotSelector::for_field("value"))]);
    let extractor = MessageExtractor::new(
        TopicExtractor::Selector(FieldExtractor::new(KeyOrValue::Value, topic_selector)),
        Some(FieldExtractor::new(KeyOrValue::Value, value_selector)),
    );

    let value = mk_record();
    assert_eq!(extractor.extract_topic(None, &value), Some("target"));
    assert_eq!(
        extractor.extract_payload(None, &value),
        Some(&Value::from(434))
    );
}

#[test]
fn key_message_extractor() {
    let topic_selector = ChainSelector::default();
    let extractor = MessageExtractor::new(
        TopicExtractor::Selector(FieldExtractor::new(KeyOrValue::Key, topic_selector)),
        None,
    );

    let key = Value::text("target");
    let value = Value::text("hello");
    assert_eq!(extractor.extract_topic(Some(&key), &value), Some("target"));
    assert_eq!(extractor.extract_payload(Some(&key), &value), Some(&value));
}

const HOST: &str = "ws://host:8080";
const NODE1: &str = "/node1";
const NODE2: &str = "/node2";
const LANE: &str = "lane";

fn make_config() -> MqttEgressConfiguration {
    MqttEgressConfiguration {
        url: "mqtt://localhost".to_string(),
        fixed_topic: Some("fixed".to_string()),
        value_lanes: vec![EgressLaneSpec {
            name: "value_lane".to_string(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                payload_selector: None,
            },
        }],
        map_lanes: vec![EgressLaneSpec {
            name: "map_lane".to_string(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Selector("$key".to_string()),
                payload_selector: Some("$value".to_string()),
            },
        }],
        value_downlinks: vec![EgressDownlinkSpec {
            address: Address::new(Some(HOST), NODE1, LANE).owned(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                payload_selector: None,
            },
        }],
        map_downlinks: vec![EgressDownlinkSpec {
            address: Address::new(None, NODE2, LANE).owned(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Selector("$key".to_string()),
                payload_selector: Some("$value".to_string()),
            },
        }],
        payload_serializer: DataFormat::Recon,
        keep_alive_secs: None,
        max_packet_size: None,
        max_inflight: None,
        channel_size: None,
        credentials: None,
    }
}

#[test]
fn extractors_from_config() {
    let config = make_config();
    let extractors = MessageExtractors::try_from(&config).expect("Invalid configuration.");

    let key_select = FieldExtractor::new(KeyOrValue::Key, ChainSelector::default());
    let value_select = FieldExtractor::new(KeyOrValue::Value, ChainSelector::default());

    let expected = MessageExtractors {
        value_lanes: [(
            "value_lane".to_string(),
            MessageExtractor::new(TopicExtractor::Fixed("fixed".to_string()), None),
        )]
        .into_iter()
        .collect(),
        map_lanes: [(
            "map_lane".to_string(),
            MessageExtractor::new(
                TopicExtractor::Selector(key_select.clone()),
                Some(value_select.clone()),
            ),
        )]
        .into_iter()
        .collect(),
        value_downlinks: [(
            Address::new(Some(HOST.to_string()), NODE1.to_string(), LANE.to_string()),
            MessageExtractor::new(TopicExtractor::Fixed("fixed".to_string()), None),
        )]
        .into_iter()
        .collect(),
        map_downlinks: [(
            Address::new(None, NODE2.to_string(), LANE.to_string()),
            MessageExtractor::new(TopicExtractor::Selector(key_select), Some(value_select)),
        )]
        .into_iter()
        .collect(),
    };

    assert_eq!(extractors, expected);
}
