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

use swimos_api::address::Address;
use swimos_connector::{
    deser::Endianness,
    selector::{KeyOrValue, TopicExtractor},
    InvalidExtractors,
};
use swimos_model::{Item, Value};

use crate::{
    config::{EgressDownlinkSpec, EgressLaneSpec, KafkaEgressConfiguration, TopicSpecifier},
    DataFormat, ExtractionSpec, KafkaLogLevel,
};

use super::{FieldExtractor, MessageSelector, MessageSelectors};
use swimos_connector::selector::{BasicSelector, ChainSelector, SlotSelector};

const FIXED_TOPIC: &str = "fixed";
const OTHER_TOPIC: &str = "other";
const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";
const HOST: &str = "ws://remote:8080";
const NODE1: &str = "/node1";
const NODE2: &str = "/node2";
const LANE: &str = "lane";

fn empty_config() -> KafkaEgressConfiguration {
    KafkaEgressConfiguration {
        properties: HashMap::new(),
        log_level: KafkaLogLevel::Warning,
        key_serializer: DataFormat::Int32(Endianness::BigEndian),
        payload_serializer: DataFormat::Recon,
        fixed_topic: Some(FIXED_TOPIC.to_string()),
        value_lanes: vec![],
        map_lanes: vec![],
        event_downlinks: vec![],
        map_event_downlinks: vec![],
        retry_timeout_ms: 5000,
    }
}

fn test_config() -> KafkaEgressConfiguration {
    let value_lanes = vec![EgressLaneSpec {
        name: VALUE_LANE.to_string(),
        extractor: ExtractionSpec {
            topic_specifier: TopicSpecifier::Fixed,
            key_selector: None,
            payload_selector: Some("$value.field".to_string()),
        },
    }];
    let vec = vec![EgressLaneSpec {
        name: MAP_LANE.to_string(),
        extractor: ExtractionSpec {
            topic_specifier: TopicSpecifier::Specified(OTHER_TOPIC.to_string()),
            key_selector: None,
            payload_selector: Some("$value".to_string()),
        },
    }];
    let map_lanes = vec;
    let event_downlinks = vec![EgressDownlinkSpec {
        address: Address {
            host: Some(HOST.to_string()),
            node: NODE1.to_string(),
            lane: LANE.to_string(),
        },
        extractor: ExtractionSpec {
            topic_specifier: TopicSpecifier::Fixed,
            key_selector: Some("$value.key".to_string()),
            payload_selector: Some("$value.payload".to_string()),
        },
    }];
    let map_event_downlinks = vec![EgressDownlinkSpec {
        address: Address {
            host: Some(HOST.to_string()),
            node: NODE2.to_string(),
            lane: LANE.to_string(),
        },
        extractor: ExtractionSpec {
            topic_specifier: TopicSpecifier::Fixed,
            key_selector: None,
            payload_selector: Some("$value".to_string()),
        },
    }];
    KafkaEgressConfiguration {
        value_lanes,
        map_lanes,
        event_downlinks,
        map_event_downlinks,
        ..empty_config()
    }
}

fn expected_extractors() -> MessageSelectors {
    let value_lanes = [(
        VALUE_LANE.to_string(),
        MessageSelector::new(
            TopicExtractor::Fixed(FIXED_TOPIC.to_string()),
            None,
            Some(FieldExtractor::new(
                KeyOrValue::Value,
                ChainSelector::from(vec![BasicSelector::Slot(SlotSelector::for_field("field"))]),
            )),
        ),
    )]
    .into_iter()
    .collect();
    let map_lanes = [(
        MAP_LANE.to_string(),
        MessageSelector::new(
            TopicExtractor::Fixed(OTHER_TOPIC.to_string()),
            None,
            Some(FieldExtractor::new(
                KeyOrValue::Value,
                ChainSelector::default(),
            )),
        ),
    )]
    .into_iter()
    .collect();
    let event_downlinks = [(
        Address::new(Some(HOST.to_string()), NODE1.to_string(), LANE.to_string()),
        MessageSelector::new(
            TopicExtractor::Fixed(FIXED_TOPIC.to_string()),
            Some(FieldExtractor::new(
                KeyOrValue::Value,
                ChainSelector::from(vec![BasicSelector::Slot(SlotSelector::for_field("key"))]),
            )),
            Some(FieldExtractor::new(
                KeyOrValue::Value,
                ChainSelector::from(vec![BasicSelector::Slot(SlotSelector::for_field(
                    "payload",
                ))]),
            )),
        ),
    )]
    .into_iter()
    .collect();
    let map_event_downlinks = [(
        Address::new(Some(HOST.to_string()), NODE2.to_string(), LANE.to_string()),
        MessageSelector::new(
            TopicExtractor::Fixed(FIXED_TOPIC.to_string()),
            None,
            Some(FieldExtractor::new(
                KeyOrValue::Value,
                ChainSelector::default(),
            )),
        ),
    )]
    .into_iter()
    .collect();
    MessageSelectors {
        value_lanes,
        map_lanes,
        event_downlinks,
        map_event_downlinks,
    }
}

#[test]
fn extractors_from_config() {
    let config = test_config();
    let extractors = MessageSelectors::try_from(&config).expect("Should be valid.");
    let expected = expected_extractors();
    assert_eq!(extractors, expected);
}

fn test_value() -> Value {
    let fields = vec![
        Item::slot("record_topic", "example"),
        Item::slot("record_key", 23),
        Item::slot("record_payload", "data"),
    ];
    Value::record(fields)
}

#[test]
fn field_selector_key() {
    let selector = FieldExtractor::new(KeyOrValue::Key, ChainSelector::default());

    let key = Value::from(5);
    let value = test_value();
    let selected = selector.select(Some(&key), &value);

    assert_eq!(selected, Some(&key));

    let selected = selector.select(None, &value);
    assert!(selected.is_none());
}

#[test]
fn field_selector_value() {
    let selector = FieldExtractor::new(
        KeyOrValue::Value,
        ChainSelector::from(vec![BasicSelector::Slot(SlotSelector::for_field(
            "record_payload",
        ))]),
    );

    let key = Value::from(5);
    let value = test_value();
    let selected = selector.select(Some(&key), &value);

    assert_eq!(selected, Some(&Value::from("data")));

    let selected = selector.select(None, &value);
    assert_eq!(selected, Some(&Value::from("data")));
}

#[test]
fn topic_selector_fixed() {
    let selector = TopicExtractor::Fixed("fixed".to_string());
    let key = Value::from("key");
    let value = test_value();
    let selected = selector.select(Some(&key), &value);

    assert_eq!(selected, Some("fixed"));
}

#[test]
fn topic_selector_key() {
    let selector = TopicExtractor::Selector(FieldExtractor::new(
        KeyOrValue::Key,
        ChainSelector::default(),
    ));
    let key = Value::from("key");
    let value = test_value();
    let selected = selector.select(Some(&key), &value);

    assert_eq!(selected, Some("key"));
}

#[test]
fn topic_selector_value() {
    let selector = TopicExtractor::Selector(FieldExtractor::new(
        KeyOrValue::Value,
        ChainSelector::from(vec![BasicSelector::Slot(SlotSelector::for_field(
            "record_topic",
        ))]),
    ));
    let key = Value::from("key");
    let value = test_value();
    let selected = selector.select(Some(&key), &value);

    assert_eq!(selected, Some("example"));
}

#[test]
fn message_selector() {
    let selector = MessageSelector::new(
        TopicExtractor::Fixed(FIXED_TOPIC.to_string()),
        Some(FieldExtractor::new(
            KeyOrValue::Key,
            ChainSelector::default(),
        )),
        Some(FieldExtractor::new(
            KeyOrValue::Value,
            ChainSelector::from(vec![BasicSelector::Slot(SlotSelector::for_field(
                "record_payload",
            ))]),
        )),
    );

    let key = Value::from("key");
    let value = test_value();

    assert_eq!(selector.select_topic(Some(&key), &value), Some(FIXED_TOPIC));
    assert_eq!(selector.select_key(Some(&key), &value), Some(&key));
    assert_eq!(
        selector.select_payload(Some(&key), &value),
        Some(&Value::from("data"))
    );
}

#[test]
fn message_selector_no_key_selector() {
    let selector = MessageSelector::new(
        TopicExtractor::Fixed(FIXED_TOPIC.to_string()),
        None,
        Some(FieldExtractor::new(
            KeyOrValue::Value,
            ChainSelector::from(vec![BasicSelector::Slot(SlotSelector::for_field(
                "record_payload",
            ))]),
        )),
    );

    let key = Value::from("key");
    let value = test_value();

    assert_eq!(selector.select_topic(Some(&key), &value), Some(FIXED_TOPIC));
    assert!(selector.select_key(Some(&key), &value).is_none());
    assert_eq!(
        selector.select_payload(Some(&key), &value),
        Some(&Value::from("data"))
    );
}

#[test]
fn message_selector_no_value_selector() {
    let selector = MessageSelector::new(
        TopicExtractor::Fixed(FIXED_TOPIC.to_string()),
        Some(FieldExtractor::new(
            KeyOrValue::Key,
            ChainSelector::default(),
        )),
        None,
    );

    let key = Value::from("key");
    let value = test_value();

    assert_eq!(selector.select_topic(Some(&key), &value), Some(FIXED_TOPIC));
    assert_eq!(selector.select_key(Some(&key), &value), Some(&key));
    assert_eq!(selector.select_payload(Some(&key), &value), Some(&value));
}

#[test]
fn duplicate_value_lane() {
    let config = KafkaEgressConfiguration {
        value_lanes: vec![
            EgressLaneSpec {
                name: VALUE_LANE.to_string(),
                extractor: Default::default(),
            },
            EgressLaneSpec {
                name: VALUE_LANE.to_string(),
                extractor: Default::default(),
            },
        ],
        ..empty_config()
    };
    if let Err(InvalidExtractors::NameCollision(name)) = MessageSelectors::try_from(&config) {
        assert_eq!(name, VALUE_LANE);
    } else {
        panic!("Expected name collision error.");
    }
}

#[test]
fn duplicate_map_lane() {
    let config = KafkaEgressConfiguration {
        map_lanes: vec![
            EgressLaneSpec {
                name: MAP_LANE.to_string(),
                extractor: Default::default(),
            },
            EgressLaneSpec {
                name: MAP_LANE.to_string(),
                extractor: Default::default(),
            },
        ],
        ..empty_config()
    };
    if let Err(InvalidExtractors::NameCollision(name)) = MessageSelectors::try_from(&config) {
        assert_eq!(name, MAP_LANE);
    } else {
        panic!("Expected name collision error.");
    }
}

#[test]
fn duplicate_value_and_map_lane() {
    let config = KafkaEgressConfiguration {
        value_lanes: vec![EgressLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: Default::default(),
        }],
        map_lanes: vec![EgressLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: Default::default(),
        }],
        ..empty_config()
    };
    if let Err(InvalidExtractors::NameCollision(name)) = MessageSelectors::try_from(&config) {
        assert_eq!(name, VALUE_LANE);
    } else {
        panic!("Expected name collision error.");
    }
}

#[test]
fn duplicate_value_downlink() {
    let addr = Address {
        host: Some(HOST.to_string()),
        node: NODE1.to_string(),
        lane: LANE.to_string(),
    };
    let config = KafkaEgressConfiguration {
        event_downlinks: vec![
            EgressDownlinkSpec {
                address: addr.clone(),
                extractor: ExtractionSpec::default(),
            },
            EgressDownlinkSpec {
                address: addr.clone(),
                extractor: ExtractionSpec::default(),
            },
        ],
        ..empty_config()
    };
    if let Err(InvalidExtractors::AddressCollision(address)) = MessageSelectors::try_from(&config) {
        assert_eq!(address, addr);
    } else {
        panic!("Expected name collision error.");
    }
}

#[test]
fn duplicate_map_downlink() {
    let addr = Address {
        host: Some(HOST.to_string()),
        node: NODE1.to_string(),
        lane: LANE.to_string(),
    };
    let config = KafkaEgressConfiguration {
        map_event_downlinks: vec![
            EgressDownlinkSpec {
                address: addr.clone(),
                extractor: ExtractionSpec::default(),
            },
            EgressDownlinkSpec {
                address: addr.clone(),
                extractor: ExtractionSpec::default(),
            },
        ],
        ..empty_config()
    };
    if let Err(InvalidExtractors::AddressCollision(address)) = MessageSelectors::try_from(&config) {
        assert_eq!(address, addr);
    } else {
        panic!("Expected name collision error.");
    }
}

#[test]
fn duplicate_value_and_map_downlink() {
    let addr = Address {
        host: Some(HOST.to_string()),
        node: NODE1.to_string(),
        lane: LANE.to_string(),
    };
    let config = KafkaEgressConfiguration {
        event_downlinks: vec![EgressDownlinkSpec {
            address: addr.clone(),
            extractor: ExtractionSpec::default(),
        }],
        map_event_downlinks: vec![EgressDownlinkSpec {
            address: addr.clone(),
            extractor: ExtractionSpec::default(),
        }],
        ..empty_config()
    };
    if let Err(InvalidExtractors::AddressCollision(address)) = MessageSelectors::try_from(&config) {
        assert_eq!(address, addr);
    } else {
        panic!("Expected name collision error.");
    }
}
