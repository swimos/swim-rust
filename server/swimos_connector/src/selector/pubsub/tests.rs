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

use super::{init_regex, SelectorDescriptor};
use super::{MessageField, SelectorComponent};
use crate::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use crate::deser::{Deser, MessageDeserializer, MessagePart, ReconDeserializer};
use crate::selector::{
    AttrSelector, BasicSelector, ChainSelector, IdentitySelector, IndexSelector, MapLaneSelector,
    SelectHandler, SlotSelector, ValueLaneSelector, ValueSelector,
};
use crate::{BadSelector, InvalidLaneSpec};
use swimos_model::{Attr, Item};

use crate::selector::{PubSubSelector, PubSubValueLaneSelector};
use crate::{
    selector::pubsub::{KeySelector, PayloadSelector, TopicSelector},
    test_support::{fail, run_handler, TestSpawner},
    ConnectorAgent,
};
use bytes::BytesMut;
use frunk::hlist;
use std::ops::Deref;
use swimos_agent::agent_model::{AgentSpec, ItemDescriptor};
use swimos_api::agent::WarpLaneKind;
use swimos_model::Value;

#[test]
fn init_regex_creation() {
    super::create_init_regex().expect("Creation failed.");
}

#[test]
fn match_key() {
    if let Some(captures) = init_regex().captures("$key") {
        let kind = captures.get(1).expect("Missing capture.");
        assert!(captures.get(2).is_none());
        assert_eq!(kind.as_str(), "$key");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_payload() {
    if let Some(captures) = super::init_regex().captures("$payload") {
        let kind = captures.get(1).expect("Missing capture.");
        assert!(captures.get(2).is_none());
        assert_eq!(kind.as_str(), "$payload");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_topic() {
    if let Some(captures) = super::init_regex().captures("$topic") {
        let kind = captures.get(1).expect("Missing capture.");
        assert!(captures.get(2).is_none());
        assert_eq!(kind.as_str(), "$topic");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_key_indexed() {
    if let Some(captures) = super::init_regex().captures("$key[3]") {
        let kind = captures.get(1).expect("Missing capture.");
        let index = captures.get(2).expect("Missing capture.");
        assert_eq!(kind.as_str(), "$key");
        assert_eq!(index.as_str(), "3");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_payload_indexed() {
    if let Some(captures) = super::init_regex().captures("$payload[0]") {
        let kind = captures.get(1).expect("Missing capture.");
        let index = captures.get(2).expect("Missing capture.");
        assert_eq!(kind.as_str(), "$payload");
        assert_eq!(index.as_str(), "0");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_attr() {
    if let Some(captures) = super::field_regex().captures("@my_attr") {
        let name = captures.get(1).expect("Missing capture.");
        assert!(captures.get(2).is_none());
        assert_eq!(name.as_str(), "@my_attr");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_attr_indexed() {
    if let Some(captures) = super::field_regex().captures("@attr[73]") {
        let name = captures.get(1).expect("Missing capture.");
        let index = captures.get(2).expect("Missing capture.");
        assert_eq!(name.as_str(), "@attr");
        assert_eq!(index.as_str(), "73");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_slot() {
    if let Some(captures) = super::field_regex().captures("slot") {
        let name = captures.get(1).expect("Missing capture.");
        assert!(captures.get(2).is_none());
        assert_eq!(name.as_str(), "slot");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_slot_indexed() {
    if let Some(captures) = super::field_regex().captures("slot5[123]") {
        let name = captures.get(1).expect("Missing capture.");
        let index = captures.get(2).expect("Missing capture.");
        assert_eq!(name.as_str(), "slot5");
        assert_eq!(index.as_str(), "123");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_slot_non_latin() {
    if let Some(captures) = super::field_regex().captures("اسم[123]") {
        let name = captures.get(1).expect("Missing capture.");
        let index = captures.get(2).expect("Missing capture.");
        assert_eq!(name.as_str(), "اسم");
        assert_eq!(index.as_str(), "123");
    } else {
        panic!("Did not match.");
    }
}

impl<'a> SelectorDescriptor<'a> {
    pub fn for_part(part: MessagePart, index: Option<usize>) -> Self {
        SelectorDescriptor::Part {
            part,
            index,
            components: vec![],
        }
    }

    pub fn push(&mut self, component: SelectorComponent<'a>) {
        if let Self::Part { components, .. } = self {
            components.push(component);
        }
    }
}

#[test]
fn parse_simple() {
    let key = super::parse_selector("$key").expect("Parse failed.");
    assert_eq!(key, SelectorDescriptor::for_part(MessagePart::Key, None));

    let payload = super::parse_selector("$payload").expect("Parse failed.");
    assert_eq!(
        payload,
        SelectorDescriptor::for_part(MessagePart::Payload, None)
    );

    let indexed = super::parse_selector("$key[2]").expect("Parse failed.");
    assert_eq!(
        indexed,
        SelectorDescriptor::for_part(MessagePart::Key, Some(2))
    );
}

#[test]
fn parse_topic() {
    let topic = super::parse_selector("$topic").expect("Parse failed.");
    assert_eq!(topic, SelectorDescriptor::Topic);

    assert_eq!(
        super::parse_selector("$topic[0]"),
        Err(BadSelector::TopicWithComponent)
    );
    assert_eq!(
        super::parse_selector("$topic.slot"),
        Err(BadSelector::TopicWithComponent)
    );
}

#[test]
fn parse_one_component() {
    let first = super::parse_selector("$key.@attr").expect("Parse failed.");
    let mut expected_first = SelectorDescriptor::for_part(MessagePart::Key, None);
    expected_first.push(SelectorComponent::new(true, "attr", None));
    assert_eq!(first, expected_first);

    let second = super::parse_selector("$payload.slot").expect("Parse failed.");
    let mut expected_second = SelectorDescriptor::for_part(MessagePart::Payload, None);
    expected_second.push(SelectorComponent::new(false, "slot", None));
    assert_eq!(second, expected_second);

    let third = super::parse_selector("$key.@attr[3]").expect("Parse failed.");
    let mut expected_third = SelectorDescriptor::for_part(MessagePart::Key, None);
    expected_third.push(SelectorComponent::new(true, "attr", Some(3)));
    assert_eq!(third, expected_third);

    let fourth = super::parse_selector("$payload[6].slot[8]").expect("Parse failed.");
    let mut expected_fourth = SelectorDescriptor::for_part(MessagePart::Payload, Some(6));
    expected_fourth.push(SelectorComponent::new(false, "slot", Some(8)));
    assert_eq!(fourth, expected_fourth);
}

#[test]
fn multi_component_selector() {
    let selector = super::parse_selector("$payload.red.@green[7].blue").expect("Parse failed.");
    let mut expected = SelectorDescriptor::for_part(MessagePart::Payload, None);
    expected.push(SelectorComponent::new(false, "red", None));
    expected.push(SelectorComponent::new(true, "green", Some(7)));
    expected.push(SelectorComponent::new(false, "blue", None));
    assert_eq!(selector, expected);
}

fn test_value() -> Value {
    let inner = Value::Record(
        vec![],
        vec![
            Item::slot("red", 56),
            Item::slot("green", 5),
            Item::slot("blue", Value::Extant),
        ],
    );
    Value::Record(
        vec![Attr::from("attr1"), Attr::from(("attr2", 5))],
        vec![
            Item::from(3),
            Item::from("a"),
            Item::slot("name", true),
            Item::slot("inner", inner),
        ],
    )
}

#[test]
fn identity_selector() {
    let selector = IdentitySelector;
    let value = test_value();
    let selected = selector.select_value(&value);
    assert_eq!(selected, Some(&value));
}

#[test]
fn attr_selector() {
    let value = test_value();

    let selector1 = AttrSelector::new("attr1".to_string());
    let selector2 = AttrSelector::new("attr2".to_string());
    let selector3 = AttrSelector::new("other".to_string());

    assert_eq!(selector1.select_value(&value), Some(&Value::Extant));
    assert_eq!(selector2.select_value(&value), Some(&Value::from(5)));
    assert!(selector3.select_value(&value).is_none());
}

#[test]
fn slot_selector() {
    let value = test_value();

    let selector1 = SlotSelector::for_field("name");
    let selector2 = SlotSelector::for_field("a");
    let selector3 = SlotSelector::for_field("other");

    assert_eq!(selector1.select_value(&value), Some(&Value::from(true)));
    assert!(selector2.select_value(&value).is_none());
    assert!(selector3.select_value(&value).is_none());
}

#[test]
fn index_selector() {
    let value = test_value();

    let selector1 = IndexSelector::new(0);
    let selector2 = IndexSelector::new(2);
    let selector3 = IndexSelector::new(4);

    assert_eq!(selector1.select_value(&value), Some(&Value::from(3)));
    assert_eq!(selector2.select_value(&value), Some(&Value::from(true)));
    assert!(selector3.select_value(&value).is_none());
}

#[test]
fn chain_selector() {
    let value = test_value();

    let selector1 = ChainSelector::from(vec![]);
    let selector2 = ChainSelector::from(vec![BasicSelector::Slot(SlotSelector::for_field("name"))]);
    let selector3 = ChainSelector::from(vec![
        BasicSelector::Slot(SlotSelector::for_field("inner")),
        BasicSelector::Slot(SlotSelector::for_field("green")),
    ]);
    let selector4 = ChainSelector::from(vec![
        BasicSelector::Slot(SlotSelector::for_field("name")),
        BasicSelector::Slot(SlotSelector::for_field("green")),
    ]);

    assert_eq!(selector1.select_value(&value), Some(&value));
    assert_eq!(selector2.select_value(&value), Some(&Value::from(true)));
    assert_eq!(selector3.select_value(&value), Some(&Value::from(5)));
    assert!(selector4.select_value(&value).is_none());
}

#[test]
fn topic_selector_descriptor() {
    let selector = super::parse_selector("$topic").expect("Invalid selector.");
    assert_eq!(selector.field(), MessageField::Topic);
    assert!(selector.selector().is_none());
    assert_eq!(selector.suggested_name(), Some("topic"));
}

#[test]
fn key_selector_descriptor() {
    let selector = super::parse_selector("$key").expect("Invalid selector.");
    assert_eq!(selector.field(), MessageField::Key);
    assert_eq!(selector.selector(), Some(ChainSelector::from(vec![])));
    assert_eq!(selector.suggested_name(), Some("key"));
}

#[test]
fn payload_selector_descriptor() {
    let selector = super::parse_selector("$payload").expect("Invalid selector.");
    assert_eq!(selector.field(), MessageField::Payload);
    assert_eq!(selector.selector(), Some(ChainSelector::from(vec![])));
    assert_eq!(selector.suggested_name(), Some("payload"));
}

#[test]
fn indexed_selector_descriptor() {
    let selector = super::parse_selector("$payload[1]").expect("Invalid selector.");
    assert_eq!(selector.field(), MessageField::Payload);
    assert_eq!(
        selector.selector(),
        Some(ChainSelector::from(vec![BasicSelector::Index(
            IndexSelector::new(1)
        )]))
    );
    assert!(selector.suggested_name().is_none());
}

#[test]
fn attr_selector_descriptor() {
    let selector = super::parse_selector("$payload.@attr").expect("Invalid selector.");
    assert_eq!(selector.field(), MessageField::Payload);
    assert_eq!(
        selector.selector(),
        Some(ChainSelector::from(vec![BasicSelector::Attr(
            AttrSelector::new("attr".to_string())
        )]))
    );
    assert_eq!(selector.suggested_name(), Some("attr"));
}

#[test]
fn slot_selector_descriptor() {
    let selector = super::parse_selector("$payload.slot").expect("Invalid selector.");
    assert_eq!(selector.field(), MessageField::Payload);
    assert_eq!(
        selector.selector(),
        Some(ChainSelector::from(vec![BasicSelector::Slot(
            SlotSelector::for_field("slot")
        )]))
    );
    assert_eq!(selector.suggested_name(), Some("slot"));
}

#[test]
fn complex_selector_descriptor_named() {
    let selector = super::parse_selector("$payload.@attr[3].inner").expect("Invalid selector.");
    assert_eq!(selector.field(), MessageField::Payload);
    assert_eq!(
        selector.selector(),
        Some(ChainSelector::from(vec![
            BasicSelector::Attr(AttrSelector::new("attr".to_string())),
            BasicSelector::Index(IndexSelector::new(3)),
            BasicSelector::Slot(SlotSelector::for_field("inner"))
        ]))
    );
    assert_eq!(selector.suggested_name(), Some("inner"));
}

#[test]
fn complex_selector_descriptor_unnamed() {
    let selector = super::parse_selector("$payload.@attr[3].inner[0]").expect("Invalid selector.");
    assert_eq!(selector.field(), MessageField::Payload);
    assert_eq!(
        selector.selector(),
        Some(ChainSelector::from(vec![
            BasicSelector::Attr(AttrSelector::new("attr".to_string())),
            BasicSelector::Index(IndexSelector::new(3)),
            BasicSelector::Slot(SlotSelector::for_field("inner")),
            BasicSelector::Index(IndexSelector::new(0)),
        ]))
    );
    assert!(selector.suggested_name().is_none());
}

#[test]
fn value_lane_selector_from_spec_inferred_name() {
    let spec = IngressValueLaneSpec::new(None, "$key", true);
    let selector = ValueLaneSelector::try_from(&spec).expect("Bad specification.");
    assert_eq!(selector.name(), "key");
    assert!(&selector.is_required());
    let delegate_selector = selector
        .into_selector()
        .uninject::<KeySelector, _>()
        .expect("Failed to get inner selector");

    assert_eq!(
        delegate_selector,
        KeySelector::new(ChainSelector::default())
    );
}

#[test]
fn value_lane_selector_from_spec_named() {
    let spec = IngressValueLaneSpec::new(Some("field"), "$key[0]", false);
    let selector = ValueLaneSelector::try_from(&spec).expect("Bad specification.");
    assert_eq!(selector.name(), "field");
    assert!(!&selector.is_required());
    let delegate_selector = selector
        .into_selector()
        .uninject::<KeySelector, _>()
        .expect("Failed to get inner selector");
    assert_eq!(
        delegate_selector,
        KeySelector(ChainSelector::from(vec![BasicSelector::Index(
            IndexSelector::new(0)
        )]))
    );
}

#[test]
fn value_lane_selector_from_spec_inferred_unnamed() {
    let spec = IngressValueLaneSpec::new(None, "$key[0]", true);
    let error = ValueLaneSelector::try_from(&spec).expect_err("Should fail.");
    assert_eq!(error, InvalidLaneSpec::NameCannotBeInferred);
}

#[test]
fn value_lane_selector_from_spec_bad_selector() {
    let spec = IngressValueLaneSpec::new(None, "$wrong", true);
    let error = ValueLaneSelector::try_from(&spec).expect_err("Should fail.");
    assert_eq!(error, InvalidLaneSpec::Selector(BadSelector::InvalidRoot));
}

#[test]
fn map_lane_selector_from_spec() {
    let spec = IngressMapLaneSpec::new("field", "$key", "$payload", true, false);
    let selector = MapLaneSelector::try_from(&spec).expect("Bad specification.");
    assert_eq!(selector.name(), "field");
    assert!(!selector.is_required());
    assert!(selector.remove_when_no_value());

    let (key_selector, value_selector) = selector.into_selectors();
    let key_delegate = key_selector
        .uninject::<KeySelector, _>()
        .expect("Failed to get key delegate.");
    let value_delegate = value_selector
        .uninject::<PayloadSelector, _>()
        .expect("Failed to get key delegate.");

    assert_eq!(key_delegate, KeySelector::new(ChainSelector::default()));
    assert_eq!(
        value_delegate,
        PayloadSelector::new(ChainSelector::default())
    );
}

#[test]
fn map_lane_selector_from_spec_bad_key() {
    let spec = IngressMapLaneSpec::new("field", "$other", "$payload", true, false);
    let error = MapLaneSelector::try_from(&spec).expect_err("Should fail.");
    assert_eq!(error, InvalidLaneSpec::Selector(BadSelector::InvalidRoot));
}

#[test]
fn map_lane_selector_from_spec_bad_value() {
    let spec = IngressMapLaneSpec::new("field", "$key", "$other", true, false);
    let error = MapLaneSelector::try_from(&spec).expect_err("Should fail.");
    assert_eq!(error, InvalidLaneSpec::Selector(BadSelector::InvalidRoot));
}

fn run_selector(selector: PubSubSelector, expected: Value) {
    const LANE: &str = "lane";

    let selector = PubSubValueLaneSelector::new(LANE.to_string(), selector, true);
    let topic = Value::from("topic");

    let deserializer = ReconDeserializer.boxed();
    let key = Deser::new(b"13".as_slice(), &deserializer);
    let value = Deser::new(b"64.0".as_slice(), &deserializer);
    let  args = hlist![topic, key, value];
    let handler = selector.select_handler(&args).unwrap();
    let mut agent = ConnectorAgent::default();

    agent
        .register_dynamic_item(
            LANE,
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Value,
                flags: Default::default(),
            },
        )
        .expect("Failed to register lane");

    run_handler(
        &TestSpawner::default(),
        &mut BytesMut::default(),
        &agent,
        handler,
        fail,
    );

    match agent.value_lane(LANE) {
        Some(lane) => lane.deref().read(|state| assert_eq!(state, &expected)),
        None => {
            panic!("Missing lane")
        }
    };
}

#[test]
fn selects_topic() {
    run_selector(PubSubSelector::inject(TopicSelector), Value::from("topic"));
}

#[test]
fn selects_key() {
    run_selector(
        PubSubSelector::inject(KeySelector::new(ChainSelector::default())),
        Value::from(13),
    );
}

#[test]
fn selects_payload() {
    run_selector(
        PubSubSelector::inject(PayloadSelector::new(ChainSelector::default())),
        Value::from(64f64),
    );
}
