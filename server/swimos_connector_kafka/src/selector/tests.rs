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

use crate::connector::MessagePart;
use crate::selector::{BadSelector, SelectorComponent, SelectorDescriptor};

#[test]
fn init_regex_creation() {
    super::create_init_regex().expect("Creation failed.");
}

#[test]
fn match_key() {
    if let Some(captures) = super::init_regex().captures("$key") {
        let kind = captures.get(1).expect("Missing capture.");
        assert!(captures.get(2).is_none());
        assert_eq!(kind.as_str(), "$key");
    } else {
        panic!("Did not match.");
    }
}

#[test]
fn match_value() {
    if let Some(captures) = super::init_regex().captures("$value") {
        let kind = captures.get(1).expect("Missing capture.");
        assert!(captures.get(2).is_none());
        assert_eq!(kind.as_str(), "$value");
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
fn match_value_indexed() {
    if let Some(captures) = super::init_regex().captures("$value[0]") {
        let kind = captures.get(1).expect("Missing capture.");
        let index = captures.get(2).expect("Missing capture.");
        assert_eq!(kind.as_str(), "$value");
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

    let value = super::parse_selector("$value").expect("Parse failed.");
    assert_eq!(
        value,
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
        Err(BadSelector::InvalidRoot)
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

    let second = super::parse_selector("$value.slot").expect("Parse failed.");
    let mut expected_second = SelectorDescriptor::for_part(MessagePart::Payload, None);
    expected_second.push(SelectorComponent::new(false, "slot", None));
    assert_eq!(second, expected_second);

    let third = super::parse_selector("$key.@attr[3]").expect("Parse failed.");
    let mut expected_third = SelectorDescriptor::for_part(MessagePart::Key, None);
    expected_third.push(SelectorComponent::new(true, "attr", Some(3)));
    assert_eq!(third, expected_third);

    let fourth = super::parse_selector("$value[6].slot[8]").expect("Parse failed.");
    let mut expected_fourth = SelectorDescriptor::for_part(MessagePart::Payload, Some(6));
    expected_fourth.push(SelectorComponent::new(false, "slot", Some(8)));
    assert_eq!(fourth, expected_fourth);
}

#[test]
fn multi_component_selector() {
    let selector = super::parse_selector("$value.red.@green[7].blue").expect("Parse failed.");
    let mut expected = SelectorDescriptor::for_part(MessagePart::Payload, None);
    expected.push(SelectorComponent::new(false, "red", None));
    expected.push(SelectorComponent::new(true, "green", Some(7)));
    expected.push(SelectorComponent::new(false, "blue", None));
    assert_eq!(selector, expected);
}
