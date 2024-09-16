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

use crate::connectors::relay::selector::{
    common::{Segment, Selector, SelectorBound, SelectorKind, Span, StaticBound},
    lane::parse_pattern,
    Part,
};
use crate::relay::LaneSelector;
use crate::selector::{BasicSelector, ChainSelector, SlotSelector, ValueSelector};

fn test(pattern_str: &str, expected_segment: Segment) {
    match parse_pattern(pattern_str.into()) {
        Ok(LaneSelector {
            segment: actual_segment,
            ..
        }) => {
            assert_eq!(actual_segment, expected_segment)
        }
        Err(e) => panic!("Failed to parse pattern {}: {:?}", pattern_str, e),
    }
}

#[test]
fn values() {
    test(
        "$value",
        Segment::Selector(Selector::Property {
            kind: SelectorKind::Value,
            parts: vec![],
        }),
    );
    test(
        "$value.blah.blah",
        Segment::Selector(Selector::Property {
            kind: SelectorKind::Value,
            parts: vec![
                SelectorBound {
                    start: 7,
                    end: 11,
                    attr: false,
                    index: None,
                }, // "blah"
                SelectorBound {
                    start: 12,
                    end: 16,
                    attr: false,
                    index: None,
                }, // "blah"
            ],
        }),
    );
    test(
        "$value.field.a.b.c",
        Segment::Selector(Selector::Property {
            kind: SelectorKind::Value,
            parts: vec![
                SelectorBound {
                    start: 7,
                    end: 12,
                    attr: false,
                    index: None,
                },
                SelectorBound {
                    start: 13,
                    end: 14,
                    attr: false,
                    index: None,
                },
                SelectorBound {
                    start: 15,
                    end: 16,
                    attr: false,
                    index: None,
                },
                SelectorBound {
                    start: 17,
                    end: 18,
                    attr: false,
                    index: None,
                },
            ],
        }),
    );
}

#[test]
fn topic() {
    test("$topic", Segment::Selector(Selector::Topic));
}

#[test]
fn statics() {
    test("path", Segment::Static(StaticBound { start: 0, end: 4 }));
    test("1234", Segment::Static(StaticBound { start: 0, end: 4 }));
}

#[test]
fn bad_patterns() {
    assert!(parse_pattern("/$".into()).is_err());
    assert!(parse_pattern("/::".into()).is_err());
    assert!(parse_pattern("/".into()).is_err());
    assert!(parse_pattern("//".into()).is_err());
    assert!(parse_pattern("//aaa".into()).is_err());
    assert!(parse_pattern("::/".into()).is_err());
    assert!(parse_pattern(":/".into()).is_err());
    assert!(parse_pattern("".into()).is_err());
    assert!(parse_pattern(":".into()).is_err());
    assert!(parse_pattern("$".into()).is_err());
    assert!(parse_pattern("~".into()).is_err());
    assert!(parse_pattern("$key.$value".into()).is_err());
    assert!(parse_pattern("$key/::$value".into()).is_err());
    assert!(parse_pattern("blah/blah".into()).is_err());
    assert!(parse_pattern("$key/$value".into()).is_err());
    assert!(parse_pattern("$key.field/blah".into()).is_err());
    assert!(parse_pattern("blah/$key".into()).is_err());
}

#[test]
fn iter() {
    let route = parse_pattern(Span::new("$value.field.a")).expect("Failed to parse");
    let mut iter = route.into_iter();

    assert_eq!(
        iter.next(),
        Some(Part::Selector(ValueSelector::Payload(ChainSelector::new(
            vec![
                BasicSelector::Slot(SlotSelector::for_field("field")),
                BasicSelector::Slot(SlotSelector::for_field("a"))
            ]
        ))))
    );
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
}
