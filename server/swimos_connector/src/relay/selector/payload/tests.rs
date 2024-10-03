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

use crate::relay::selector::{
    common::{Segment, Selector, SelectorKind},
    common::{SelectorBound, StaticBound},
    payload::parse_pattern,
};

fn test(pattern_str: &str, expected_segment: Segment) {
    match parse_pattern(pattern_str.into()) {
        Ok(actual_segment) => {
            assert_eq!(actual_segment, expected_segment)
        }
        Err(e) => panic!("Failed to parse pattern {}: {:?}", pattern_str, e),
    }
}

#[test]
fn selector() {
    test(
        "$key",
        Segment::Selector(Selector::Property {
            kind: SelectorKind::Key,
            parts: vec![],
        }),
    );
    test("$topic", Segment::Selector(Selector::Topic));
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
    test(
        "$key.@attr",
        Segment::Selector(Selector::Property {
            kind: SelectorKind::Key,
            parts: vec![SelectorBound {
                start: 6,
                end: 10,
                attr: true,
                index: None,
            }],
        }),
    );
    test(
        "$key.@attr[1]",
        Segment::Selector(Selector::Property {
            kind: SelectorKind::Key,
            parts: vec![SelectorBound {
                start: 6,
                end: 10,
                attr: true,
                index: Some(1),
            }],
        }),
    );
}

#[test]
fn bad_patterns() {
    assert!(parse_pattern("$key.field[".into()).is_err());
    assert!(parse_pattern("$key.field]".into()).is_err());
    assert!(parse_pattern("$key.field[]".into()).is_err());
    assert!(parse_pattern("$key.field[".into()).is_err());
    assert!(parse_pattern("$key.@field[".into()).is_err());
    assert!(parse_pattern("$key.@field]".into()).is_err());
    assert!(parse_pattern("$key.@field[]".into()).is_err());
    assert!(parse_pattern("$key.@field[".into()).is_err());
    assert!(parse_pattern("$key.@field[$.".into()).is_err());
    assert!(parse_pattern("$key.[$.".into()).is_err());
    assert!(parse_pattern("$key.[.".into()).is_err());
    assert!(parse_pattern("$key.]$.".into()).is_err());
    assert!(parse_pattern("$key.[]".into()).is_err());
}

#[test]
fn statics() {
    test("path", Segment::Static(StaticBound { start: 0, end: 4 }));
    test("12345", Segment::Static(StaticBound { start: 0, end: 5 }));
}
