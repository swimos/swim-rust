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

use crate::simple::json::{
    selector::{
        common::{Bound, Segment, Selector, SelectorKind, Span},
        lane::parse_pattern,
        Part,
    },
    LaneSelectorPattern,
};

fn test(pattern_str: &str, expected_segments: Vec<Segment>) {
    match parse_pattern(pattern_str.into()) {
        Ok(LaneSelectorPattern {
               segments: actual_segments,
               ..
           }) => {
            assert_eq!(actual_segments, expected_segments)
        }
        Err(e) => panic!("Failed to parse pattern {}: {:?}", pattern_str, e),
    }
}

#[test]
fn values() {
    test(
        "$value",
        vec![Segment::Selector(Selector {
            kind: SelectorKind::Value,
            parts: vec![],
        })],
    );
    test(
        "$value.blah.blah",
        vec![Segment::Selector(Selector {
            kind: SelectorKind::Value,
            parts: vec![
                Bound { start: 7, end: 11 },  // "blah"
                Bound { start: 12, end: 16 }, // "blah"
            ],
        })],
    );
    test(
        "$value.field.a.b.c",
        vec![Segment::Selector(Selector {
            kind: SelectorKind::Value,
            parts: vec![
                Bound { start: 7, end: 12 },
                Bound { start: 13, end: 14 },
                Bound { start: 15, end: 16 },
                Bound { start: 17, end: 18 },
            ],
        })],
    );
}

#[test]
fn escaped() {
    test(
        "$key$_$value",
        vec![
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 5, end: 6 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![],
            }),
        ],
    );
    test(
        "$key$_value",
        vec![
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 5, end: 11 }),
        ],
    );
}

#[test]
fn statics() {
    test("path", vec![Segment::Static(Bound { start: 0, end: 4 })]);
    test("1234", vec![Segment::Static(Bound { start: 0, end: 4 })]);
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
}

#[test]
fn iter() {
    let route = parse_pattern(Span::new("$key$_$value.field.a")).expect("Failed to parse");
    let mut iter = route.into_iter();

    assert_eq!(iter.next(), Some(Part::KeySelector(vec![])));
    assert_eq!(iter.next(), Some(Part::Static("_")));
    assert_eq!(iter.next(), Some(Part::ValueSelector(vec!["field", "a"])));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
}
