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
        common::{Bound, Segment, Selector, SelectorKind},
        payload::parse_pattern,
    },
    PayloadSelectorPattern,
};

fn test(pattern_str: &str, expected_segment: Segment) {
    match parse_pattern(pattern_str.into()) {
        Ok(PayloadSelectorPattern {
            segment: actual_segment,
            ..
        }) => {
            assert_eq!(actual_segment, expected_segment)
        }
        Err(e) => panic!("Failed to parse pattern {}: {:?}", pattern_str, e),
    }
}

#[test]
fn selector() {
    test(
        "$key",
        Segment::Selector(Selector {
            kind: SelectorKind::Key,
            parts: vec![],
        }),
    );
    test(
        "$value.field.a.b.c",
        Segment::Selector(Selector {
            kind: SelectorKind::Value,
            parts: vec![
                Bound { start: 7, end: 12 },
                Bound { start: 13, end: 14 },
                Bound { start: 15, end: 16 },
                Bound { start: 17, end: 18 },
            ],
        }),
    );
}

#[test]
fn statics() {
    test("path", Segment::Static(Bound { start: 0, end: 4 }));
    test("12345", Segment::Static(Bound { start: 0, end: 5 }));
}
