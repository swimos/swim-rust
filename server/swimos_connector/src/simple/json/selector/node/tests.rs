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
        node::parse_pattern,
        ParseError, Part,
    },
    NodeSelectorPattern,
};

fn test(pattern_str: &str, expected_scheme: Option<Bound>, expected_segments: Vec<Segment>) {
    match parse_pattern(pattern_str.into()) {
        Ok(NodeSelectorPattern {
            scheme: actual_scheme,
            segments: actual_segments,
            ..
        }) => {
            assert_eq!(expected_scheme, actual_scheme);
            assert_eq!(actual_segments, expected_segments)
        }
        Err(e) => panic!("Failed to parse pattern {}: {:?}", pattern_str, e),
    }
}

#[test]
fn keys() {
    test(
        "/$key",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
        ],
    );
    test(
        "/$key/$value/$value.field.a.b.c",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 5, end: 6 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 12, end: 13 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![
                    Bound { start: 20, end: 25 },
                    Bound { start: 26, end: 27 },
                    Bound { start: 28, end: 29 },
                    Bound { start: 30, end: 31 },
                ],
            }),
        ],
    );
}

#[test]
fn values() {
    test(
        "/$value",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![],
            }),
        ],
    );
    test(
        "/$value.blah.blah",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![
                    Bound { start: 8, end: 12 },  // "blah"
                    Bound { start: 13, end: 17 }, // "blah"
                ],
            }),
        ],
    );
    test(
        "/$value.field.a.b.c",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![
                    Bound { start: 8, end: 13 },
                    Bound { start: 14, end: 15 },
                    Bound { start: 16, end: 17 },
                    Bound { start: 18, end: 19 },
                ],
            }),
        ],
    );
}

#[test]
fn escaped() {
    test(
        "/$key$_$value",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 6, end: 7 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![],
            }),
        ],
    );
    test(
        "/$key$_value",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 6, end: 12 }),
        ],
    );
}

#[test]
fn mixed() {
    test(
        "/$value/$key.field$value.field",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 7, end: 8 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![Bound { start: 13, end: 18 }], // "field"
            }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![Bound { start: 25, end: 30 }], // "field"
            }),
        ],
    );
    test(
        "/$value/$key.field_a$value.field_b",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 7, end: 8 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![Bound { start: 13, end: 20 }], // "field_a"
            }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![Bound { start: 27, end: 34 }], // "field_b"
            }),
        ],
    );
    test(
        "/$value/$key.field_a$_$value.field_b/static_path",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 7, end: 8 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![Bound { start: 13, end: 20 }], // "field_a"
            }),
            Segment::Static(Bound { start: 21, end: 22 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![Bound { start: 29, end: 36 }], // "field_b"
            }),
            Segment::Static(Bound { start: 36, end: 48 }),
        ],
    );
}

#[test]
fn statics() {
    test(
        "/path",
        None,
        vec![Segment::Static(Bound { start: 0, end: 5 })],
    );
    test(
        "/a/b/c",
        None,
        vec![Segment::Static(Bound { start: 0, end: 6 })],
    );
    test(
        "/a/$key/c",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 3 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 7, end: 9 }),
        ],
    );
}

#[test]
fn invalid_double_slash() {
    assert_eq!(parse_pattern("/home//profile".into()), Err(ParseError(7)));
}

#[test]
fn invalid_double_slash_with_scheme() {
    assert_eq!(
        parse_pattern("http:/home//profile".into()),
        Err(ParseError(12))
    );
}

#[test]
fn no_leading_slash() {
    assert_eq!(parse_pattern("home".into()), Err(ParseError(0)));
}

#[test]
fn trailing_slash() {
    test(
        "/home/",
        None,
        vec![Segment::Static(Bound { start: 0, end: 6 })],
    );
}

#[test]
fn scheme_with_static_route() {
    test(
        "node:/blah",
        Some(Bound { start: 0, end: 4 }), // "node"
        vec![Segment::Static(Bound { start: 5, end: 10 })],
    );
}

#[test]
fn scheme_with_selector_route() {
    test(
        "swim:/$key",
        Some(Bound { start: 0, end: 4 }), // "swim"
        vec![
            Segment::Static(Bound { start: 5, end: 6 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
        ],
    );
}

#[test]
fn scheme_with_static_and_selector() {
    test(
        "swim:/path/$key",
        Some(Bound { start: 0, end: 4 }), // "swim"
        vec![
            Segment::Static(Bound { start: 5, end: 11 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
        ],
    );
}

#[test]
fn selector_with_special_characters() {
    test(
        "/$key/$value.id123",
        None,
        vec![
            Segment::Static(Bound { start: 0, end: 1 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Key,
                parts: vec![],
            }),
            Segment::Static(Bound { start: 5, end: 6 }),
            Segment::Selector(Selector {
                kind: SelectorKind::Value,
                parts: vec![Bound { start: 13, end: 18 }], // "id123"
            }),
        ],
    );
}

#[test]
fn bad_patterns() {
    assert!(parse_pattern("/$".into()).is_err());
    assert!(parse_pattern("swim:/$".into()).is_err());
    assert!(parse_pattern("swim:/".into()).is_err());
    assert!(parse_pattern("swim:".into()).is_err());
    assert!(parse_pattern("swim".into()).is_err());
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
    assert!(parse_pattern("$blah".into()).is_err());
    assert!(parse_pattern("$key.$value".into()).is_err());
    assert!(parse_pattern("$key/::$value".into()).is_err());
}

#[test]
fn display() {
    fn display_eq(i: &str) {
        let out = parse_pattern(Span::new(i)).expect("Parse error");
        assert_eq!(i, out.to_string());
    }

    display_eq("/$value/$key.field_a$value.field_b");
    display_eq("/value/$key.field_a$value.field_b");
    display_eq("/value/$key.field_a/$value.field_b");
    display_eq("/value/key/$value.field_b");
    display_eq("/$value/$key.field_a$value.field_b/aaa$value");
    display_eq("/$key$_value");
}

#[test]
fn iter_nested() {
    let route =
        parse_pattern(Span::new("/$value/$key.field_a$value.field_b")).expect("Failed to parse");
    let mut iter = route.into_iter();

    assert_eq!(iter.next(), Some(Part::Static("/")));
    assert_eq!(iter.next(), Some(Part::ValueSelector(vec![])));
    assert_eq!(iter.next(), Some(Part::Static("/")));
    assert_eq!(iter.next(), Some(Part::KeySelector(vec!["field_a"])));
    assert_eq!(iter.next(), Some(Part::ValueSelector(vec!["field_b"])));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
}

#[test]
fn iter_static() {
    let route = parse_pattern(Span::new("/a/b/c_ddd/e")).expect("Failed to parse");
    let mut iter = route.into_iter();

    assert_eq!(iter.next(), Some(Part::Static("/a/b/c_ddd/e")));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
}
