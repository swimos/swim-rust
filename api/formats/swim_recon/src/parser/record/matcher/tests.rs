// Copyright 2015-2021 Swim Inc.
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

use nom::character::complete::multispace0;
use nom::combinator::eof;
use nom::sequence::{delimited, preceded};

use nom::IResult;

use crate::parser::Span;

use super::HeaderPeeler;

#[derive(Clone, Debug, Default)]
struct TestPeeler<'a> {
    name: Option<String>,
    slots: Vec<(String, &'a str)>,
    values: Vec<Option<&'a str>>,
    body: Option<&'a str>,
}

#[derive(Clone)]
struct Inconsistent;

impl<'a> HeaderPeeler<'a> for TestPeeler<'a> {
    type Output = Self;

    type Error = Inconsistent;

    fn tag(mut self, name_str: &str) -> Result<Self, Self::Error> {
        let TestPeeler { name, .. } = &mut self;
        if name.is_some() {
            Err(Inconsistent)
        } else {
            *name = Some(name_str.to_string());
            Ok(self)
        }
    }

    fn feed_header_slot(mut self, name: &str, value: Span<'a>) -> Result<Self, Self::Error> {
        let TestPeeler { slots, .. } = &mut self;
        slots.push((name.to_string(), *value));
        Ok(self)
    }

    fn feed_header_value(mut self, value: Span<'a>) -> Result<Self, Self::Error> {
        let TestPeeler { values, .. } = &mut self;
        values.push(Some(*value));
        Ok(self)
    }

    fn feed_header_extant(mut self) -> Result<Self, Self::Error> {
        let TestPeeler { values, .. } = &mut self;
        values.push(None);
        Ok(self)
    }

    fn done(mut self, body_span: Span<'a>) -> Result<Self::Output, Self::Error> {
        let TestPeeler { body, .. } = &mut self;
        if body.is_some() {
            Err(Inconsistent)
        } else {
            *body = Some(*body_span);
            Ok(self)
        }
    }
}

const RECON_DOCS: &[&str] = &[
    "ident",
    "\"two words\"",
    "5",
    "0.5",
    "true",
    "%YW55IGNhcm5hbCBwbGVhc3Vy",
    "{}",
    "@attr",
    "@\"two words\"",
    "@attr()",
    "@attr {}",
    "@attr() {}",
    "@attr1@attr2",
    "@attr1@attr2 {}",
    "@attr1()@attr2() {}",
    "@attr(0)",
    "@attr(a:0)",
    "@attr(0, 1, 2)",
    "@attr(0; 1; 2)",
    "@attr(a:0, b:1, c:2)",
    "@attr(a:0; b:1; c:2)",
    "@attr(a:0\n b:1\n c:2)",
    "@attr(\na:0\n b:1\n c:2\n)",
    "@attr(,)",
    "@attr(,,)",
    "@attr(\n,\n,\n)",
    "{ 0 }",
    "{ a:0 }",
    "{ 0, 1, 2 }",
    "{ 0; 1; 2 }",
    "{ a:0, b:1, c:2 }",
    "{ a:0; b:1; c:2 }",
    "{ a:0\n b:1\n c:2 }",
    "{\na:0\n b:1\n c:2\n}",
    "{,}",
    "{,,}",
    "{\n,\n,\n}",
    "@attr(@inner)",
    "@attr(@inner{})",
    "{ @attr }",
    "{ @attr, @attr(@inner) }",
    "{{}}",
    "{{{}}}",
    "{{},{}}",
    "@attr 4",
    "@attr() 4",
];

fn complete_value(input: Span<'_>) -> IResult<Span<'_>, Span<'_>> {
    delimited(multispace0, super::value, preceded(multispace0, eof))(input)
}

#[test]
fn recognize_values() {
    for doc in RECON_DOCS {
        let span = Span::new(*doc);
        if let Ok((_, parsed)) = complete_value(span) {
            assert_eq!(parsed, span);
        } else {
            panic!("Parse failed.");
        }
    }
}

#[test]
fn peel_tag() {
    let msg = Span::new("@attr");
    let result = super::peel_message(TestPeeler::default())(msg);
    assert!(result.is_ok());
    let (
        _,
        TestPeeler {
            name,
            slots,
            values,
            body,
        },
    ) = result.unwrap();

    assert_eq!(name, Some("attr".to_string()));
    assert!(slots.is_empty());
    assert!(values.is_empty());
    assert_eq!(body, Some(""));
}

#[test]
fn peel_tag_empty_body() {
    let msg = Span::new("@attr()");
    let result = super::peel_message(TestPeeler::default())(msg);

    assert!(result.is_ok());
    let (
        _,
        TestPeeler {
            name,
            slots,
            values,
            body,
        },
    ) = result.unwrap();

    assert_eq!(name, Some("attr".to_string()));
    assert!(slots.is_empty());
    assert_eq!(values, vec![None]);
    assert_eq!(body, Some(""));
}

#[test]
fn peel_tag_simple_body() {
    let msg = Span::new("@attr(5)");
    let result = super::peel_message(TestPeeler::default())(msg);

    assert!(result.is_ok());
    let (
        _,
        TestPeeler {
            name,
            slots,
            values,
            body,
        },
    ) = result.unwrap();

    assert_eq!(name, Some("attr".to_string()));
    assert!(slots.is_empty());

    assert_eq!(values, vec![Some("5")]);
    assert_eq!(body, Some(""));
}

#[test]
fn peel_tag_several_value_items() {
    let check = |msg, expected| {
        let result = super::peel_message(TestPeeler::default())(msg);

        assert!(result.is_ok());
        let (
            _,
            TestPeeler {
                name,
                slots,
                values,
                body,
            },
        ) = result.unwrap();

        assert_eq!(name, Some("attr".to_string()));
        assert!(slots.is_empty());

        assert_eq!(values, expected);
        assert_eq!(body, Some(""));
    };

    check(
        Span::new("@attr(1,2,3)"),
        vec![Some("1"), Some("2"), Some("3")],
    );
    check(
        Span::new("@attr( 1, 2, 3 )"),
        vec![Some("1"), Some("2"), Some("3")],
    );
    check(
        Span::new("@attr(1;2;3)"),
        vec![Some("1"), Some("2"), Some("3")],
    );
    check(
        Span::new("@attr(1\n2\n3)"),
        vec![Some("1"), Some("2"), Some("3")],
    );
    check(
        Span::new("@attr(\n1\n2\n3\n)"),
        vec![Some("1"), Some("2"), Some("3")],
    );
    check(Span::new("@attr(1,,3)"), vec![Some("1"), None, Some("3")]);
    check(Span::new("@attr(,2,3)"), vec![None, Some("2"), Some("3")]);
    check(Span::new("@attr(1,2,)"), vec![Some("1"), Some("2"), None]);
}

#[test]
fn peel_tag_single_slot() {
    let msg = Span::new("@attr(name : \"bob\")");
    let result = super::peel_message(TestPeeler::default())(msg);

    assert!(result.is_ok());
    let (
        _,
        TestPeeler {
            name,
            slots,
            values,
            body,
        },
    ) = result.unwrap();

    assert_eq!(name, Some("attr".to_string()));
    assert_eq!(slots, vec![("name".to_string(), "\"bob\"")]);

    assert!(values.is_empty());
    assert_eq!(body, Some(""));
}

#[test]
fn peel_tag_several_slots() {
    let check = |msg, expected_slots, expected_values| {
        let result = super::peel_message(TestPeeler::default())(msg);

        assert!(result.is_ok());
        let (
            _,
            TestPeeler {
                name,
                slots,
                values,
                body,
            },
        ) = result.unwrap();

        assert_eq!(name, Some("attr".to_string()));
        assert_eq!(slots, expected_slots);

        assert_eq!(values, expected_values);
        assert_eq!(body, Some(""));
    };

    check(
        Span::new("@attr(first:1,second:2,third:3)"),
        vec![
            ("first".to_string(), "1"),
            ("second".to_string(), "2"),
            ("third".to_string(), "3"),
        ],
        vec![],
    );
    check(
        Span::new("@attr( first:1, second:2, third:3 )"),
        vec![
            ("first".to_string(), "1"),
            ("second".to_string(), "2"),
            ("third".to_string(), "3"),
        ],
        vec![],
    );
    check(
        Span::new("@attr( first: 1; second: 2; third: 3 )"),
        vec![
            ("first".to_string(), "1"),
            ("second".to_string(), "2"),
            ("third".to_string(), "3"),
        ],
        vec![],
    );
    check(
        Span::new("@attr( first: 1\n second: 2\n third: 3 )"),
        vec![
            ("first".to_string(), "1"),
            ("second".to_string(), "2"),
            ("third".to_string(), "3"),
        ],
        vec![],
    );
    check(
        Span::new("@attr(\nfirst: 1\n second: 2\n third: 3\n)"),
        vec![
            ("first".to_string(), "1"),
            ("second".to_string(), "2"),
            ("third".to_string(), "3"),
        ],
        vec![],
    );
    check(
        Span::new("@attr( first:1, , third:3 )"),
        vec![("first".to_string(), "1"), ("third".to_string(), "3")],
        vec![None],
    );
    check(
        Span::new("@attr( , second:2 , third:3 )"),
        vec![("second".to_string(), "2"), ("third".to_string(), "3")],
        vec![None],
    );
    check(
        Span::new("@attr( first:1, second:2, )"),
        vec![("first".to_string(), "1"), ("second".to_string(), "2")],
        vec![None],
    );
}

#[test]
fn peel_body() {
    let check = |msg, expected_body| {
        let result = super::peel_message(TestPeeler::default())(msg);
        assert!(result.is_ok());
        let (
            _,
            TestPeeler {
                name,
                slots,
                values,
                body,
            },
        ) = result.unwrap();

        assert_eq!(name, Some("attr".to_string()));
        assert!(slots.is_empty());

        assert_eq!(values, vec![Some("0")]);
        assert_eq!(body, Some(expected_body));
    };

    check(Span::new("@attr(0) {}"), "{}");
    check(Span::new("@attr(0) 1"), "1");
    check(Span::new("@attr(0)@attr2 {}"), "@attr2 {}");
    check(Span::new("@attr(0)@attr2 {\na:1\n}"), "@attr2 {\na:1\n}");
}

#[test]
fn peel_complex() {
    let msg = Span::new(
        "@attr(@inner { 2, 7 }, key: @compound(5) { first: 1, second: name}) @other(1) {1,2,3}",
    );
    let result = super::peel_message(TestPeeler::default())(msg);

    assert!(result.is_ok());
    let (
        _,
        TestPeeler {
            name,
            slots,
            values,
            body,
        },
    ) = result.unwrap();

    assert_eq!(name, Some("attr".to_string()));
    assert_eq!(
        slots,
        vec![("key".to_string(), "@compound(5) { first: 1, second: name}")]
    );

    assert_eq!(values, vec![Some("@inner { 2, 7 }")]);
    assert_eq!(body, Some("@other(1) {1,2,3}"));
}
