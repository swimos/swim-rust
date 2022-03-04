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

use nom::sequence::{delimited, preceded};
use nom::character::complete::multispace0;
use nom::combinator::eof;

use nom::IResult;

use crate::parser::Span;

use super::HeaderPeeler;

#[derive(Clone, Debug)]
struct TestPeeler<'a> {
    name: Option<String>,
    slots: Vec<(String, Span<'a>)>,
    values: Vec<Option<Span<'a>>>,
    body: Option<Span<'a>>,
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
        slots.push((name.to_string(), value));
        Ok(self)
    }

    fn feed_header_value(mut self, value: Span<'a>) -> Result<Self, Self::Error> {
        let TestPeeler { values, .. } = &mut self;
        values.push(Some(value));
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
            *body = Some(body_span);
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
