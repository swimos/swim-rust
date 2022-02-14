// Copyright 2015-2022 Swim Inc.
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

use crate::hasher::calculate_hash;
use crate::parser::{ParseError, Span};
use std::collections::hash_map::DefaultHasher;
use swim_model::Value;

fn value_from_string(rep: &str) -> Result<Value, ParseError> {
    let span = Span::new(rep);
    crate::parser::parse_recognize(span, false)
}

fn cmp_eq(first: &str, second: &str) {
    assert_eq!(
        calculate_hash(first, DefaultHasher::new()).unwrap(),
        calculate_hash(second, DefaultHasher::new()).unwrap()
    );

    let result_1 = value_from_string(first).unwrap();
    let result_2 = value_from_string(second).unwrap();
    assert_eq!(result_1, result_2);
}

fn cmp_ne(first: &str, second: &str) {
    assert_ne!(
        calculate_hash(first, DefaultHasher::new()).unwrap(),
        calculate_hash(second, DefaultHasher::new()).unwrap()
    );

    let result_1 = value_from_string(first).unwrap();
    let result_2 = value_from_string(second).unwrap();
    assert_ne!(result_1, result_2);
}

#[test]
fn recon_hash_eq() {
    let first = "@name(a: 1, b: 2)";
    let second = "@name({a: 1, b: 2})";
    cmp_eq(first, second);

    let first = "@foo({a: 1})";
    let second = "@foo(a: 1)";
    cmp_eq(first, second);

    let first = "@foo(a:)";
    let second = "@foo({a: })";
    cmp_eq(first, second);

    let first = "@name({a: 1, b: 2}, 3)";
    let second = "@name({{a: 1, b: 2}, 3})";
    cmp_eq(first, second);

    let first = "@name(3, {a: 1, b: 2})";
    let second = "@name({3, {a: 1, b: 2}})";
    cmp_eq(first, second);

    let first = "@attr({{1,2},{3,4}})";
    let second = "@attr({1,2},{3,4})";
    cmp_eq(first, second);

    let first = "@attr({1,2,3,4})";
    let second = "@attr(1,2,3,4)";
    cmp_eq(first, second);

    let first = "@foo({one: 1, two: @bar(1,2,3), three: 3, four: {@baz({1,2})}})";
    let second = "@foo(one: 1, two: @bar({1,2,3}), three: 3, four: {@baz(1,2)})";
    cmp_eq(first, second);

    let first = "@name(a: 1, b: 2)";
    let second = "@name(a: 1, b: 2)";
    cmp_eq(first, second);

    let first = "\"test\"";
    let second = "test";
    cmp_eq(first, second);

    let first = "{a:2}";
    let second = "{ a: 2 }";
    cmp_eq(first, second);

    let first = "@tag(){}:1";
    let second = "@tag{}:1";
    cmp_eq(first, second);

    let first = "@name(a: 1, b: 2)";
    let second = "@name({a: 1, b: 2})";
    cmp_eq(first, second);

    let first = "@first(1)@second(2)";
    let second = "@first(1)@second(2) {}";
    cmp_eq(first, second);

    let first = "{ @inner(0), after }";
    let second = "{ @inner(0) {}, after }";
    cmp_eq(first, second);

    let first = "@outer(@inner)";
    let second = "@outer(@inner {})";
    cmp_eq(first, second);

    let first = "@foo({one: 1, two: @bar(1,2,3), three: 3, four: {@baz({1,2})}})";
    let second = "@foo(one: 1, two: @bar({1,2,3}), three: 3, four: {@baz(1,2)})";
    cmp_eq(first, second);

    let first = "@foo(1,2)";
    let second = "@foo({1,2})";
    cmp_eq(first, second);

    let first = "@foo({one: 1, two: @bar(1,2,3)})";
    let second = "@foo(one: 1, two: @bar({1,2,3}))";
    cmp_eq(first, second);

    let first = "@name(@foo,@bar)";
    let second = "@name({@foo, @bar})";
    cmp_eq(first, second);

    let first = "@name(1, @foo@bar)";
    let second = "@name({1, @foo@bar})";
    cmp_eq(first, second);

    let first = "@name(@foo@bar({@bar, 1, @baz()}), @bar@foo()@baz)";
    let second = "@name({@foo@bar(@bar, 1, @baz()), @bar@foo()@baz})";
    cmp_eq(first, second);

    let first = "@foo(1, @bar(2,3), 4, 5)";
    let second = "@foo(1, @bar({2,3}), 4, 5)";
    cmp_eq(first, second);

    let first = "@name(a: @foo)";
    let second = "@name({a: @foo})";
    cmp_eq(first, second);

    let first = "@name(b: @foo@bar@baz, @foo(1,2), @bar@baz))";
    let second = "@name({b: @foo@bar@baz, @foo({1,2}), @bar@baz})";
    cmp_eq(first, second);

    let first = "@name(b: )";
    let second = "@name({b: })";
    cmp_eq(first, second);
}

#[test]
fn recon_hash_not_eq() {
    let first = "@foo({1})";
    let second = "@name({a: 1, b: 2})";
    cmp_ne(first, second);

    let first = "@foo({})";
    let second = "@foo()";
    cmp_ne(first, second);

    let first = "@attr({{}})";
    let second = "@attr({})";
    cmp_ne(first, second);

    let first = "@foo({1})";
    let second = "@foo(1)";
    cmp_ne(first, second);

    let first = "@name(@foo@bar)";
    let second = "@name({@foo@bar})";
    cmp_ne(first, second);

    let first = "@foo(@bar {})";
    let second = "@foo(@bar, {})";
    cmp_ne(first, second);

    let first = "@name(a: 1, b: 2, c: 3)";
    let second = "@name(a:1, b: 4, c: 3)";
    cmp_ne(first, second);

    let first = "@foo({{1,2}})";
    let second = "@foo({1, 2})";
    cmp_ne(first, second);

    let first = "@name(a: 1, b: 2)";
    let second = "@name(   {a: 3, b: 2}    )";
    cmp_ne(first, second);

    let first = "@foo(1)";
    let second = "@foo({1})";
    cmp_ne(first, second);

    let first = "@foo()";
    let second = "@foo({})";
    cmp_ne(first, second);

    let first = "@foo(@bar@baz) ";
    let second = "@foo({@bar@baz})";
    cmp_ne(first, second);

    let first = "@name({1,2},{3,4})";
    let second = "@name({1,2,3,4})";
    cmp_ne(first, second);

    let first = "@foo(1, @bar(2), 3, 4)";
    let second = "@foo(1, @bar({2}), 3, 4)";
    cmp_ne(first, second);
}
