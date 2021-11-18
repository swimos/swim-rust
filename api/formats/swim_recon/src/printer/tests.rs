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

use swim_model::{Attr, Item, Value};

fn print_value(v: &Value) -> String {
    format!("{}", super::print_recon(v))
}

#[test]
fn primitive_values() {
    assert_eq!(print_value(&Value::Extant), "");
    assert_eq!(print_value(&Value::Int32Value(1)), "1");
    assert_eq!(
        print_value(&Value::Int64Value(-10000000000)),
        "-10000000000"
    );
    assert_eq!(print_value(&Value::text("hello")), "hello");
    assert_eq!(print_value(&Value::text("two words")), "\"two words\"");
    assert_eq!(print_value(&Value::BooleanValue(true)), "true");
    assert_eq!(print_value(&Value::Float64Value(0.0)), "0.0");
}

#[test]
fn simple_no_attribute_records() {
    assert_eq!(print_value(&Value::empty_record()), "{}");

    let single_value = Value::from_vec(vec![Item::of(1)]);
    let single_slot = Value::from_vec(vec![Item::of(("name", 1))]);

    assert_eq!(print_value(&single_value), "{ 1 }");
    assert_eq!(print_value(&single_slot), "{ name: 1 }");

    let multiple_values = Value::from_vec(vec![1, 2, 3]);
    assert_eq!(print_value(&multiple_values), "{ 1, 2, 3 }");

    let multiple_slots = Value::from_vec(vec![
        Item::of(("first", 1)),
        Item::of(("second", 2)),
        Item::of(("third", 3)),
    ]);
    assert_eq!(
        print_value(&multiple_slots),
        "{ first: 1, second: 2, third: 3 }"
    );
}

#[test]
fn simple_attributes() {
    let basic = Value::of_attr("tag");
    assert_eq!(print_value(&basic), "@tag");

    let simple_value = Value::of_attr(("tag", 1));
    assert_eq!(print_value(&simple_value), "@tag(1)");

    let empty_value = Value::of_attr(("tag", Value::empty_record()));
    assert_eq!(print_value(&empty_value), "@tag({})");

    let single_value = Value::of_attr(("tag", Value::from_vec(vec![1])));
    assert_eq!(print_value(&single_value), "@tag({ 1 })");

    let single_slot = Value::of_attr(("tag", Value::from_vec(vec![("name", 1)])));
    assert_eq!(print_value(&single_slot), "@tag(name: 1)");

    let multiple_values = Value::of_attr(("tag", Value::from_vec(vec![1, 2, 3])));
    assert_eq!(print_value(&multiple_values), "@tag(1, 2, 3)");

    let multiple_items = Value::of_attr((
        "tag",
        Value::from_vec(vec![Item::of(1), Item::slot("slot", 2), Item::of(3)]),
    ));
    assert_eq!(print_value(&multiple_items), "@tag(1, slot: 2, 3)");
}

#[test]
fn nested_no_attribute_records() {
    let inner = Value::from_vec(vec![1, 2, 3]);

    let single_nested = Value::from_vec(vec![Item::of(inner.clone())]);
    let single_slot_nested = Value::from_vec(vec![Item::of(("name", inner.clone()))]);

    assert_eq!(print_value(&single_nested), "{ { 1, 2, 3 } }");
    assert_eq!(print_value(&single_slot_nested), "{ name: { 1, 2, 3 } }");

    let multiple_values_nested = Value::from_vec(vec![Item::of(1), Item::of(2), Item::of(inner)]);
    assert_eq!(
        print_value(&multiple_values_nested),
        "{ 1, 2, { 1, 2, 3 } }"
    );
}

#[test]
fn complete_records() {
    let first = Attr::of("first");
    let second = Attr::of(("second", 1));

    let items = vec![Item::of(1), Item::slot("name", 2), Item::of(true)];

    let rec = Value::Record(vec![first.clone()], items.clone());
    assert_eq!(print_value(&rec), "@first { 1, name: 2, true }");

    let single_value = vec![Item::of(1)];

    let rec = Value::Record(vec![first.clone()], single_value.clone());
    assert_eq!(print_value(&rec), "@first 1");

    let rec = Value::Record(vec![first.clone(), second.clone()], items.clone());
    assert_eq!(print_value(&rec), "@first @second(1) { 1, name: 2, true }");
}

#[test]
fn complex_attributes() {
    let first = Attr::of("first");
    let second = Attr::of(("second", 1));

    let items = vec![Item::of(1), Item::slot("name", 2), Item::of(true)];

    let rec = Value::of_attr(("tag", Value::Record(vec![first.clone()], items.clone())));
    assert_eq!(print_value(&rec), "@tag(@first { 1, name: 2, true })");

    let single_value = vec![Item::of(1)];

    let rec = Value::of_attr((
        "tag",
        Value::Record(vec![first.clone()], single_value.clone()),
    ));

    assert_eq!(print_value(&rec), "@tag(@first 1)");

    let rec = Value::of_attr((
        "tag",
        Value::Record(vec![first.clone(), second.clone()], items.clone()),
    ));

    assert_eq!(
        print_value(&rec),
        "@tag(@first @second(1) { 1, name: 2, true })"
    );
}
