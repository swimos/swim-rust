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

fn print_value_compact(v: &Value) -> String {
    format!("{}", super::print_recon_compact(v))
}

fn print_value_pretty(v: &Value) -> String {
    format!("{}", super::print_recon_pretty(v))
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
fn simple_no_attribute_records_compact() {
    assert_eq!(print_value_compact(&Value::empty_record()), "{}");

    let single_value = Value::from_vec(vec![Item::of(1)]);
    let single_slot = Value::from_vec(vec![Item::of(("name", 1))]);

    assert_eq!(print_value_compact(&single_value), "{1}");
    assert_eq!(print_value_compact(&single_slot), "{name:1}");

    let multiple_values = Value::from_vec(vec![1, 2, 3]);
    assert_eq!(print_value_compact(&multiple_values), "{1,2,3}");

    let multiple_slots = Value::from_vec(vec![
        Item::of(("first", 1)),
        Item::of(("second", 2)),
        Item::of(("third", 3)),
    ]);
    assert_eq!(
        print_value_compact(&multiple_slots),
        "{first:1,second:2,third:3}"
    );
}

#[test]
fn simple_no_attribute_records_pretty() {
    assert_eq!(print_value_pretty(&Value::empty_record()), "{}");

    let single_value = Value::from_vec(vec![Item::of(1)]);
    let single_slot = Value::from_vec(vec![Item::of(("name", 1))]);

    assert_eq!(print_value_pretty(&single_value), "{\n    1\n}");
    assert_eq!(print_value_pretty(&single_slot), "{\n    name: 1\n}");

    let multiple_values = Value::from_vec(vec![1, 2, 3]);
    assert_eq!(
        print_value_pretty(&multiple_values),
        "{\n    1,\n    2,\n    3\n}"
    );

    let multiple_slots = Value::from_vec(vec![
        Item::of(("first", 1)),
        Item::of(("second", 2)),
        Item::of(("third", 3)),
    ]);
    assert_eq!(
        print_value_pretty(&multiple_slots),
        "{\n    first: 1,\n    second: 2,\n    third: 3\n}"
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
fn simple_attributes_compact() {
    let basic = Value::of_attr("tag");
    assert_eq!(print_value_compact(&basic), "@tag");

    let simple_value = Value::of_attr(("tag", 1));
    assert_eq!(print_value_compact(&simple_value), "@tag(1)");

    let empty_value = Value::of_attr(("tag", Value::empty_record()));
    assert_eq!(print_value_compact(&empty_value), "@tag({})");

    let single_value = Value::of_attr(("tag", Value::from_vec(vec![1])));
    assert_eq!(print_value_compact(&single_value), "@tag({1})");

    let single_slot = Value::of_attr(("tag", Value::from_vec(vec![("name", 1)])));
    assert_eq!(print_value_compact(&single_slot), "@tag(name:1)");

    let multiple_values = Value::of_attr(("tag", Value::from_vec(vec![1, 2, 3])));
    assert_eq!(print_value_compact(&multiple_values), "@tag(1,2,3)");

    let multiple_items = Value::of_attr((
        "tag",
        Value::from_vec(vec![Item::of(1), Item::slot("slot", 2), Item::of(3)]),
    ));
    assert_eq!(print_value_compact(&multiple_items), "@tag(1,slot:2,3)");
}

#[test]
fn simple_attributes_pretty() {
    let basic = Value::of_attr("tag");
    assert_eq!(print_value_pretty(&basic), "@tag");

    let simple_value = Value::of_attr(("tag", 1));
    assert_eq!(print_value_pretty(&simple_value), "@tag(1)");

    let empty_value = Value::of_attr(("tag", Value::empty_record()));
    assert_eq!(print_value_pretty(&empty_value), "@tag({})");

    let single_value = Value::of_attr(("tag", Value::from_vec(vec![1])));
    assert_eq!(print_value_pretty(&single_value), "@tag({\n    1\n})");

    let single_slot = Value::of_attr(("tag", Value::from_vec(vec![("name", 1)])));
    assert_eq!(print_value_pretty(&single_slot), "@tag(name: 1)");

    let multiple_values = Value::of_attr(("tag", Value::from_vec(vec![1, 2, 3])));
    assert_eq!(print_value_pretty(&multiple_values), "@tag(1, 2, 3)");

    let multiple_items = Value::of_attr((
        "tag",
        Value::from_vec(vec![Item::of(1), Item::slot("slot", 2), Item::of(3)]),
    ));
    assert_eq!(print_value_pretty(&multiple_items), "@tag(1, slot: 2, 3)");
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
fn nested_no_attribute_records_compact() {
    let inner = Value::from_vec(vec![1, 2, 3]);

    let single_nested = Value::from_vec(vec![Item::of(inner.clone())]);
    let single_slot_nested = Value::from_vec(vec![Item::of(("name", inner.clone()))]);

    assert_eq!(print_value_compact(&single_nested), "{{1,2,3}}");
    assert_eq!(print_value_compact(&single_slot_nested), "{name:{1,2,3}}");

    let multiple_values_nested = Value::from_vec(vec![Item::of(1), Item::of(2), Item::of(inner)]);
    assert_eq!(
        print_value_compact(&multiple_values_nested),
        "{1,2,{1,2,3}}"
    );
}

#[test]
fn nested_no_attribute_records_pretty() {
    let inner = Value::from_vec(vec![1, 2, 3]);

    let single_nested = Value::from_vec(vec![Item::of(inner.clone())]);
    let single_slot_nested = Value::from_vec(vec![Item::of(("name", inner.clone()))]);

    assert_eq!(
        print_value_pretty(&single_nested),
        "{\n    {\n        1,\n        2,\n        3\n    }\n}"
    );
    assert_eq!(
        print_value_pretty(&single_slot_nested),
        "{\n    name: {\n        1,\n        2,\n        3\n    }\n}"
    );

    let multiple_values_nested = Value::from_vec(vec![Item::of(1), Item::of(2), Item::of(inner)]);
    assert_eq!(
        print_value_pretty(&multiple_values_nested),
        "{\n    1,\n    2,\n    {\n        1,\n        2,\n        3\n    }\n}"
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

    let rec = Value::Record(vec![first.clone()], single_value);
    assert_eq!(print_value(&rec), "@first 1");

    let rec = Value::Record(vec![first, second], items);
    assert_eq!(print_value(&rec), "@first @second(1) { 1, name: 2, true }");
}

#[test]
fn complete_records_compact() {
    let first = Attr::of("first");
    let second = Attr::of(("second", 1));

    let items = vec![Item::of(1), Item::slot("name", 2), Item::of(true)];

    let rec = Value::Record(vec![first.clone()], items.clone());
    assert_eq!(print_value_compact(&rec), "@first{1,name:2,true}");

    let single_value = vec![Item::of(1)];

    let rec = Value::Record(vec![first.clone()], single_value);
    assert_eq!(print_value_compact(&rec), "@first 1");

    let rec = Value::Record(vec![first, second], items);
    assert_eq!(print_value_compact(&rec), "@first@second(1){1,name:2,true}");
}

#[test]
fn complete_records_pretty() {
    let first = Attr::of("first");
    let second = Attr::of(("second", 1));

    let items = vec![Item::of(1), Item::slot("name", 2), Item::of(true)];

    let rec = Value::Record(vec![first.clone()], items.clone());
    assert_eq!(
        print_value_pretty(&rec),
        "@first {\n    1,\n    name: 2,\n    true\n}"
    );

    let single_value = vec![Item::of(1)];

    let rec = Value::Record(vec![first.clone()], single_value);
    assert_eq!(print_value_pretty(&rec), "@first 1");

    let rec = Value::Record(vec![first, second], items);
    assert_eq!(
        print_value_pretty(&rec),
        "@first @second(1) {\n    1,\n    name: 2,\n    true\n}"
    );
}

#[test]
fn complex_attributes() {
    let first = Attr::of("first");
    let second = Attr::of(("second", 1));

    let items = vec![Item::of(1), Item::slot("name", 2), Item::of(true)];

    let rec = Value::of_attr(("tag", Value::Record(vec![first.clone()], items.clone())));
    assert_eq!(print_value(&rec), "@tag(@first { 1, name: 2, true })");

    let single_value = vec![Item::of(1)];

    let rec = Value::of_attr(("tag", Value::Record(vec![first.clone()], single_value)));

    assert_eq!(print_value(&rec), "@tag(@first 1)");

    let rec = Value::of_attr((
        "tag",
        Value::Record(vec![first.clone(), second.clone()], items.clone()),
    ));

    assert_eq!(
        print_value(&rec),
        "@tag(@first @second(1) { 1, name: 2, true })"
    );

    let rec = Value::of_attr((
        "tag",
        Value::from_vec(vec![
            Item::of(1),
            Item::of(Value::Record(vec![first, second], items)),
            Item::slot("slot", 2),
            Item::of(3),
        ]),
    ));

    assert_eq!(
        print_value(&rec),
        "@tag(1, @first @second(1) { 1, name: 2, true }, slot: 2, 3)"
    );
}

#[test]
fn complex_attributes_compact() {
    let first = Attr::of("first");
    let second = Attr::of(("second", 1));

    let items = vec![Item::of(1), Item::slot("name", 2), Item::of(true)];

    let rec = Value::of_attr(("tag", Value::Record(vec![first.clone()], items.clone())));
    assert_eq!(print_value_compact(&rec), "@tag(@first{1,name:2,true})");

    let single_value = vec![Item::of(1)];

    let rec = Value::of_attr(("tag", Value::Record(vec![first.clone()], single_value)));

    assert_eq!(print_value_compact(&rec), "@tag(@first 1)");

    let rec = Value::of_attr((
        "tag",
        Value::Record(vec![first.clone(), second.clone()], items.clone()),
    ));

    assert_eq!(
        print_value_compact(&rec),
        "@tag(@first@second(1){1,name:2,true})"
    );

    let rec = Value::of_attr((
        "tag",
        Value::from_vec(vec![
            Item::of(1),
            Item::of(Value::Record(vec![first, second], items)),
            Item::slot("slot", 2),
            Item::of(3),
        ]),
    ));

    assert_eq!(
        print_value_compact(&rec),
        "@tag(1,@first@second(1){1,name:2,true},slot:2,3)"
    );
}

#[test]
fn complex_attributes_pretty() {
    let first = Attr::of("first");
    let second = Attr::of(("second", 1));

    let items = vec![Item::of(1), Item::slot("name", 2), Item::of(true)];

    let rec = Value::of_attr(("tag", Value::Record(vec![first.clone()], items.clone())));
    assert_eq!(
        print_value_pretty(&rec),
        "@tag(@first {\n    1,\n    name: 2,\n    true\n})"
    );

    let single_value = vec![Item::of(1)];

    let rec = Value::of_attr(("tag", Value::Record(vec![first.clone()], single_value)));

    assert_eq!(print_value_pretty(&rec), "@tag(@first 1)");

    let rec = Value::of_attr((
        "tag",
        Value::Record(vec![first.clone(), second.clone()], items.clone()),
    ));

    assert_eq!(
        print_value_pretty(&rec),
        "@tag(@first @second(1) {\n    1,\n    name: 2,\n    true\n})"
    );

    let rec = Value::of_attr((
        "tag",
        Value::from_vec(vec![
            Item::of(1),
            Item::of(Value::Record(vec![first, second], items)),
            Item::slot("slot", 2),
            Item::of(3),
        ]),
    ));

    assert_eq!(
        print_value_pretty(&rec),
        "@tag(1, @first @second(1) {\n    1,\n    name: 2,\n    true\n}, slot: 2, 3)"
    );
}
