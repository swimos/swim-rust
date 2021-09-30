// Copyright 2015-2021 SWIM.AI inc.
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

use crate::structural::read::parser::{parse_recognize, parse_recognize_with, Span};
use crate::structural::read::recognizer::Recognizer;
use crate::structural::read::recognizer::RecognizerReadable;
use crate::structural::read::StructuralReadable;
use swim_model::{Attr, Blob, Item, Text, Value};
use swim_model::bigint::{BigInt, BigUint};

mod swim_form {
    pub use crate::*;
}

fn run_recognizer<T: StructuralReadable>(rep: &str) -> T {
    let span = Span::new(rep);
    parse_recognize(span).unwrap()
}

fn run_specific_recognizer<R: Recognizer>(rep: &str, recognizer: &mut R) -> R::Target {
    let span = Span::new(rep);
    parse_recognize_with(span, recognizer).unwrap()
}

fn round_trip(value: Value) {
    let result1 = Value::try_read_from(&value);
    assert_eq!(result1.as_ref(), Ok(&value));
    let result2 = Value::try_transform(value);
    assert_eq!(result2, result1);
}

#[test]
fn prim_round_trip() {
    round_trip(Value::Int32Value(-7));
    round_trip(Value::Int64Value(-8475729399494));
    round_trip(Value::Extant);
    round_trip(Value::UInt32Value(64));
    round_trip(Value::UInt64Value(8889));
    round_trip(Value::BigInt(BigInt::from(-73637)));
    round_trip(Value::BigUint(BigUint::from(64738283u64)));
    round_trip(Value::BooleanValue(true));
    round_trip(Value::Text(Text::new("hello")));
    round_trip(Value::Float64Value(0.1));
    round_trip(Value::Data(Blob::from_vec(vec![0u8, 2u8, 4u8])));
}

#[test]
fn simple_record_round_trip() {
    let value = Value::from_vec(vec![Item::slot("name", true), Item::from(2)]);
    round_trip(value);
}

#[test]
fn complex_slot_round_trip() {
    let value1 = Value::from_vec(vec![Item::slot("name", Value::empty_record())]);
    round_trip(value1);

    let value2 = Value::from_vec(vec![Item::slot("name", Value::from_vec(vec![1]))]);
    round_trip(value2);
}

#[test]
fn with_attributes_round_trip() {
    let attr1 = Attr::of("tag");
    let attr2 = Attr::of(("simple", 1));
    let attr3 = Attr::of(("simple", Value::from_vec(vec![("name", 1)])));

    let body = vec![Item::from(2), Item::slot("name", true)];

    let rec1 = Value::Record(vec![attr1.clone()], body.clone());
    round_trip(rec1);

    let rec2 = Value::Record(vec![attr2.clone()], body.clone());
    round_trip(rec2);

    let rec3 = Value::Record(vec![attr3.clone()], body.clone());
    round_trip(rec3);

    let rec4 = Value::Record(
        vec![attr1.clone(), attr2.clone(), attr3.clone()],
        body.clone(),
    );
    round_trip(rec4);
}

#[test]
fn nested_round_trip() {
    let inner1 = Value::from_vec(vec![Item::from(2), Item::slot("name", true)]);
    let inner2 = Value::Record(
        vec![Attr::of("tag")],
        vec![Item::from(2), Item::slot("name", true)],
    );
    let rec1 = Value::record(vec![
        Item::slot("first", inner1.clone()),
        Item::slot(inner1.clone(), "second"),
    ]);

    let rec2 = Value::record(vec![
        Item::slot("first", inner2.clone()),
        Item::slot(inner2.clone(), "second"),
    ]);

    round_trip(rec1);
    round_trip(rec2);
}

#[derive(StructuralReadable, PartialEq, Eq, Debug)]
struct AttrWrapper {
    #[form(attr)]
    inner: Value,
}

#[test]
fn value_from_empty_attr_body() {
    let wrapper = run_recognizer::<AttrWrapper>("@AttrWrapper @inner");
    assert_eq!(
        wrapper,
        AttrWrapper {
            inner: Value::Extant
        }
    );
}

#[test]
fn value_from_simple_attr_body() {
    let wrapper = run_recognizer::<AttrWrapper>("@AttrWrapper @inner(2)");
    assert_eq!(
        wrapper,
        AttrWrapper {
            inner: Value::Int32Value(2)
        }
    );
}

#[test]
fn value_from_slot_attr_body() {
    let wrapper = run_recognizer::<AttrWrapper>("@AttrWrapper @inner(a:2)");
    assert_eq!(
        wrapper,
        AttrWrapper {
            inner: Value::record(vec![Item::slot("a", 2)])
        }
    );
}

#[test]
fn value_from_complex_attr_body() {
    let wrapper = run_recognizer::<AttrWrapper>("@AttrWrapper @inner(1, 2, 3)");
    assert_eq!(
        wrapper,
        AttrWrapper {
            inner: Value::record(vec![Item::of(1), Item::of(2), Item::of(3)])
        }
    );
}

#[test]
fn value_from_nested_attr_body() {
    let wrapper = run_recognizer::<AttrWrapper>("@AttrWrapper @inner({})");
    assert_eq!(
        wrapper,
        AttrWrapper {
            inner: Value::empty_record()
        }
    );
}

#[test]
fn value_delegate_body() {
    let mut recog = Value::make_body_recognizer();
    let value = run_specific_recognizer("{}", &mut recog);
    assert_eq!(value, Value::Extant);

    let value = run_specific_recognizer("{ 2 }", &mut recog);
    assert_eq!(value, Value::Int32Value(2));

    let value = run_specific_recognizer("{ a: 2 }", &mut recog);
    assert_eq!(value, Value::record(vec![Item::slot("a", 2)]));

    let value = run_specific_recognizer("{ {} }", &mut recog);
    assert_eq!(value, Value::empty_record());

    let value = run_specific_recognizer("{ 1, 2 }", &mut recog);
    assert_eq!(value, Value::record(vec![Item::of(1), Item::of(2)]));
}
