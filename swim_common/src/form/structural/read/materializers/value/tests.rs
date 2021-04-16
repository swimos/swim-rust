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

use crate::form::structural::read::{
    BodyReader, HeaderReader, ReadError, StructuralReadable, ValueReadable,
};
use crate::model::blob::Blob;
use crate::model::text::Text;
use crate::model::{Attr, Item, Value};
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;

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
