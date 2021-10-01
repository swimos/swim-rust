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

use crate::structural::write::interpreters::value::ValueInterpreter;
use crate::structural::write::StructuralWritable;
use swim_model::bigint::{BigInt, BigUint};
use swim_model::{Attr, Blob, Item, Value};

fn check_value(v: Value) {
    assert_eq!(v.write_with_infallible(ValueInterpreter::default()), v);
}

#[test]
fn primitive_values() {
    check_value(Value::Extant);
    check_value(Value::Int32Value(2));
    check_value(Value::Int64Value(10000000000));
    check_value(Value::UInt32Value(3));
    check_value(Value::UInt64Value(10000000000));
    check_value(Value::BooleanValue(true));
    check_value(Value::Float64Value(1.0));
    check_value(Value::BigInt(BigInt::from(1)));
    check_value(Value::BigUint(BigUint::from(1u32)));
    check_value(Value::text("hello"));
    check_value(Value::Data(Blob::from_vec(vec![1, 2, 3])));
}

#[test]
fn simple_records() {
    let rec = Value::from_vec(vec![Item::of(3), Item::of(("field1", true))]);

    check_value(rec);

    let rec = Value::of_attr("name");

    check_value(rec);

    let rec = Value::of_attr(("name", 3));

    check_value(rec);

    let rec = Value::of_attrs(vec![Attr::of("first"), Attr::of(("second", true))]);

    check_value(rec);
}

#[test]
fn record_with_attrs_and_items() {
    let rec = Value::Record(
        vec![Attr::of("name")],
        vec![Item::of(3), Item::of(("field1", true))],
    );

    check_value(rec);
}

#[test]
fn nested_record() {
    let inner1 = Value::from_vec(vec![Item::of(("name", 1))]);

    let inner2 = Value::from_vec(vec![Item::of(("first", 1)), Item::of(("second", 2))]);

    let rec = Value::Record(
        vec![Attr::of(("tag", inner1))],
        vec![Item::of(("nested", inner2))],
    );

    check_value(rec);
}
