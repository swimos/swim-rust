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

use num_bigint::{BigInt, BigUint};

use super::*;
use swim_common::form::{Form, FormErr, ValidatedForm};
use swim_common::model::schema::Schema;
use swim_common::model::{Attr, Item};

#[test]
pub fn clear_to_value() {
    let expected = Value::of_attr("clear");
    assert_eq!(
        Form::into_value(UntypedMapModification::<Value>::Clear),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::<Value>::Clear),
        expected
    );
}

type MapModResult = Result<UntypedMapModification<Value>, FormErr>;

#[test]
pub fn clear_from_value() {
    let rep = Value::of_attr("clear");
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(result1, Ok(UntypedMapModification::<Value>::Clear));
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(result2, Ok(UntypedMapModification::<Value>::Clear));
}

#[test]
pub fn take_to_value() {
    let expected = Value::of_attr(("take", 3));
    assert_eq!(
        Form::into_value(UntypedMapModification::<Value>::Take(3)),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::<Value>::Take(3)),
        expected
    );
}

#[test]
pub fn take_from_value() {
    let rep = Value::of_attr(("take", 3));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(result1, Ok(UntypedMapModification::Take(3)));
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(result2, Ok(UntypedMapModification::Take(3)));
}

#[test]
pub fn skip_to_value() {
    let expected = Value::of_attr(("drop", 5));
    assert_eq!(
        Form::into_value(UntypedMapModification::<Value>::Drop(5)),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::<Value>::Drop(5)),
        expected
    );
}

#[test]
pub fn skip_from_value() {
    let rep = Value::of_attr(("drop", 5));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(result1, Ok(UntypedMapModification::Drop(5)));
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(result2, Ok(UntypedMapModification::Drop(5)));
}

#[test]
pub fn remove_to_value() {
    let expected = Value::of_attr(("remove", Value::record(vec![Item::slot("key", "hello")])));
    assert_eq!(
        Form::into_value(UntypedMapModification::<Value>::Remove(Value::text(
            "hello"
        ))),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::<Value>::Remove(Value::text(
            "hello"
        ))),
        expected
    );
}

#[test]
pub fn remove_from_value() {
    let rep = Value::of_attr(("remove", Value::record(vec![Item::slot("key", "hello")])));
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(
        result1,
        Ok(UntypedMapModification::<Value>::Remove(Value::text(
            "hello"
        )))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(
        result2,
        Ok(UntypedMapModification::Remove(Value::text("hello")))
    );
}

#[test]
pub fn simple_insert_to_value() {
    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let body = Item::ValueItem(Value::Int32Value(2));
    let expected = Value::Record(vec![attr], vec![body]);
    assert_eq!(
        Form::into_value(UntypedMapModification::Update(
            Value::text("hello"),
            Arc::new(Value::Int32Value(2))
        )),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::Update(
            Value::text("hello"),
            Arc::new(Value::Int32Value(2))
        )),
        expected
    );
}

#[test]
pub fn simple_insert_from_value() {
    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let body = Item::ValueItem(Value::Int32Value(2));
    let rep = Value::Record(vec![attr], vec![body]);
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(
        result1,
        Ok(UntypedMapModification::Update(
            Value::text("hello"),
            Arc::new(Value::Int32Value(2))
        ))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(
        result2,
        Ok(UntypedMapModification::Update(
            Value::text("hello"),
            Arc::new(Value::Int32Value(2))
        ))
    );
}

#[test]
pub fn complex_insert_to_value() {
    let body = Arc::new(Value::Record(
        vec![Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    ));
    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let expected = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );
    assert_eq!(
        Form::into_value(UntypedMapModification::Update(
            Value::text("hello"),
            body.clone()
        )),
        expected
    );
    assert_eq!(
        Form::as_value(&UntypedMapModification::Update(
            Value::text("hello"),
            body.clone()
        )),
        expected
    );
}

#[test]
pub fn complex_insert_from_value() {
    let body = Arc::new(Value::Record(
        vec![Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    ));
    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let rep = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );
    let result1: MapModResult = Form::try_from_value(&rep);
    assert_eq!(
        result1,
        Ok(UntypedMapModification::Update(
            Value::text("hello"),
            body.clone()
        ))
    );
    let result2: MapModResult = Form::try_convert(rep);
    assert_eq!(
        result2,
        Ok(UntypedMapModification::Update(
            Value::text("hello"),
            body.clone()
        ))
    );
}

#[test]
pub fn map_modification_schema() {
    let clear = Value::of_attr("clear");
    let take = Value::of_attr(("take", 3));
    let skip = Value::of_attr(("drop", 5));
    let remove = Value::of_attr(("remove", Value::record(vec![Item::slot("key", "hello")])));

    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let body = Item::ValueItem(Value::Int32Value(2));
    let simple_insert = Value::Record(vec![attr], vec![body]);

    let attr = Attr::of(("update", Value::record(vec![Item::slot("key", "hello")])));
    let complex_insert = Value::Record(
        vec![attr, Attr::of(("complex", 0))],
        vec![Item::slot("a", true)],
    );

    let schema = <UntypedMapModification<Value> as ValidatedForm>::schema();

    assert!(schema.matches(&clear));
    assert!(schema.matches(&take));
    assert!(schema.matches(&skip));
    assert!(schema.matches(&remove));
    assert!(schema.matches(&simple_insert));
    assert!(schema.matches(&complex_insert));
}

#[test]
fn test_val_map_i32() {
    let mut map = ValMap::new();

    map.insert(Value::Int32Value(1), Arc::new(Value::Int32Value(10)));
    map.insert(Value::Int32Value(2), Arc::new(Value::Int32Value(20)));
    map.insert(Value::Int32Value(3), Arc::new(Value::Int32Value(30)));

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::Int32Value(10))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::Int32Value(20))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::Int32Value(30))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::Int32Value(10))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::Int32Value(20))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::Int32Value(30))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::Int32Value(10))
    );
}

#[test]
fn test_val_map_i64() {
    let mut map = ValMap::new();

    map.insert(Value::Int64Value(1), Arc::new(Value::Int64Value(10)));
    map.insert(Value::Int64Value(2), Arc::new(Value::Int64Value(20)));
    map.insert(Value::Int64Value(3), Arc::new(Value::Int64Value(30)));

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::Int64Value(10))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::Int64Value(20))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::Int64Value(30))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::Int64Value(10))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::Int64Value(20))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::Int64Value(30))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::Int64Value(10))
    );
}

#[test]
fn test_val_map_u32() {
    let mut map = ValMap::new();

    map.insert(Value::UInt32Value(1), Arc::new(Value::UInt32Value(10)));
    map.insert(Value::UInt32Value(2), Arc::new(Value::UInt32Value(20)));
    map.insert(Value::UInt32Value(3), Arc::new(Value::UInt32Value(30)));

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::UInt32Value(10))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::UInt32Value(20))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::UInt32Value(30))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::UInt32Value(10))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::UInt32Value(20))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::UInt32Value(30))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::UInt32Value(10))
    );
}

#[test]
fn test_val_map_u64() {
    let mut map = ValMap::new();

    map.insert(Value::UInt64Value(1), Arc::new(Value::UInt64Value(10)));
    map.insert(Value::UInt64Value(2), Arc::new(Value::UInt64Value(20)));
    map.insert(Value::UInt64Value(3), Arc::new(Value::UInt64Value(30)));

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::UInt64Value(10))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::UInt64Value(20))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::UInt64Value(30))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::UInt64Value(10))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::UInt64Value(20))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::UInt64Value(30))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::UInt64Value(10))
    );
}

#[test]
fn test_val_map_f64() {
    let mut map = ValMap::new();

    map.insert(
        Value::Float64Value(1.0),
        Arc::new(Value::Float64Value(10.0)),
    );
    map.insert(
        Value::Float64Value(2.0),
        Arc::new(Value::Float64Value(20.0)),
    );
    map.insert(
        Value::Float64Value(3.0),
        Arc::new(Value::Float64Value(30.0)),
    );

    assert_eq!(
        map.get(&Value::Int32Value(1)).unwrap(),
        &Arc::new(Value::Float64Value(10.0))
    );

    assert_eq!(
        map.get(&Value::Int64Value(2)).unwrap(),
        &Arc::new(Value::Float64Value(20.0))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::Float64Value(30.0))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(1)).unwrap(),
        &Arc::new(Value::Float64Value(10.0))
    );

    assert_eq!(
        map.get(&Value::Float64Value(2.0)).unwrap(),
        &Arc::new(Value::Float64Value(20.0))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::Float64Value(30.0))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::Float64Value(10.0))
    );
}

#[test]
fn test_val_map_big_int() {
    let mut map = ValMap::new();

    map.insert(
        Value::BigInt(BigInt::from(1)),
        Arc::new(Value::BigInt(BigInt::from(10))),
    );
    map.insert(
        Value::BigInt(BigInt::from(2)),
        Arc::new(Value::BigInt(BigInt::from(20))),
    );
    map.insert(
        Value::BigInt(BigInt::from(3)),
        Arc::new(Value::BigInt(BigInt::from(30))),
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(1))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(10)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(2))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(20)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(30)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(1))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(10)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(2))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(20)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(3))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(30)))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(1))).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(10)))
    );
}

#[test]
fn test_val_map_big_uint() {
    let mut map = ValMap::new();

    map.insert(
        Value::BigUint(BigUint::from(1u32)),
        Arc::new(Value::BigUint(BigUint::from(10u32))),
    );
    map.insert(
        Value::BigUint(BigUint::from(2u32)),
        Arc::new(Value::BigUint(BigUint::from(20u32))),
    );
    map.insert(
        Value::BigUint(BigUint::from(3u32)),
        Arc::new(Value::BigUint(BigUint::from(30u32))),
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(10u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(2u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(20u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(3u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(30u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(10u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(2u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(20u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(3u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(30u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(1u32))).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(10u32)))
    );
}

#[test]
fn test_val_map_mixed() {
    let mut map = ValMap::new();

    map.insert(Value::UInt64Value(3), Arc::new(Value::UInt64Value(30)));
    map.insert(Value::Int32Value(6), Arc::new(Value::Int32Value(60)));
    map.insert(
        Value::BigInt(BigInt::from(1)),
        Arc::new(Value::BigInt(BigInt::from(10))),
    );
    map.insert(Value::UInt32Value(7), Arc::new(Value::UInt32Value(70)));
    map.insert(
        Value::BigUint(BigUint::from(5u32)),
        Arc::new(Value::BigUint(BigUint::from(50u32))),
    );
    map.insert(
        Value::Float64Value(2.0),
        Arc::new(Value::Float64Value(20.0)),
    );
    map.insert(Value::Int64Value(4), Arc::new(Value::Int64Value(40)));

    assert_eq!(
        map.get(&Value::Float64Value(1.0)).unwrap(),
        &Arc::new(Value::BigInt(BigInt::from(10)))
    );

    assert_eq!(
        map.get(&Value::Int32Value(2)).unwrap(),
        &Arc::new(Value::Float64Value(20.0))
    );

    assert_eq!(
        map.get(&Value::UInt32Value(3)).unwrap(),
        &Arc::new(Value::UInt64Value(30))
    );

    assert_eq!(
        map.get(&Value::BigInt(BigInt::from(4))).unwrap(),
        &Arc::new(Value::Int64Value(40))
    );

    assert_eq!(
        map.get(&Value::UInt64Value(5)).unwrap(),
        &Arc::new(Value::BigUint(BigUint::from(50u32)))
    );

    assert_eq!(
        map.get(&Value::BigUint(BigUint::from(6u32))).unwrap(),
        &Arc::new(Value::Int32Value(60))
    );

    assert_eq!(
        map.get(&Value::Int64Value(7)).unwrap(),
        &Arc::new(Value::UInt32Value(70))
    );
}
