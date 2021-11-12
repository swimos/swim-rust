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

use crate::structural::read::StructuralReadable;
use crate::structural::tests::GeneralType;
use swim_model::{Blob, Item, Text, Value};

#[test]
fn bridge_numeric_and_logical() {
    assert!(<() as StructuralReadable>::try_read_from(&Value::Extant).is_ok());
    assert_eq!(i32::try_read_from(&Value::Int32Value(4)), Ok(4));
    assert_eq!(i64::try_read_from(&Value::Int64Value(-12)), Ok(-12i64));
    assert_eq!(u32::try_read_from(&Value::UInt32Value(5)), Ok(5u32));
    assert_eq!(u64::try_read_from(&Value::UInt64Value(44)), Ok(44u64));
    assert_eq!(bool::try_read_from(&Value::BooleanValue(false)), Ok(false));
    assert!(f64::try_read_from(&Value::Float64Value(0.5)).eq(&Ok(0.5)));
}

#[test]
fn bridge_transform_numeric_and_logical() {
    assert!(<() as StructuralReadable>::try_transform(Value::Extant).is_ok());
    assert_eq!(i32::try_transform(Value::Int32Value(4)), Ok(4));
    assert_eq!(i64::try_transform(Value::Int64Value(-12)), Ok(-12i64));
    assert_eq!(u32::try_transform(Value::UInt32Value(5)), Ok(5u32));
    assert_eq!(u64::try_transform(Value::UInt64Value(44)), Ok(44u64));
    assert_eq!(bool::try_transform(Value::BooleanValue(false)), Ok(false));
    assert!(f64::try_transform(Value::Float64Value(0.5)).eq(&Ok(0.5)));
}

#[test]
fn bridge_text() {
    assert_eq!(
        String::try_read_from(&Value::Text("hello".into())),
        Ok("hello".to_string())
    );
    assert_eq!(
        Text::try_read_from(&Value::Text("hello".into())),
        Ok(Text::new("hello"))
    );
}

#[test]
fn bridge_transform_text() {
    assert_eq!(
        String::try_transform(Value::Text("hello".into())),
        Ok("hello".to_string())
    );
    assert_eq!(
        Text::try_transform(Value::Text("hello".into())),
        Ok(Text::new("hello"))
    );
}

#[test]
fn bridge_blob() {
    let blob = vec![1u8, 2u8, 3u8];
    let rep = Value::Data(Blob::from_vec(blob.clone()));

    assert_eq!(
        <Vec<u8> as StructuralReadable>::try_read_from(&rep),
        Ok(blob.clone())
    );
}

#[test]
fn bridge_transform_blob() {
    let blob = vec![1u8, 2u8, 3u8];
    let rep = Value::Data(Blob::from_vec(blob.clone()));

    assert_eq!(
        <Vec<u8> as StructuralReadable>::try_transform(rep.clone()),
        Ok(blob.clone())
    );
}

#[test]
fn bridge_optional() {
    assert_eq!(
        <Option<i32> as StructuralReadable>::try_read_from(&Value::Extant),
        Ok(None)
    );
    assert_eq!(
        <Option<i32> as StructuralReadable>::try_read_from(&Value::Int32Value(55)),
        Ok(Some(55))
    );
}

#[test]
fn bridge_transform_optional() {
    assert_eq!(
        <Option<i32> as StructuralReadable>::try_transform(Value::Extant),
        Ok(None)
    );
    assert_eq!(
        <Option<i32> as StructuralReadable>::try_transform(Value::Int32Value(55)),
        Ok(Some(55))
    );
}

#[test]
fn bridge_vec() {
    let rep = Value::from_vec(vec![1, 2, 3]);
    assert_eq!(
        <Vec<i32> as StructuralReadable>::try_read_from(&rep),
        Ok(vec![1, 2, 3])
    );
}

#[test]
fn bridge_transform_vec() {
    let rep = Value::from_vec(vec![1, 2, 3]);
    assert_eq!(
        <Vec<i32> as StructuralReadable>::try_transform(rep),
        Ok(vec![1, 2, 3])
    );
}

#[test]
fn bridge_simple_compound() {
    let expected = GeneralType::new(2, 3);
    let rep = Value::record(vec![Item::slot("first", 2), Item::slot("second", 3)]);
    let result = <GeneralType<i32, i32> as StructuralReadable>::try_read_from(&rep);
    assert_eq!(result, Ok(expected));
}

#[test]
fn bridge_simple_compound_into() {
    let expected = GeneralType::new(2, 3);
    let rep = Value::record(vec![Item::slot("first", 2), Item::slot("second", 3)]);
    let result = <GeneralType<i32, i32> as StructuralReadable>::try_transform(rep);
    assert_eq!(result, Ok(expected));
}

#[test]
fn bridge_simple_missing_field() {
    type WithOpt = GeneralType<i32, Option<i32>>;

    let expected1: WithOpt = GeneralType::new(2, Some(3));
    let expected2: WithOpt = GeneralType::new(2, None);
    let rep1 = Value::record(vec![Item::slot("first", 2), Item::slot("second", 3)]);
    let rep2 = Value::record(vec![Item::slot("first", 2)]);
    let rep3 = Value::record(vec![
        Item::slot("first", 2),
        Item::slot("second", Value::Extant),
    ]);

    let result1 = WithOpt::try_read_from(&rep1);
    assert_eq!(result1, Ok(expected1));

    let result2 = WithOpt::try_read_from(&rep2);
    assert_eq!(result2, Ok(expected2.clone()));

    let result3 = WithOpt::try_read_from(&rep3);
    assert_eq!(result3, Ok(expected2));
}

#[test]
fn bridge_simple_missing_field_into() {
    type WithOpt = GeneralType<i32, Option<i32>>;

    let expected1: WithOpt = GeneralType::new(2, Some(3));
    let expected2: WithOpt = GeneralType::new(2, None);
    let rep1 = Value::record(vec![Item::slot("first", 2), Item::slot("second", 3)]);
    let rep2 = Value::record(vec![Item::slot("first", 2)]);
    let rep3 = Value::record(vec![
        Item::slot("first", 2),
        Item::slot("second", Value::Extant),
    ]);

    let result1 = WithOpt::try_transform(rep1);
    assert_eq!(result1, Ok(expected1));

    let result2 = WithOpt::try_transform(rep2);
    assert_eq!(result2, Ok(expected2.clone()));

    let result3 = WithOpt::try_transform(rep3);
    assert_eq!(result3, Ok(expected2));
}

#[test]
fn bridge_nested() {
    type WithVec = GeneralType<i32, Vec<i32>>;
    let expected = GeneralType::new(6, vec![5, 6, 7, 8]);
    let rep = Value::record(vec![
        Item::slot("first", 6),
        Item::slot("second", Value::from_vec(vec![5, 6, 7, 8])),
    ]);

    let result = WithVec::try_read_from(&rep);
    assert_eq!(result, Ok(expected));
}

#[test]
fn bridge_nested_into() {
    type WithVec = GeneralType<i32, Vec<i32>>;
    let expected = GeneralType::new(6, vec![5, 6, 7, 8]);
    let rep = Value::record(vec![
        Item::slot("first", 6),
        Item::slot("second", Value::from_vec(vec![5, 6, 7, 8])),
    ]);

    let result = WithVec::try_transform(rep);
    assert_eq!(result, Ok(expected));
}
