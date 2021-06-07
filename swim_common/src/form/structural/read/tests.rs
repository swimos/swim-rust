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
use crate::model::text::Text;
use crate::model::ValueKind;
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn read_unit() {
    assert_eq!(<() as ValueReadable>::read_extant(), Ok(()));
}

#[test]
fn read_i32() {
    assert_eq!(i32::read_i32(3), Ok(3));
}

#[test]
fn read_i64() {
    assert_eq!(i64::read_i64(3), Ok(3));
}

#[test]
fn read_u32() {
    assert_eq!(u32::read_u32(3), Ok(3));
}

#[test]
fn read_u64() {
    assert_eq!(u64::read_u64(3), Ok(3));
}

#[test]
fn read_bool() {
    assert_eq!(bool::read_bool(true), Ok(true));
}

#[test]
fn read_big_int() {
    assert_eq!(BigInt::read_big_int(BigInt::from(3)), Ok(BigInt::from(3)));
}

#[test]
fn read_big_uint() {
    assert_eq!(
        BigUint::read_big_uint(BigUint::from(3u32)),
        Ok(BigUint::from(3u32))
    );
}

#[test]
fn read_float() {
    assert!(f64::read_f64(1.5).eq(&Ok(1.5)));
}

#[test]
fn read_string() {
    assert_eq!(
        String::read_text(Cow::Borrowed("hello")),
        Ok(String::from("hello"))
    );
}

#[test]
fn read_text() {
    assert_eq!(
        Text::read_text(Cow::Borrowed("hello")),
        Ok(Text::from("hello"))
    );
}

#[test]
fn read_blob_vec() {
    let result = <Vec<u8> as ValueReadable>::read_blob(vec![1u8, 2u8, 3u8]);
    assert_eq!(result, Ok(vec![1u8, 2u8, 3u8]))
}

#[test]
fn read_blob_slice() {
    let result = <Box<[u8]> as ValueReadable>::read_blob(vec![1u8, 2u8, 3u8]);
    assert_eq!(result, Ok(vec![1u8, 2u8, 3u8].into_boxed_slice()))
}

#[test]
fn read_arc() {
    let result = <Arc<i32> as ValueReadable>::read_i32(3);
    assert_eq!(result, Ok(Arc::new(3)));
}

#[test]
fn read_optional_prim() {
    let result = <Option<i32> as ValueReadable>::read_i32(3);
    assert_eq!(result, Ok(Some(3)));

    let result = <Option<i32> as ValueReadable>::read_extant();
    assert_eq!(result, Ok(None));
}

#[test]
fn read_prim_vec() {
    let mut reader = <Vec<i32> as StructuralReadable>::record_reader()
        .unwrap()
        .start_body()
        .unwrap();
    assert!(reader.push_i32(1).is_ok());
    assert!(reader.push_i32(2).is_ok());
    assert!(reader.push_i32(3).is_ok());
    let result = <Vec<i32> as StructuralReadable>::try_terminate(reader);
    assert_eq!(result, Ok(vec![1, 2, 3]));
}

#[test]
fn read_simple_hash_map() {
    let mut reader = <HashMap<String, i32> as StructuralReadable>::record_reader()
        .unwrap()
        .start_body()
        .unwrap();
    assert!(reader.push_text(Cow::Borrowed("first")).is_ok());
    assert!(reader.start_slot().is_ok());
    assert!(reader.push_i32(1).is_ok());
    assert!(reader.push_text(Cow::Borrowed("second")).is_ok());
    assert!(reader.start_slot().is_ok());
    assert!(reader.push_i32(2).is_ok());
    let result = <HashMap<String, i32> as StructuralReadable>::try_terminate(reader);
    let mut expected = HashMap::new();
    expected.insert("first".to_string(), 1);
    expected.insert("second".to_string(), 2);
    assert_eq!(result, Ok(expected));
}

#[test]
fn read_nested_collection() {
    type Nested = HashMap<String, Vec<i32>>;
    type Reader = <Nested as StructuralReadable>::Reader;
    type Body = <Reader as HeaderReader>::Body;

    let mut reader = Nested::record_reader().unwrap().start_body().unwrap();
    assert!(reader.push_text(Cow::Borrowed("first")).is_ok());
    assert!(reader.start_slot().is_ok());
    let mut delegate = reader.push_record().unwrap().start_body().unwrap();
    assert!(delegate.push_i32(1).is_ok());
    assert!(delegate.push_i32(2).is_ok());
    assert!(delegate.push_i32(3).is_ok());
    reader = <Body as BodyReader>::restore(delegate).unwrap();
    assert!(reader.push_text(Cow::Borrowed("second")).is_ok());
    assert!(reader.start_slot().is_ok());
    let mut delegate = reader.push_record().unwrap().start_body().unwrap();
    assert!(delegate.push_i32(4).is_ok());
    assert!(delegate.push_i32(5).is_ok());
    assert!(delegate.push_i32(6).is_ok());

    reader = <Body as BodyReader>::restore(delegate).unwrap();
    let result = Nested::try_terminate(reader);

    let mut expected = HashMap::new();
    expected.insert("first".to_string(), vec![1, 2, 3]);
    expected.insert("second".to_string(), vec![4, 5, 6]);
    assert_eq!(result, Ok(expected));
}

#[test]
fn read_error_display() {
    let string = ReadError::UnexpectedField(Text::new("name")).to_string();
    assert_eq!(string, "Unexpected field: 'name'");

    let string = ReadError::DuplicateField(Text::new("name")).to_string();
    assert_eq!(string, "Field 'name' ocurred more than once.");

    let string = ReadError::UnexpectedSlot.to_string();
    assert_eq!(string, "Unexpected slot in record.");

    let string = ReadError::InconsistentState.to_string();
    assert_eq!(string, "The deserialization state became corrupted.");

    let string = ReadError::UnexpectedAttribute(Text::new("name")).to_string();
    assert_eq!(string, "Unexpected attribute: 'name'");

    let string =
        ReadError::MissingFields(vec![Text::new("first"), Text::new("second")]).to_string();
    assert_eq!(string, "Fields [first, second] are required.");

    let string = ReadError::IncompleteRecord.to_string();
    assert_eq!(
        string,
        "The record ended before all parts of the value were deserialized."
    );

    let string = ReadError::ReaderOverflow.to_string();
    assert_eq!(string, "Record more deeply nested than expected for type.");

    let string = ReadError::DoubleSlot.to_string();
    assert_eq!(
        string,
        "Slot divider encountered within the value of a slot."
    );

    let string = ReadError::ReaderUnderflow.to_string();
    assert_eq!(string, "Stack undeflow deserializing the value.");

    let string = ReadError::UnexpectedKind(ValueKind::Text).to_string();
    assert_eq!(string, "Unexpected value kind: Text");
}
