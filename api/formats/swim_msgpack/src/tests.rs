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

use crate::MsgPackInterpreter;
use crate::{read_from_msg_pack, MsgPackReadError};
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use std::fmt::Debug;
use swim_form::Form;
use swim_model::{Attr, Item, Value};

fn validate<T: Form + PartialEq + Debug>(value: &T) {
    let mut buffer = BytesMut::new();
    let mut writer = (&mut buffer).writer();

    let interp = MsgPackInterpreter::new(&mut writer);
    assert!(value.write_with(interp).is_ok());

    let bytes = buffer.split().freeze();

    let restored: Result<T, MsgPackReadError> = read_from_msg_pack(&mut bytes.clone());

    assert!(restored.is_ok());

    assert_eq!(value, &restored.unwrap());
}

const I32VALUES: [i32; 4] = [1, -100, 1234, -87657];
const U32VALUES: [u32; 5] = [1u32, 100u32, 1234u32, 87657u32, u32::MAX];
const I64VALUES: [i64; 5] = [1i64, -100i64, 1234i64, -87657i64, 105678750199i64];
const U64VALUES: [u64; 6] = [1u64, 100u64, 1234u64, 87657u64, 105678750199u64, u64::MAX];

#[test]
fn msgpack_small_numbers() {
    for n in &I32VALUES {
        validate(n);
    }
    for n in &I64VALUES {
        validate(n);
    }
    for n in &U32VALUES {
        validate(n);
    }
    for n in &U64VALUES {
        validate(n);
    }
}

#[test]
fn msgpack_bools() {
    validate(&true);
    validate(&false);
}

#[test]
fn msgpack_floats() {
    validate(&0.0);
    validate(&123e-78);
}

#[test]
fn msgpack_unit() {
    validate(&());
}

const SIMPLE_STRINGS: [&str; 3] = [
    "",
    "some text",
    "a moderately long sentence with quite a few words in it",
];

const DATA_LENGTHS: [usize; 2] = [u8::MAX as usize + 1, u16::MAX as usize + 1];

#[test]
fn msgpack_string() {
    for s in &SIMPLE_STRINGS {
        validate(&s.to_string());
    }
    for n in &DATA_LENGTHS {
        let long: String = std::iter::repeat('a').take(*n).collect();
        validate(&long);
    }
}

#[test]
fn msgpack_blobs() {
    let small_blob = vec![1u8, 2u8, 3u8];
    validate(&small_blob);
    for n in &DATA_LENGTHS {
        let blob: Vec<u8> = std::iter::repeat(2u8).take(*n).collect();
        validate(&blob);
    }
}

#[test]
fn msgpack_optional() {
    let some: Option<i32> = Some(45);
    let none: Option<i32> = None;

    validate(&some);
    validate(&none);
}

#[test]
fn msgpack_empty_record() {
    let empty = Value::empty_record();
    validate(&empty);
}

#[test]
fn msgpack_simple_attr() {
    let value = Value::of_attr("tag");
    validate(&value);
}

#[test]
fn msgpack_simple_attr_with_value() {
    let value = Value::of_attr(("tag", 5));
    validate(&value);
}

#[test]
fn msgpack_multiple_simple_attrs() {
    let value = Value::of_attrs(vec![
        Attr::of(("first", -8)),
        Attr::of(("second", false)),
        Attr::of(("third", "name")),
    ]);
    validate(&value);
}

#[test]
fn msgpack_array_like_record() {
    let value = vec![1, 2, 3, 4, 5];
    validate(&value);
}

#[test]
fn msgpack_map_like_record() {
    let mut map = HashMap::new();
    map.insert("first".to_string(), 1);
    map.insert("second".to_string(), 2);
    map.insert("third".to_string(), 3);

    validate(&map);
}

#[test]
fn msgpack_mixed_record() {
    let value = Value::record(vec![
        Item::of(3),
        Item::slot("slot", true),
        Item::of("name"),
    ]);

    validate(&value);
}

#[test]
fn msgpack_attr_and_items_record() {
    let value = Value::Record(
        vec![Attr::of("first"), Attr::of(("second", -5000000))],
        vec![Item::of(3), Item::slot("slot", true), Item::of("name")],
    );

    validate(&value);
}

#[test]
fn msgpack_nested_record() {
    let mut map = HashMap::new();
    map.insert("first".to_string(), vec![1, 2, 3]);
    map.insert("second".to_string(), vec![4, 5, 6]);
    map.insert("third".to_string(), vec![]);

    validate(&map);
}

#[test]
fn msgpack_nested_attribute() {
    let value = Value::of_attr(("tag", Value::from_vec(vec![1, 2, 3])));
    validate(&value);
}
