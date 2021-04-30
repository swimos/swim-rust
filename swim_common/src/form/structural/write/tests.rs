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

use crate::form::structural::write::{RecordBodyKind, StructuralWritable};
use crate::model::blob::Blob;
use crate::model::text::Text;
use crate::model::{Item, Value};
use num_bigint::{BigInt, BigUint};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

#[test]
fn unit_structure() {
    let value = ().structure();
    assert_eq!(value, Value::Extant);
}

#[test]
fn unit_into_structure() {
    let value = ().into_structure();
    assert_eq!(value, Value::Extant);
}

#[test]
fn i32_structure() {
    let value = 2i32.structure();
    assert_eq!(value, Value::Int32Value(2));
}

#[test]
fn i32_into_structure() {
    let value = 2i32.into_structure();
    assert_eq!(value, Value::Int32Value(2));
}

#[test]
fn i64_structure() {
    let value = 2i64.structure();
    assert_eq!(value, Value::Int64Value(2));
}

#[test]
fn i64_into_structure() {
    let value = 2i64.into_structure();
    assert_eq!(value, Value::Int64Value(2));
}

#[test]
fn u32_structure() {
    let value = 2u32.structure();
    assert_eq!(value, Value::UInt32Value(2));
}

#[test]
fn u32_into_structure() {
    let value = 2u32.into_structure();
    assert_eq!(value, Value::UInt32Value(2));
}

#[test]
fn u64_structure() {
    let value = 2u64.structure();
    assert_eq!(value, Value::UInt64Value(2));
}

#[test]
fn u64_into_structure() {
    let value = 2u64.into_structure();
    assert_eq!(value, Value::UInt64Value(2));
}

#[test]
fn bool_structure() {
    let value = true.structure();
    assert_eq!(value, Value::BooleanValue(true));
}

#[test]
fn bool_into_structure() {
    let value = true.into_structure();
    assert_eq!(value, Value::BooleanValue(true));
}

#[test]
fn f64_structure() {
    let value = 2.0.structure();
    assert_eq!(value, Value::Float64Value(2.0));
}

#[test]
fn f64_into_structure() {
    let value = 2.0.into_structure();
    assert_eq!(value, Value::Float64Value(2.0));
}

#[test]
fn big_int_into_structure() {
    let value = BigInt::from(2).into_structure();
    assert_eq!(value, Value::BigInt(BigInt::from(2)));
}

#[test]
fn big_uint_structure() {
    let value = BigUint::from(2u32).structure();
    assert_eq!(value, Value::BigUint(BigUint::from(2u32)));
}

#[test]
fn big_uint_into_structure() {
    let value = BigUint::from(2u32).into_structure();
    assert_eq!(value, Value::BigUint(BigUint::from(2u32)));
}

#[test]
fn str_structure() {
    let value = "hello".structure();
    assert_eq!(value, Value::Text(Text::new("hello")));
}

#[test]
fn str_into_structure() {
    let value = "hello".into_structure();
    assert_eq!(value, Value::Text(Text::new("hello")));
}

#[test]
fn string_structure() {
    let value = "hello".to_string().structure();
    assert_eq!(value, Value::Text(Text::new("hello")));
}

#[test]
fn string_into_structure() {
    let value = "hello".to_string().into_structure();
    assert_eq!(value, Value::Text(Text::new("hello")));
}

#[test]
fn text_structure() {
    let value = Text::new("hello").structure();
    assert_eq!(value, Value::Text(Text::new("hello")));
}

#[test]
fn text_into_structure() {
    let value = Text::new("hello").into_structure();
    assert_eq!(value, Value::Text(Text::new("hello")));
}

#[test]
fn ref_structure() {
    let text = Text::new("hello");
    let value = (&text).structure();
    assert_eq!(value, Value::Text(text));
}

#[test]
fn ref_into_structure() {
    let text = Text::new("hello");
    let value = (&text).into_structure();
    assert_eq!(value, Value::Text(text));
}

#[test]
fn mut_structure() {
    let mut text = Text::new("hello");
    let value = (&mut text).structure();
    assert_eq!(value, Value::Text(text));
}

#[test]
fn mut_into_structure() {
    let mut text = Text::new("hello");
    let value = (&mut text).into_structure();
    assert_eq!(value, Value::Text(text));
}

#[test]
fn arc_structure() {
    let text = Text::new("hello");
    let value = Arc::new(text.clone()).structure();
    assert_eq!(value, Value::Text(text));
}

#[test]
fn arc_into_structure() {
    let text = Text::new("hello");
    let value = Arc::new(text.clone()).into_structure();
    assert_eq!(value, Value::Text(text));
}

#[test]
fn rc_structure() {
    let text = Text::new("hello");
    let value = Rc::new(text.clone()).structure();
    assert_eq!(value, Value::Text(text));
}

#[test]
fn rc_into_structure() {
    let text = Text::new("hello");
    let value = Rc::new(text.clone()).into_structure();
    assert_eq!(value, Value::Text(text));
}

#[test]
fn blob_vec_structure() {
    let blob = vec![0u8, 1u8, 2u8];
    let value = blob.structure();
    assert_eq!(value, Value::Data(Blob::from_vec(blob)));
}

#[test]
fn blob_vec_into_structure() {
    let blob = vec![0u8, 1u8, 2u8];
    let value = blob.clone().into_structure();
    assert_eq!(value, Value::Data(Blob::from_vec(blob)));
}

#[test]
fn blob_structure() {
    let blob = Blob::from_vec(vec![0u8, 1u8, 2u8]);
    let value = blob.structure();
    assert_eq!(value, Value::Data(blob));
}

#[test]
fn blob_into_structure() {
    let blob = Blob::from_vec(vec![0u8, 1u8, 2u8]);
    let value = blob.clone().into_structure();
    assert_eq!(value, Value::Data(blob));
}

#[test]
fn blob_slice_structure() {
    let blob = vec![0u8, 1u8, 2u8];
    let value = (blob.as_slice()).structure();
    assert_eq!(value, Value::Data(Blob::from_vec(blob)));
}

#[test]
fn blob_slice_into_structure() {
    let blob = vec![0u8, 1u8, 2u8];
    let value = (blob.as_slice()).into_structure();
    assert_eq!(value, Value::Data(Blob::from_vec(blob)));
}

#[test]
fn vec_structure() {
    let vec = vec![1, 2, 3];
    let value = vec.structure();
    assert_eq!(value, Value::from_vec(vec![1, 2, 3]));
}

#[test]
fn vec_into_structure() {
    let vec = vec![1, 2, 3];
    let value = vec.into_structure();
    assert_eq!(value, Value::from_vec(vec![1, 2, 3]));
}

#[test]
fn hash_map_strucuture() {
    let mut map = HashMap::new();
    map.insert("first".to_string(), 1);
    map.insert("second".to_string(), 2);
    let value = map.into_structure();

    let expected1 = Item::slot("first", 1);
    let expected2 = Item::slot("second", 2);

    match value {
        Value::Record(attrs, items) if attrs.is_empty() => match items.as_slice() {
            [item1, item2] => {
                assert!(
                    (item1 == &expected1 && item2 == &expected2)
                        || (item1 == &expected2 && item2 == &expected1)
                );
            }
            _ => {
                panic!("Wrong number of items.");
            }
        },
        ow => panic!("Unepected value: {}", ow),
    };
}

#[test]
fn nested_collection_strucuture() {
    let mut map = HashMap::new();
    map.insert("first".to_string(), vec![1, 2, 3]);
    map.insert("second".to_string(), vec![4, 5, 6]);
    let value = map.into_structure();

    let expected1 = Item::slot("first", Value::from_vec(vec![1, 2, 3]));
    let expected2 = Item::slot("second", Value::from_vec(vec![4, 5, 6]));

    match value {
        Value::Record(attrs, items) if attrs.is_empty() => match items.as_slice() {
            [item1, item2] => {
                assert!(
                    (item1 == &expected1 && item2 == &expected2)
                        || (item1 == &expected2 && item2 == &expected1)
                );
            }
            _ => {
                panic!("Wrong number of items.");
            }
        },
        ow => panic!("Unepected value: {}", ow),
    };
}

#[test]
fn some_structure() {
    let opt = Some(1);
    let value = opt.structure();
    assert_eq!(value, Value::Int32Value(1));
}

#[test]
fn some_into_structure() {
    let opt = Some(1);
    let value = opt.into_structure();
    assert_eq!(value, Value::Int32Value(1));
}

#[test]
fn none_structure() {
    let opt: Option<i32> = None;
    let value = opt.structure();
    assert_eq!(value, Value::Extant);
}

#[test]
fn none_into_structure() {
    let opt: Option<i32> = None;
    let value = opt.into_structure();
    assert_eq!(value, Value::Extant);
}

#[test]
fn calculate_body_kind() {
    let array_like = vec![Item::of(1), Item::of("name"), Item::of(true)];
    let map_like = vec![
        Item::slot("first", 1),
        Item::slot(2, "second"),
        Item::slot(true, false),
    ];
    let mixed = vec![Item::of(1), Item::slot("second", 2)];

    assert_eq!(
        RecordBodyKind::of_iter(array_like.iter()),
        Some(RecordBodyKind::ArrayLike)
    );
    assert_eq!(
        RecordBodyKind::of_iter(map_like.iter()),
        Some(RecordBodyKind::MapLike)
    );
    assert_eq!(
        RecordBodyKind::of_iter(mixed.iter()),
        Some(RecordBodyKind::Mixed)
    );
    assert_eq!(RecordBodyKind::of_iter(vec![].iter()), None);
}
