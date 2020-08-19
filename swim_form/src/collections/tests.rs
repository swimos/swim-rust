// Copyright 2015-2020 SWIM.AI inc.
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

use super::*;
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use std::collections::HashSet;
use swim_common::model::schema::Schema;
use swim_common::model::{Attr, Value};

#[test]
fn value_to_vector() {
    let value = Value::from_vec(vec![1, 2, 3]);
    let result: Result<Vec<i32>, _> = Form::try_from_value(&value);
    assert_that!(result, eq(Ok(vec![1, 2, 3])));
}

#[test]
fn value_into_vector() {
    let value = Value::from_vec(vec![1, 2, 3]);
    let result: Result<Vec<i32>, _> = Form::try_convert(value);
    assert_that!(result, eq(Ok(vec![1, 2, 3])));
}

#[test]
fn vector_to_value() {
    let value = vec![1, 2, 3].as_value();
    assert_that!(value, eq(Value::from_vec(vec![1, 2, 3])));
}

#[test]
fn vector_into_value() {
    let value = vec![1, 2, 3].into_value();
    assert_that!(value, eq(Value::from_vec(vec![1, 2, 3])));
}

#[test]
fn vector_schema() {
    let good = Value::from_vec(vec![1, 2, 3]);
    let bad1 = Value::Int32Value(2);
    let bad2 = Value::Record(
        vec![Attr::of("name")],
        vec![Item::of(1), Item::of(2), Item::of(3)],
    );

    let schema = <Vec<i32> as ValidatedForm>::schema();
    assert!(schema.matches(&good));
    assert!(!schema.matches(&bad1));
    assert!(!schema.matches(&bad2));
}

#[test]
fn hashmap_to_value() {
    let mut map = HashMap::new();
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);
    map.insert("c".to_string(), 3);

    let value = map.as_value();

    let items = match value {
        Value::Record(attrs, items) => {
            assert_that!(attrs.as_slice(), empty());
            assert_that!(items.len(), eq(3));
            Some(items.into_iter().collect::<HashSet<_>>())
        }
        _ => None,
    };
    let mut expected = HashSet::new();
    expected.insert(Item::slot("a", 1));
    expected.insert(Item::slot("b", 2));
    expected.insert(Item::slot("c", 3));
    assert_that!(items, eq(Some(expected)));
}

#[test]
fn hashmap_into_value() {
    let mut map = HashMap::new();
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);
    map.insert("c".to_string(), 3);

    let value = map.into_value();

    let items = match value {
        Value::Record(attrs, items) => {
            assert_that!(attrs.as_slice(), empty());
            assert_that!(items.len(), eq(3));
            Some(items.into_iter().collect::<HashSet<_>>())
        }
        _ => None,
    };
    let mut expected = HashSet::new();
    expected.insert(Item::slot("a", 1));
    expected.insert(Item::slot("b", 2));
    expected.insert(Item::slot("c", 3));
    assert_that!(items, eq(Some(expected)));
}

#[test]
fn value_to_hashmap() {
    let value = Value::from_vec(vec![("a", 1), ("b", 2), ("c", 3)]);
    let mut expected = HashMap::new();
    expected.insert("a".to_string(), 1);
    expected.insert("b".to_string(), 2);
    expected.insert("c".to_string(), 3);

    let map: Result<HashMap<String, i32>, _> = Form::try_from_value(&value);
    assert_that!(map, eq(Ok(expected)));
}

#[test]
fn value_into_hashmap() {
    let value = Value::from_vec(vec![("a", 1), ("b", 2), ("c", 3)]);
    let mut expected = HashMap::new();
    expected.insert("a".to_string(), 1);
    expected.insert("b".to_string(), 2);
    expected.insert("c".to_string(), 3);

    let map: Result<HashMap<String, i32>, _> = Form::try_convert(value);
    assert_that!(map, eq(Ok(expected)));
}

#[test]
fn btreemap_to_value() {
    let mut map = BTreeMap::new();
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);
    map.insert("c".to_string(), 3);

    let value = map.as_value();

    let items = match value {
        Value::Record(attrs, items) => {
            assert_that!(attrs.as_slice(), empty());
            assert_that!(items.len(), eq(3));
            Some(items.into_iter().collect::<HashSet<_>>())
        }
        _ => None,
    };
    let mut expected = HashSet::new();
    expected.insert(Item::slot("a", 1));
    expected.insert(Item::slot("b", 2));
    expected.insert(Item::slot("c", 3));
    assert_that!(items, eq(Some(expected)));
}

#[test]
fn btreemap_into_value() {
    let mut map = BTreeMap::new();
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);
    map.insert("c".to_string(), 3);

    let value = map.into_value();

    let items = match value {
        Value::Record(attrs, items) => {
            assert_that!(attrs.as_slice(), empty());
            assert_that!(items.len(), eq(3));
            Some(items.into_iter().collect::<HashSet<_>>())
        }
        _ => None,
    };
    let mut expected = HashSet::new();
    expected.insert(Item::slot("a", 1));
    expected.insert(Item::slot("b", 2));
    expected.insert(Item::slot("c", 3));
    assert_that!(items, eq(Some(expected)));
}

#[test]
fn value_to_btreemap() {
    let value = Value::from_vec(vec![("a", 1), ("b", 2), ("c", 3)]);
    let mut expected = BTreeMap::new();
    expected.insert("a".to_string(), 1);
    expected.insert("b".to_string(), 2);
    expected.insert("c".to_string(), 3);

    let map: Result<BTreeMap<String, i32>, _> = Form::try_from_value(&value);
    assert_that!(map, eq(Ok(expected)));
}

#[test]
fn btree_into_hashmap() {
    let value = Value::from_vec(vec![("a", 1), ("b", 2), ("c", 3)]);
    let mut expected = BTreeMap::new();
    expected.insert("a".to_string(), 1);
    expected.insert("b".to_string(), 2);
    expected.insert("c".to_string(), 3);

    let map: Result<BTreeMap<String, i32>, _> = Form::try_convert(value);
    assert_that!(map, eq(Ok(expected)));
}

#[test]
fn ordmap_to_value() {
    let mut map = OrdMap::new();
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);
    map.insert("c".to_string(), 3);

    let value = map.as_value();

    let items = match value {
        Value::Record(attrs, items) => {
            assert_that!(attrs.as_slice(), empty());
            assert_that!(items.len(), eq(3));
            Some(items.into_iter().collect::<HashSet<_>>())
        }
        _ => None,
    };
    let mut expected = HashSet::new();
    expected.insert(Item::slot("a", 1));
    expected.insert(Item::slot("b", 2));
    expected.insert(Item::slot("c", 3));
    assert_that!(items, eq(Some(expected)));
}

#[test]
fn ordmap_into_value() {
    let mut map = OrdMap::new();
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);
    map.insert("c".to_string(), 3);

    let value = map.into_value();

    let items = match value {
        Value::Record(attrs, items) => {
            assert_that!(attrs.as_slice(), empty());
            assert_that!(items.len(), eq(3));
            Some(items.into_iter().collect::<HashSet<_>>())
        }
        _ => None,
    };
    let mut expected = HashSet::new();
    expected.insert(Item::slot("a", 1));
    expected.insert(Item::slot("b", 2));
    expected.insert(Item::slot("c", 3));
    assert_that!(items, eq(Some(expected)));
}

#[test]
fn value_to_ordmap() {
    let value = Value::from_vec(vec![("a", 1), ("b", 2), ("c", 3)]);
    let mut expected = OrdMap::new();
    expected.insert("a".to_string(), 1);
    expected.insert("b".to_string(), 2);
    expected.insert("c".to_string(), 3);

    let map: Result<OrdMap<String, i32>, _> = Form::try_from_value(&value);
    assert_that!(map, eq(Ok(expected)));
}

#[test]
fn value_into_ordmap() {
    let value = Value::from_vec(vec![("a", 1), ("b", 2), ("c", 3)]);
    let mut expected = OrdMap::new();
    expected.insert("a".to_string(), 1);
    expected.insert("b".to_string(), 2);
    expected.insert("c".to_string(), 3);

    let map: Result<OrdMap<String, i32>, _> = Form::try_convert(value);
    assert_that!(map, eq(Ok(expected)));
}

#[test]
fn map_schemas() {
    let good = Value::from_vec(vec![("a", 1), ("b", 2), ("c", 3)]);
    let bad1 = Value::from_vec(vec![1, 2, 3]);
    let bad2 = Value::Int32Value(2);
    let bad3 = Value::Record(
        vec![Attr::of("name")],
        vec![Item::of(1), Item::of(2), Item::of(3)],
    );

    let schema1 = <HashMap<String, i32> as ValidatedForm>::schema();
    let schema2 = <BTreeMap<String, i32> as ValidatedForm>::schema();
    let schema3 = <OrdMap<String, i32> as ValidatedForm>::schema();
    assert!(schema1.matches(&good));
    assert!(schema2.matches(&good));
    assert!(schema3.matches(&good));
    assert!(!schema1.matches(&bad1));
    assert!(!schema2.matches(&bad1));
    assert!(!schema3.matches(&bad1));
    assert!(!schema1.matches(&bad2));
    assert!(!schema2.matches(&bad2));
    assert!(!schema3.matches(&bad2));
    assert!(!schema1.matches(&bad3));
    assert!(!schema2.matches(&bad3));
    assert!(!schema3.matches(&bad3));
}
