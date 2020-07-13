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

use crate::Form;
use common::model::{Item, Value};

use im::{HashMap as ImHashMap, HashSet as ImHashSet, OrdSet};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet, LinkedList, VecDeque};

fn sort_record(value: &mut Value) {
    if let Value::Record(_, items) = value {
        items.sort()
    } else {
        panic!()
    };
}

fn expected() -> Value {
    Value::record(vec![
        Item::of(Value::Int32Value(1)),
        Item::of(Value::Int32Value(2)),
        Item::of(Value::Int32Value(3)),
        Item::of(Value::Int32Value(4)),
        Item::of(Value::Int32Value(5)),
    ])
}

#[test]
fn test_vec() {
    let vec = vec![1, 2, 3, 4, 5];
    let value = vec.as_value();

    assert_eq!(value, expected());
}

#[test]
fn im_hashset() {
    let mut hs = ImHashSet::new();
    hs.insert(1);
    hs.insert(2);
    hs.insert(3);
    hs.insert(4);
    hs.insert(5);

    let mut value = hs.as_value();
    sort_record(&mut value);

    assert_eq!(value, expected());
}

#[test]
fn test_ord_set() {
    let mut os = OrdSet::new();
    os.insert(1);
    os.insert(2);
    os.insert(3);
    os.insert(4);
    os.insert(5);

    let value = os.as_value();
    assert_eq!(value, expected());
}

#[test]
fn test_vecdeque() {
    let mut vec = VecDeque::new();
    vec.push_back(1);
    vec.push_back(2);
    vec.push_back(3);
    vec.push_back(4);
    vec.push_back(5);

    let value = vec.as_value();
    assert_eq!(value, expected());
}

#[test]
fn test_binaryheap() {
    let mut bh = BinaryHeap::new();
    bh.push(1);
    bh.push(2);
    bh.push(3);
    bh.push(4);
    bh.push(5);

    let mut value = bh.as_value();
    sort_record(&mut value);

    assert_eq!(value, expected());
}

#[test]
fn test_btreeset() {
    let mut bts = BTreeSet::new();
    bts.insert(1);
    bts.insert(2);
    bts.insert(3);
    bts.insert(4);
    bts.insert(5);

    let value = bts.as_value();
    assert_eq!(value, expected());
}

#[test]
fn test_hashset() {
    let mut hs = HashSet::new();
    hs.insert(1);
    hs.insert(2);
    hs.insert(3);
    hs.insert(4);
    hs.insert(5);

    let mut value = hs.as_value();
    sort_record(&mut value);

    assert_eq!(value, expected());
}

#[test]
fn test_linkedlist() {
    let mut ll = LinkedList::new();
    ll.push_back(1);
    ll.push_back(2);
    ll.push_back(3);
    ll.push_back(4);
    ll.push_back(5);

    let value = ll.as_value();

    assert_eq!(value, expected());
}

#[test]
fn test_imhashmap() {
    let mut hm = ImHashMap::new();
    hm.insert(String::from("1"), 1);
    hm.insert(String::from("2"), 2);
    hm.insert(String::from("3"), 3);
    hm.insert(String::from("4"), 4);
    hm.insert(String::from("5"), 5);

    let expected = Value::record(vec![
        Item::slot(String::from("1"), Value::Int32Value(1)),
        Item::slot(String::from("2"), Value::Int32Value(2)),
        Item::slot(String::from("3"), Value::Int32Value(3)),
        Item::slot(String::from("4"), Value::Int32Value(4)),
        Item::slot(String::from("5"), Value::Int32Value(5)),
    ]);

    let mut value = hm.as_value();
    sort_record(&mut value);

    assert_eq!(value, expected);
}

#[test]
fn test_btreemap() {
    let mut btm = BTreeMap::new();
    btm.insert(1, 1);
    btm.insert(2, 2);
    btm.insert(3, 3);
    btm.insert(4, 4);
    btm.insert(5, 5);

    let mut value = btm.as_value();
    sort_record(&mut value);
    let expected = Value::record(vec![
        Item::slot(1, Value::Int32Value(1)),
        Item::slot(2, Value::Int32Value(2)),
        Item::slot(3, Value::Int32Value(3)),
        Item::slot(4, Value::Int32Value(4)),
        Item::slot(5, Value::Int32Value(5)),
    ]);

    assert_eq!(value, expected);
}
