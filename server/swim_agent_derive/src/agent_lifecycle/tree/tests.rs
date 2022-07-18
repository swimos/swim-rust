// Copyright 2015-2021 Swim Inc.
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

use std::collections::BTreeMap;

use super::BinTree;

#[test]
fn empty_tree() {
    let source = BTreeMap::new();
    let tree = BinTree::<i32, String>::from(source);

    assert_eq!(tree, BinTree::Leaf);
}

#[test]
fn single_item() {
    let mut source = BTreeMap::new();
    source.insert(1, "a");
    let tree = BinTree::from(source);

    assert_eq!(tree, BinTree::new(1, "a"));
}

#[test]
fn two_items() {
    let mut source = BTreeMap::new();
    source.insert(1, "a");
    source.insert(2, "b");
    let tree = BinTree::from(source);

    assert_eq!(tree, BinTree::with_right(1, "a", BinTree::new(2, "b")));
}

#[test]
fn three_items() {
    let mut source = BTreeMap::new();
    source.insert(1, "a");
    source.insert(2, "b");
    source.insert(3, "c");
    let tree = BinTree::from(source);

    assert_eq!(tree, BinTree::branch(2, "b", BinTree::new(1, "a"), BinTree::new(3, "c")));
}

#[test]
fn four_items() {
    let mut source = BTreeMap::new();
    source.insert(1, "a");
    source.insert(2, "b");
    source.insert(3, "c");
    source.insert(4, "d");
    let tree = BinTree::from(source);

    let expected_left = BinTree::with_right(1, "a", BinTree::new(2, "b"));
    let expected_right = BinTree::new(4, "d");

    assert_eq!(tree, BinTree::branch(3, "c", expected_left, expected_right));
}

#[test]
fn five_items() {
    let mut source = BTreeMap::new();
    source.insert(1, "a");
    source.insert(2, "b");
    source.insert(3, "c");
    source.insert(4, "d");
    source.insert(5, "e");
    let tree = BinTree::from(source);

    let expected_left = BinTree::with_right(1, "a", BinTree::new(2, "b"));
    let expected_right = BinTree::with_right(4, "d", BinTree::new(5, "e"));

    assert_eq!(tree, BinTree::branch(3, "c", expected_left, expected_right));
}