// Copyright 2015-2023 Swim Inc.
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

use std::collections::{BTreeMap, VecDeque};

#[cfg(test)]
mod tests;

/// An binary tree of labelled values.
///
/// #Type parameters.
/// * `K` - The type of the labels.
/// * `T` - The type of the data at each branch node of the tree.
#[derive(Debug, PartialEq, Eq)]
pub enum BinTree<K, T> {
    Branch {
        key: K,
        data: T,
        left: Box<Self>,
        right: Box<Self>,
    },
    Leaf,
}

impl<K, T> Default for BinTree<K, T> {
    fn default() -> Self {
        BinTree::Leaf
    }
}

impl<K, T> BinTree<K, T> {
    /// Create a tree with a single branch node.
    pub fn new(key: K, data: T) -> Self {
        BinTree::Branch {
            key,
            data,
            left: Default::default(),
            right: Default::default(),
        }
    }

    /// Create a tree node with a branch on the right only.
    pub fn with_right(key: K, data: T, right: Self) -> Self {
        BinTree::Branch {
            key,
            data,
            left: Default::default(),
            right: Box::new(right),
        }
    }

    /// Create a tree node with two subranches.
    pub fn branch(key: K, data: T, left: Self, right: Self) -> Self {
        BinTree::Branch {
            key,
            data,
            left: Box::new(left),
            right: Box::new(right),
        }
    }
}

impl<K, T> From<BTreeMap<K, T>> for BinTree<K, T> {
    fn from(map: BTreeMap<K, T>) -> Self {
        from_sorted_vec(map.into_iter().collect())
    }
}

/// Create a sorted tree from a sorted vector of labelled items.
fn from_sorted_vec<K, T>(mut data: VecDeque<(K, T)>) -> BinTree<K, T> {
    if data.len() < 3 {
        let first = data.pop_front();
        let second = data.pop_front();
        match (first, second) {
            (Some((k1, t1)), Some((k2, t2))) => BinTree::with_right(k1, t1, BinTree::new(k2, t2)),
            (Some((k, t)), _) => BinTree::new(k, t),
            _ => BinTree::Leaf,
        }
    } else {
        let offset = data.len() / 2;
        let mut upper = data.split_off(offset);

        let (k_cent, t_cent) = upper.pop_front().unwrap(); // The upper half must have at least one member.

        let left = from_sorted_vec(data);
        let right = from_sorted_vec(upper);
        BinTree::branch(k_cent, t_cent, left, right)
    }
}
