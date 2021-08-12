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

use crate::hash_indexer::HashIndexer;

#[test]
fn test_hash_indexer_insert() {
    let mut indexer = HashIndexer::new();
    assert_eq!(indexer.len(), 0);

    let first_index = indexer.insert("First".to_string());
    assert_eq!(indexer.len(), 1);

    let second_index = indexer.insert("Second".to_string());
    assert_eq!(indexer.len(), 2);

    let third_index = indexer.insert("Third".to_string());
    assert_eq!(indexer.len(), 3);

    assert_eq!(first_index, 0);
    assert_eq!(second_index, 1);
    assert_eq!(third_index, 2);
}

#[test]
fn test_hash_indexer_remove_existing() {
    let mut indexer = HashIndexer::new();
    let first_index = indexer.insert("First".to_string());
    let second_index = indexer.insert("Second".to_string());
    let third_index = indexer.insert("Third".to_string());
    assert_eq!(indexer.len(), 3);

    let item = indexer.remove(second_index).unwrap();
    assert_eq!(item, "Second".to_string());
    assert_eq!(indexer.len(), 2);

    let item = indexer.remove(third_index).unwrap();
    assert_eq!(item, "Third".to_string());
    assert_eq!(indexer.len(), 1);

    let item = indexer.remove(first_index).unwrap();
    assert_eq!(item, "First".to_string());
    assert_eq!(indexer.len(), 0);
}

#[test]
fn test_hash_indexer_remove_non_existing() {
    let mut indexer = HashIndexer::new();
    let _first_index = indexer.insert("First".to_string());
    let _second_index = indexer.insert("Second".to_string());
    let _third_index = indexer.insert("Third".to_string());
    assert_eq!(indexer.len(), 3);

    let item = indexer.remove(3);
    assert!(item.is_none());
    assert_eq!(indexer.len(), 3);

    let item = indexer.remove(4);
    assert!(item.is_none());
    assert_eq!(indexer.len(), 3);

    let item = indexer.remove(5);
    assert!(item.is_none());
    assert_eq!(indexer.len(), 3);
}

#[test]
fn test_hash_indexer_reuse_indices() {
    let mut indexer = HashIndexer::new();
    assert_eq!(indexer.top_idx, 0);
    assert_eq!(indexer.empty_idx.len(), 0);

    let first_index = indexer.insert("First".to_string());
    assert_eq!(first_index, 0);
    assert_eq!(indexer.top_idx, 1);
    assert_eq!(indexer.empty_idx.len(), 0);
    assert_eq!(indexer.len(), 1);

    let second_index = indexer.insert("Second".to_string());
    assert_eq!(second_index, 1);
    assert_eq!(indexer.top_idx, 2);
    assert_eq!(indexer.empty_idx.len(), 0);
    assert_eq!(indexer.len(), 2);

    let third_index = indexer.insert("Third".to_string());
    assert_eq!(third_index, 2);
    assert_eq!(indexer.top_idx, 3);
    assert_eq!(indexer.empty_idx.len(), 0);
    assert_eq!(indexer.len(), 3);

    let item = indexer.remove(third_index).unwrap();
    assert_eq!(item, "Third".to_string());
    assert_eq!(indexer.top_idx, 3);
    assert_eq!(indexer.empty_idx, vec![2]);
    assert_eq!(indexer.len(), 2);

    let item = indexer.remove(first_index).unwrap();
    assert_eq!(item, "First".to_string());
    assert_eq!(indexer.top_idx, 3);
    assert_eq!(indexer.empty_idx, vec![2, 0]);
    assert_eq!(indexer.len(), 1);

    let fourth_index = indexer.insert("Fourth".to_string());
    assert_eq!(fourth_index, 0);
    assert_eq!(indexer.top_idx, 3);
    assert_eq!(indexer.empty_idx, vec![2]);
    assert_eq!(indexer.len(), 2);

    let fifth_index = indexer.insert("Fifth".to_string());
    assert_eq!(fifth_index, 2);
    assert_eq!(indexer.top_idx, 3);
    assert_eq!(indexer.empty_idx.len(), 0);
    assert_eq!(indexer.len(), 3);

    let sixth_index = indexer.insert("Sixth".to_string());
    assert_eq!(sixth_index, 3);
    assert_eq!(indexer.top_idx, 4);
    assert_eq!(indexer.empty_idx.len(), 0);
    assert_eq!(indexer.len(), 4);

    let seventh_index = indexer.insert("Seventh".to_string());
    assert_eq!(seventh_index, 4);
    assert_eq!(indexer.top_idx, 5);
    assert_eq!(indexer.empty_idx.len(), 0);
    assert_eq!(indexer.len(), 5);
}
