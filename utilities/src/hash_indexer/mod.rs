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

use std::collections::HashMap;

#[cfg(test)]
mod tests;

/// A data structure that stores items and indexes them.
///
/// When inserting an item into the indexer, it returns a unique index for the item. The index
/// can later be used to retrieve the item.
///
/// The indexer retrieves and removes items in constant time and reuses indices that have been freed.
#[derive(Debug)]
pub struct HashIndexer<T> {
    items: HashMap<u32, T>,
    empty_idx: Vec<u32>,
    top_idx: u32,
}

impl<T> HashIndexer<T> {
    pub fn new() -> Self {
        HashIndexer {
            items: HashMap::new(),
            empty_idx: vec![],
            top_idx: 0,
        }
    }

    /// Inserts an item into the indexer and returns its unique index.
    pub fn insert(&mut self, item: T) -> u32 {
        let index = if self.empty_idx.is_empty() {
            let index = self.top_idx;
            self.top_idx += 1;
            index
        } else {
            self.empty_idx.pop().unwrap()
        };

        self.items.insert(index, item);
        index
    }

    /// Removes and returns an item from the indexer given its index. If the index
    /// is not present, [`None`] is returned.
    pub fn remove(&mut self, id: u32) -> Option<T> {
        let item = self.items.remove(&id)?;
        self.empty_idx.push(id);
        Some(item)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.len() == 0
    }

    pub fn items(&self) -> &HashMap<u32, T> {
        &self.items
    }

    pub fn items_mut(&mut self) -> &mut HashMap<u32, T> {
        &mut self.items
    }

    pub fn into_items(self) -> HashMap<u32, T> {
        self.items
    }
}

impl<T> Default for HashIndexer<T> {
    fn default() -> Self {
        HashIndexer::new()
    }
}
