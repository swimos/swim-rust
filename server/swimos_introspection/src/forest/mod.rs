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

mod iter;
#[cfg(test)]
mod tests;

use smol_str::SmolStr;
use std::collections::HashMap;
use std::fmt::Debug;
use std::iter::Peekable;

pub use self::iter::{PathSegmentIterator, UriForestIterator, UriPart, UriPartIterator};

static_assertions::assert_impl_all!(UriForest<()>: Send, Sync);

/// A trie-like data structure mapping URIs to an associated value. This struct offers operations
/// for inserting a URI and associating data alongside it, removing URIs and querying all the
/// available URIs or by a prefix.
///
/// While the URI forest is not as time efficient for insertion and removal operations as a map, it
/// is more efficient prefix lookups; such as finding all URIs prefixed by "/host/".
///
/// # Internal representation:
/// From running the following:
/// ```ignore
/// let mut forest = UriForest::new();
///
/// forest.insert("/listener", ());
/// forest.insert("/unit/1/cnt/2", ());
/// forest.insert("/unit/2/cnt/3", ());
/// ```
///
/// The internal representation of the URIs in the struct is is:
/// ```ignore
///                     /
///             listener    unit
///                             \
///                         1       2
///                     /           /
///                 cnt         cnt
///             /                     \
///         2                              3
/// ```
#[derive(Debug)]
pub struct UriForest<D> {
    /// A collection of trees in this forest.
    trees: HashMap<SmolStr, TreeNode<D>>,
}

impl<D> Default for UriForest<D> {
    fn default() -> Self {
        UriForest {
            trees: HashMap::default(),
        }
    }
}

impl<D> UriForest<D> {
    /// Constructs a new URI forest.
    pub fn new() -> UriForest<D> {
        UriForest {
            trees: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.trees.is_empty()
    }

    /// Inserts 'uri' into this forest and associates 'node_data' with it.
    pub fn insert(&mut self, uri: &str, node_data: D) {
        let UriForest { trees } = self;
        let mut segment_iter = PathSegmentIterator::new(uri).peekable();

        if let Some(segment) = segment_iter.next() {
            match trees.get_mut(segment) {
                Some(root) => {
                    // A tree exists in the forest so traverse it until we find where to start
                    // inserting segments
                    traverse_insert(segment, root, segment_iter, node_data)
                }
                None => {
                    // No tree exists, build a new one
                    if segment_iter.peek().is_some() {
                        let node = trees.entry(segment.into()).or_insert(TreeNode::new(None));
                        insert_uri(segment_iter, node, node_data);
                    } else {
                        trees.insert(segment.into(), TreeNode::new(Some(node_data)));
                    }
                }
            }
        }
    }

    /// Attempts to remove 'uri' from this forest, returning any associated data.
    pub fn remove(&mut self, uri: &str) -> Option<D> {
        let UriForest { trees } = self;
        let mut segment_iter = PathSegmentIterator::new(uri).peekable();

        match segment_iter.next() {
            Some(segment) => {
                let data = match trees.get_mut(segment) {
                    Some(root) => {
                        if root.has_descendants() {
                            // The node has descendants that need to be traversed
                            let data = traverse_remove(root, segment_iter);
                            if !root.has_data() && !root.has_descendants() {
                                data
                            } else {
                                return data;
                            }
                        } else {
                            // The node has no descendants so remove it directly
                            None
                        }
                    }
                    None => return None,
                };

                match data {
                    Some(data) => {
                        trees.remove(segment);
                        Some(data)
                    }
                    None => trees.remove(segment)?.data,
                }
            }
            None => None,
        }
    }

    /// Returns an optional mutable reference to the data associated at 'uri'
    pub fn get_mut(&mut self, uri: &str) -> Option<&mut D> {
        let UriForest { trees } = self;
        let mut segment_iter = PathSegmentIterator::new(uri).peekable();

        match segment_iter.next() {
            Some(segment) => {
                match trees.get_mut(segment) {
                    Some(root) => {
                        let mut current_node = root;
                        while let Some(segment) = segment_iter.next() {
                            match (
                                current_node.get_descendant_mut(segment),
                                segment_iter.peek(),
                            ) {
                                (Some(descendant), Some(_)) => {
                                    // We found a matching node and there is another segment to come in
                                    // the path. Update the cursor and carry on.
                                    current_node = descendant;
                                }
                                (Some(node), None) => return node.data.as_mut(),
                                (None, Some(_)) => return None,
                                (None, None) => return None,
                            }
                        }
                        current_node.data.as_mut()
                    }
                    None => None,
                }
            }
            None => None,
        }
    }

    /// Returns whether this URI forest contains 'uri'.
    #[cfg(test)]
    pub fn contains_uri(&self, uri: &str) -> bool {
        let UriForest { trees } = self;
        let mut segment_iter = PathSegmentIterator::new(uri).peekable();

        match segment_iter.next() {
            Some(segment) => {
                match trees.get(segment) {
                    Some(root) => {
                        let mut current_node = root;
                        while let Some(segment) = segment_iter.next() {
                            match (current_node.get_descendant(segment), segment_iter.peek()) {
                                (Some(descendant), Some(_)) => {
                                    // We found a matching node and there is another segment to come in
                                    // the path. Update the cursor and carry on.
                                    current_node = descendant;
                                }
                                (Some(node), None) => {
                                    // We found a matching node but we *only* want to return true iff
                                    // the node has data
                                    return node.has_data();
                                }
                                (None, Some(_)) => {
                                    return false;
                                }
                                (None, None) => {
                                    return false;
                                }
                            }
                        }
                        // This will be reached if the URI only has one segment in its path. We *only*
                        // want to return true iff the node has data.
                        current_node.has_data()
                    }
                    None => {
                        // No tree exists
                        false
                    }
                }
            }
            None => false,
        }
    }

    /// Returns an iterator that will yield every URI in the forest.
    pub fn uri_iter(&self) -> UriForestIterator<'_, D> {
        let UriForest { trees } = self;
        UriForestIterator::new("".to_string(), trees)
    }

    /// Returns an iterator that yields URI parts; either a leaf item containing node data or a
    /// junction item containing the number of descendants.
    pub fn part_iter(&self) -> UriPartIterator<'_, D> {
        let UriForest { trees } = self;
        UriPartIterator::new(trees)
    }
}

fn traverse_remove<'l, D, I>(
    current_node: &mut TreeNode<D>,
    mut segment_iter: Peekable<I>,
) -> Option<D>
where
    I: Iterator<Item = &'l str>,
{
    // Scan down the tree with two cursors. One for the current node and one for the next segment
    // in the URI
    match segment_iter.next() {
        Some(segment) => {
            // Does the current segment exist in the tree?
            return match current_node.get_descendant_mut(segment) {
                // It does. Scan ahead to see if there is another segment in the URI
                Some(descendant) => match segment_iter.peek() {
                    // There is another segment in the URI. We will recursively call ourself if the
                    // next segment exists in the URI or we will return None if it does not
                    Some(next_segment) => {
                        if descendant.has_descendant(next_segment) {
                            // We've made as much progress as we can in this iteration. Recurse
                            let data = traverse_remove(descendant, segment_iter);

                            if !descendant.has_descendants() && !current_node.has_data() {
                                // We want to prune the current node from the tree iff it does not
                                // have any data associated with it and it has no descendants
                                current_node.remove_descendant(segment);
                            }

                            data
                        } else {
                            // The requested node does not exist in the tree
                            None
                        }
                    }
                    // We've reached the end of the URI
                    None => {
                        // This is a junction node so we cannot remove it
                        if descendant.has_descendants() {
                            descendant.take_data()
                        } else {
                            // This is a leaf node, remove it and return the data
                            current_node
                                .remove_descendant(segment)
                                .expect("Missing node")
                                .data
                        }
                    }
                },
                None => {
                    // The requested node does not exist in the tree
                    None
                }
            };
        }
        None => None,
    }
}

fn traverse_insert<'l, D, I>(
    current_segment: &str,
    current_node: &mut TreeNode<D>,
    mut segment_iter: Peekable<I>,
    node_data: D,
) where
    I: Iterator<Item = &'l str>,
{
    if let Some(segment) = segment_iter.next() {
        match current_node.get_descendant_mut(segment) {
            Some(descendant) => {
                if descendant.has_descendants() {
                    if segment_iter.peek().is_some() {
                        traverse_insert(segment, descendant, segment_iter, node_data)
                    } else {
                        // There aren't any more segments in the URI and the descendant node matches
                        // the segment, update the data
                        descendant.update_data(node_data);
                    }
                } else if segment_iter.peek().is_none() {
                    // There aren't any more segments in the URI and the descendant node matches
                    // the segment, update the data
                    descendant.update_data(node_data);
                }
            }
            None => {
                if current_segment == segment {
                    // The current node matches the segment, update the data
                    current_node.update_data(node_data);
                } else if segment_iter.peek().is_none() {
                    // There's no more segments left so insert a new node
                    current_node.add_descendant(segment, TreeNode::new(Some(node_data)));
                } else {
                    // We've reached a leaf. Insert the current node and then write the remaining
                    // URI segments from it
                    let current_node = current_node.add_descendant(segment, TreeNode::new(None));
                    insert_uri(segment_iter, current_node, node_data);
                }
            }
        }
    }
}

fn insert_uri<'l, I, D>(segment_iter: I, mut node: &mut TreeNode<D>, node_data: D)
where
    I: Iterator<Item = &'l str>,
{
    let mut segment_iter = segment_iter.peekable();
    loop {
        match (segment_iter.next(), segment_iter.peek().is_some()) {
            (Some(segment), false) => {
                // There are no more segments remaining, write a leaf node
                node.add_descendant(segment, TreeNode::new(Some(node_data)));
                return;
            }
            (Some(segment), true) => {
                // There are more segments remaining
                node = node.add_descendant(segment, TreeNode::new(None));
            }
            (None, _) => {
                // Unreachable when this function is called with more than one segment in the
                // iterator but it's possible that this function will be called with none
                return;
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct TreeNode<D> {
    data: Option<D>,
    descendants: HashMap<SmolStr, TreeNode<D>>,
}

impl<D> TreeNode<D> {
    fn new(data: Option<D>) -> TreeNode<D> {
        TreeNode {
            data,
            descendants: HashMap::new(),
        }
    }

    fn update_data(&mut self, data: D) {
        self.data = Some(data);
    }

    fn take_data(&mut self) -> Option<D> {
        self.data.take()
    }

    fn has_data(&self) -> bool {
        self.data.is_some()
    }

    fn get_descendant_mut(&mut self, segment: &str) -> Option<&mut TreeNode<D>> {
        self.descendants.get_mut(segment)
    }

    #[cfg(test)]
    fn get_descendant(&self, segment: &str) -> Option<&TreeNode<D>> {
        self.descendants.get(segment)
    }

    fn has_descendant(&mut self, segment: &str) -> bool {
        self.descendants.contains_key(segment)
    }

    fn add_descendant(&mut self, segment: &str, node: TreeNode<D>) -> &mut TreeNode<D> {
        self.descendants.entry(segment.into()).or_insert(node)
    }

    fn has_descendants(&self) -> bool {
        !self.descendants.is_empty()
    }

    fn remove_descendant(&mut self, segment: &str) -> Option<TreeNode<D>> {
        self.descendants.remove(segment)
    }

    fn descendants_len(&self) -> usize {
        self.descendants.len()
    }
}
