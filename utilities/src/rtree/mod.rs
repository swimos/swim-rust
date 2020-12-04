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

//! Immutable R-tree implementation.
//!
//! The module provides traits for implementing custom 2D and 3D objects that can be stored in the R-tree.
pub use super::rect;
pub use crate::rtree::rectangles::*;
pub use crate::rtree::strategies::*;
use num::traits::Pow;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[macro_use]
mod rectangles;
mod strategies;

#[cfg(test)]
mod tests;

/// Immutable tree data structure for efficient storage and retrieval of multi-dimensional information.
///
/// The R-tree can be created by incrementally inserting elements and supports
/// two different node splitting strategies: Linear and Quadratic.
///
/// An R-tree can also be created by bulk-loading elements using the Sort-Tile-Recursive (STR).
#[derive(Debug, Clone)]
pub struct RTree<B>
where
    B: BoxBounded,
{
    root: Node<B>,
    len: usize,
}

impl<B> RTree<B>
where
    B: BoxBounded,
{
    /// Creates a new R-rtree.
    ///
    /// Each node of the tree has a minimum and maximum capacity specified by
    /// `min_children` and `max_children` respectively. The minimum capacity must be less than or equal to
    /// half of the maximum capacity. i.e. `min <= max / 2`
    ///
    /// If the maximum capacity in a node is exceeded, the node is split into two using the provided
    /// split strategy.
    ///
    /// If a node has less elements that the minimum capacity after removal, the remaining elements
    /// in the node are merged back with the rest of the tree.
    ///
    /// # Example:
    /// ```
    /// use utilities::rtree::{Point2D, Rect, RTree, SplitStrategy, rect};
    /// use std::num::NonZeroUsize;
    /// let mut rtree = RTree::new(NonZeroUsize::new(5).unwrap(), NonZeroUsize::new(10).unwrap(), SplitStrategy::Linear);
    ///
    /// rtree.insert(rect!((0.0, 0.0), (1.0, 1.0)));
    ///
    /// assert_eq!(rtree.len(), 1)
    /// ```
    ///
    /// # Panics:
    /// ```should_panic
    /// # use utilities::rtree::{Point2D, Rect, RTree, SplitStrategy};
    /// # use std::num::NonZeroUsize;
    /// #
    /// // min cannot be greater than half of max
    /// let rtree: RTree<Rect<Point2D<f64>>> = RTree::new(NonZeroUsize::new(6).unwrap(), NonZeroUsize::new(10).unwrap(), SplitStrategy::Linear);
    /// ```
    pub fn new(
        min_children: NonZeroUsize,
        max_children: NonZeroUsize,
        split_strat: SplitStrategy,
    ) -> Self {
        check_children(&min_children, &max_children);

        RTree {
            root: Node::new_root(min_children.get(), max_children.get(), split_strat),
            len: 0,
        }
    }

    /// Returns the number of items in the tree.
    ///
    /// # Example:
    /// ```
    /// use utilities::rtree::{Point2D, Rect, RTree, SplitStrategy, rect};
    /// use std::num::NonZeroUsize;
    /// let mut rtree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(5).unwrap(), SplitStrategy::Linear);
    ///
    /// rtree.insert(rect!((0.0, 0.0), (1.0, 1.0)));
    /// assert_eq!(rtree.len(), 1);
    ///
    /// rtree.insert(rect!((0.0, 0.0), (2.0, 2.0)));
    /// assert_eq!(rtree.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether or not the tree has any items.
    ///
    /// # Example:
    /// ```
    /// use utilities::rtree::{Point2D, Rect, RTree, SplitStrategy, rect};
    /// use std::num::NonZeroUsize;
    /// let mut rtree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(5).unwrap(), SplitStrategy::Linear);
    ///
    /// assert!(rtree.is_empty());
    ///
    /// rtree.insert(rect!((0.0, 0.0), (1.0, 1.0)));
    ///
    /// assert!(!rtree.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a list of all elements that are enclosed completely by the given area.
    /// If no such entries are found, `None` is returned.
    ///
    /// # Example:
    /// ```
    /// use utilities::rtree::{Point2D, Rect, RTree, SplitStrategy, rect};
    /// use std::num::NonZeroUsize;
    /// let mut rtree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(5).unwrap(), SplitStrategy::Linear);
    ///
    /// let first_item = rect!((0.0, 0.0), (1.0, 1.0));
    /// let second_item = rect!((0.0, 0.0), (2.0, 2.0));
    ///
    /// rtree.insert(first_item.clone());
    /// rtree.insert(second_item.clone());
    ///
    /// let maybe_found = rtree.search(&rect!((0.0, 0.0), (1.5, 1.5)));
    /// assert_eq!(maybe_found.unwrap(), vec![&first_item]);
    ///
    /// let maybe_found = rtree.search(&rect!((-10.0, -20.0), (-5.0, -10.0)));
    /// assert!(maybe_found.is_none());
    ///
    /// let maybe_found = rtree.search(&rect!((0.0, 0.0), (3.0, 3.0)));
    /// assert_eq!(maybe_found.unwrap(), vec![&first_item, &second_item]);
    /// ```
    pub fn search(&self, area: &Rect<B::Point>) -> Option<Vec<&B>> {
        self.root.search(area)
    }

    /// Inserts a new item in the tree.
    ///
    /// # Example:
    /// ```
    /// use utilities::rtree::{Point2D, Rect, RTree, SplitStrategy, rect};
    /// use std::num::NonZeroUsize;
    /// let mut rtree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(5).unwrap(), SplitStrategy::Linear);
    ///
    /// rtree.insert(rect!((0.0, 0.0), (1.0, 1.0)));
    /// assert_eq!(rtree.len(), 1);
    ///
    /// rtree.insert(rect!((0.0, 0.0), (2.0, 2.0)));
    /// assert_eq!(rtree.len(), 2);
    /// ```
    pub fn insert(&mut self, item: B) {
        self.internal_insert(Arc::new(Entry::Leaf { item }), 0);
        self.len += 1;
    }

    /// Removes and returns an item from the tree that has bounding box equal to the given bounding box.
    /// If no such item is found, `None` is returned.
    /// If multiple items have a matching bounding box, only the first one is removed.
    ///
    /// # Example:
    /// ```
    /// use utilities::rtree::{Point2D, Rect, RTree, SplitStrategy, rect};
    /// use std::num::NonZeroUsize;
    /// let mut rtree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(5).unwrap(), SplitStrategy::Linear);
    ///
    /// let first_item = rect!((0.0, 0.0), (1.0, 1.0));
    /// let second_item = rect!((0.0, 0.0), (2.0, 2.0));
    ///
    /// rtree.insert(first_item.clone());
    /// assert_eq!(rtree.len(), 1);
    ///
    /// rtree.insert(second_item.clone());
    /// assert_eq!(rtree.len(), 2);
    ///
    /// let maybe_removed = rtree.remove(&rect!((0.0, 0.0), (2.0, 2.0)));
    /// assert_eq!(maybe_removed.unwrap(), second_item);
    /// assert_eq!(rtree.len(), 1);
    ///
    /// let maybe_removed = rtree.remove(&rect!((50.0, 60.0), (60.0, 70.0)));
    /// assert!(maybe_removed.is_none());
    /// assert_eq!(rtree.len(), 1);
    ///
    /// let maybe_removed = rtree.remove(&rect!((0.0, 0.0), (1.0, 1.0)));
    /// assert_eq!(maybe_removed.unwrap(), first_item);
    /// assert_eq!(rtree.len(), 0);
    /// ```
    pub fn remove(&mut self, bounding_box: &Rect<B::Point>) -> Option<B> {
        let (removed, maybe_orphan_nodes) = self.root.remove(bounding_box)?;
        self.len -= 1;

        if self.root.num_entries() == 1 && !self.root.is_leaf() {
            let entry_ptr = self.root.entries.pop().unwrap();

            let entry = if Arc::strong_count(&entry_ptr) == 1 {
                Arc::try_unwrap(entry_ptr).unwrap()
            } else {
                (*entry_ptr).clone()
            };

            match entry {
                Entry::Branch { child, .. } => self.root = child,
                Entry::Leaf { .. } => (),
            }
        }

        if maybe_orphan_nodes.is_some() {
            for orphan in maybe_orphan_nodes? {
                match *orphan {
                    Entry::Leaf { .. } => self.internal_insert(orphan, 0),
                    Entry::Branch {
                        child:
                            Node {
                                ref entries, level, ..
                            },
                        ..
                    } => {
                        for entry in entries {
                            self.internal_insert(entry.clone(), level)
                        }
                    }
                }
            }
        }

        Some(removed)
    }

    /// Creates a new R-tree from a list of items.
    ///
    /// The items are loaded into the tree using the Sort-Tile-Recursive (STR) algorithm.
    ///
    /// Each node of the tree has a minimum and maximum capacity specified by
    /// `min_children` and `max_children` respectively. The minimum capacity must be less than or equal to
    /// half of the maximum capacity. i.e. `min <= max / 2`
    ///
    /// The split strategy defines which algorithm will be used when a node needs to be split into two.
    ///
    /// # Example:
    /// ```
    /// use utilities::rtree::{Point2D, Rect, RTree, SplitStrategy, rect};          
    /// use std::num::NonZeroUsize;
    ///
    /// let items = vec![
    ///         rect!((0.0, 0.0), (10.0, 10.0)),
    ///         rect!((12.0, 0.0), (15.0, 15.0)),
    ///         rect!((7.0, 7.0), (14.0, 14.0)),
    ///         rect!((10.0, 11.0), (11.0, 12.0)),
    ///         rect!((4.0, 4.0), (5.0, 6.0)),
    ///         rect!((4.0, 9.0), (5.0, 11.0)),
    ///         rect!((13.0, 0.0), (14.0, 1.0)),
    ///         rect!((13.0, 13.0), (16.0, 16.0)),
    ///         rect!((2.0, 13.0), (4.0, 16.0)),
    ///         rect!((2.0, 2.0), (3.0, 3.0)),
    ///         rect!((10.0, 0.0), (12.0, 5.0)),
    ///         rect!((7.0, 3.0), (8.0, 6.0)),
    ///     ];
    ///
    /// let rtree = RTree::bulk_load(
    ///     NonZeroUsize::new(2).unwrap(),
    ///     NonZeroUsize::new(4).unwrap(),
    ///     SplitStrategy::Quadratic,
    ///     items,
    /// );
    ///
    /// assert_eq!(rtree.len(), 12);
    /// ```
    pub fn bulk_load(
        min_children: NonZeroUsize,
        max_children: NonZeroUsize,
        split_strat: SplitStrategy,
        items: Vec<B>,
    ) -> RTree<B> {
        check_children(&min_children, &max_children);

        let items_num = items.len();

        let items = items
            .into_iter()
            .map(|item| Arc::new(Entry::Leaf { item }))
            .collect();

        let root = RTree::internal_bulk_load(
            min_children.get(),
            max_children.get(),
            split_strat,
            items,
            0,
        );

        RTree {
            root,
            len: items_num,
        }
    }

    fn internal_insert(&mut self, item: EntryPtr<B>, level: usize) {
        if let Some((first_entry, second_entry)) = self.root.insert(item, level) {
            self.root = Node {
                entries: vec![first_entry, second_entry],
                level: self.root.level + 1,
                min_children: self.root.min_children,
                max_children: self.root.max_children,
                split_strat: self.root.split_strat,
            };
        }
    }

    fn internal_bulk_load(
        min_children: usize,
        max_children: usize,
        split_strat: SplitStrategy,
        mut entries: Vec<EntryPtr<B>>,
        mut level: usize,
    ) -> Node<B> {
        let mut entries_count = entries.len();

        while entries_count > max_children {
            // We choose to fill the nodes halfway between the min and max capacity to avoid splits and merges after a single insert/remove
            let node_capacity = (max_children + min_children) / 2;
            let coord_count = B::Point::get_coord_count();

            // Sort all by the first dimension
            entries.sort_by(|first, second| {
                let first_center = first.get_mbb().get_center();
                let second_center = second.get_mbb().get_center();

                first_center
                    .get_nth_coord(0)
                    .unwrap()
                    .partial_cmp(&second_center.get_nth_coord(0).unwrap())
                    .unwrap()
            });

            let mut chunks = vec![entries];

            //Split and sort by every dimension after the first
            for dim in 1..coord_count {
                let entries_count = chunks.get(0).unwrap().len();
                let coord_count = coord_count - dim + 1;
                let mut axis_chunks = vec![];
                let chunk_size = calculate_chunk_size(node_capacity, coord_count, entries_count);

                for items in chunks {
                    let sort_by_dim = |mut items: Vec<EntryPtr<B>>| {
                        items.sort_by(|first, second| {
                            let first_center = first.get_mbb().get_center();
                            let second_center = second.get_mbb().get_center();

                            first_center
                                .get_nth_coord(dim)
                                .unwrap()
                                .partial_cmp(&second_center.get_nth_coord(dim).unwrap())
                                .unwrap()
                        });
                        items
                    };

                    axis_chunks.extend(into_chunks(items, chunk_size, sort_by_dim));
                }

                chunks = axis_chunks;
            }

            //Pack into entries
            entries = vec![];

            for chunk in chunks {
                let construct_entry = |items: Vec<EntryPtr<B>>| {
                    let mut items_iter = items.iter();
                    let first_mbb = *items_iter.next().unwrap().get_mbb();
                    let mbb = items
                        .iter()
                        .fold(first_mbb, |acc, item| acc.combine_boxes(item.get_mbb()));

                    let node = Node {
                        entries: items,
                        level,
                        min_children,
                        max_children,
                        split_strat,
                    };

                    Arc::new(Entry::Branch { mbb, child: node })
                };

                entries.extend(into_chunks(chunk, node_capacity, construct_entry));
            }

            level += 1;
            entries_count = entries.len();
        }

        Node {
            entries,
            level,
            min_children,
            max_children,
            split_strat,
        }
    }
}

fn into_chunks<Input, Output, F>(items: Vec<Input>, chunk_size: usize, transform: F) -> Vec<Output>
where
    F: Fn(Vec<Input>) -> Output,
{
    let mut output = vec![];
    let mut chunk = vec![];
    let total_size = items.len();

    for (i, item) in items.into_iter().enumerate() {
        chunk.push(item);

        if (i + 1) % chunk_size == 0 || (i + 1) == total_size {
            output.push(transform(chunk));
            chunk = vec![];
        }
    }

    output
}

fn calculate_chunk_size(node_capacity: usize, coord_count: usize, entries_count: usize) -> usize {
    let leaf_pages = (entries_count as f64 / node_capacity as f64).ceil();

    let vertical_chunks = if coord_count == 2 {
        leaf_pages.sqrt()
    } else if coord_count == 3 {
        leaf_pages.cbrt()
    } else {
        panic!("Only 2D and 3D data is supported!")
    };

    let chunk_size = node_capacity * (vertical_chunks.pow((coord_count - 1) as f64) as usize);
    chunk_size as usize
}

fn check_children(min_children: &NonZeroUsize, max_children: &NonZeroUsize) {
    assert!(min_children.get() <= max_children.get() / 2, "The minimum number of children cannot be more than half of the maximum number of children.");
}

#[derive(Debug, Clone)]
pub(in crate) struct Node<B>
where
    B: BoxBounded,
{
    entries: Vec<EntryPtr<B>>,
    level: usize,
    min_children: usize,
    max_children: usize,
    split_strat: SplitStrategy,
}

impl<B> Node<B>
where
    B: BoxBounded,
{
    fn new_root(min_children: usize, max_children: usize, split_strat: SplitStrategy) -> Self {
        Node {
            entries: Vec::new(),
            level: 0,
            min_children,
            max_children,
            split_strat,
        }
    }

    fn num_entries(&self) -> usize {
        self.entries.len()
    }

    fn is_leaf(&self) -> bool {
        self.level == 0
    }

    fn search(&self, area: &Rect<B::Point>) -> Option<Vec<&B>> {
        let mut found = vec![];

        if self.is_leaf() {
            for entry in &self.entries {
                match **entry {
                    Entry::Leaf { item: ref entry } if area.is_covering(entry.get_mbb()) => {
                        found.push(entry);
                    }
                    _ => (),
                }
            }
        } else {
            for entry in &self.entries {
                if area.is_intersecting(entry.get_mbb()) {
                    match entry.search(area) {
                        None => {}
                        Some(matching) => found.extend(matching),
                    }
                }
            }
        }

        if found.is_empty() {
            None
        } else {
            Some(found)
        }
    }

    fn insert(&mut self, item: EntryPtr<B>, level: usize) -> MaybeSplit<B> {
        match *item {
            //If we have a branch and we are at the right level -> insert
            Entry::Branch { .. } if self.level == level => {
                self.entries.push(item);

                if self.entries.len() > self.max_children {
                    let split_entries = self.split();
                    return Some(split_entries);
                }
            }

            _ => {
                //If we are at a leaf -> insert
                if self.is_leaf() {
                    self.entries.push(item);
                    if self.entries.len() > self.max_children {
                        let split_entries = self.split();
                        return Some(split_entries);
                    }
                } else {
                    //If we are at a branch but not at the right level -> go deeper
                    let mut entries_iter = self.entries.iter_mut();

                    let mut min_entry = entries_iter.next().unwrap();
                    let mut min_entry_idx = 0;
                    let mut min_rect = min_entry.get_mbb().combine_boxes(item.get_mbb());
                    let mut min_diff = min_rect.measure() - min_entry.get_mbb().measure();

                    for (entry, idx) in entries_iter.zip(1..) {
                        let expanded_rect = entry.get_mbb().combine_boxes(item.get_mbb());
                        let diff = expanded_rect.measure() - entry.get_mbb().measure();

                        if diff < min_diff {
                            min_diff = diff;
                            min_rect = expanded_rect;
                            min_entry = entry;
                            min_entry_idx = idx;
                        }
                    }

                    let min_entry = Arc::make_mut(&mut min_entry);

                    if let Some((first_entry, second_entry)) =
                        min_entry.insert(item, min_rect, level)
                    {
                        self.entries.remove(min_entry_idx);
                        self.entries.push(first_entry);
                        self.entries.push(second_entry);

                        if self.entries.len() > self.max_children {
                            let split_entries = self.split();
                            return Some(split_entries);
                        }
                    }
                }
            }
        }
        None
    }

    fn remove(&mut self, bounding_box: &Rect<B::Point>) -> Option<(B, MaybeOrphans<B>)> {
        if self.is_leaf() {
            //If this is leaf try to find the item
            let mut remove_idx = None;

            for (idx, entry) in self.entries.iter().enumerate() {
                match **entry {
                    Entry::Leaf { item: ref entry } if entry.get_mbb() == bounding_box => {
                        remove_idx = Some(idx);
                        break;
                    }
                    _ => (),
                }
            }

            let entry_ptr = self.entries.remove(remove_idx?);
            let entry = if Arc::strong_count(&entry_ptr) == 1 {
                Arc::try_unwrap(entry_ptr).unwrap()
            } else {
                (*entry_ptr).clone()
            };

            if let Entry::Leaf { item } = entry {
                Some((item, None))
            } else {
                None
            }
        } else {
            // If this is a branch, go deeper
            let mut entry_index = None;
            let mut maybe_removed = None;

            for (idx, entry) in self.entries.iter_mut().enumerate() {
                if entry.get_mbb().is_covering(bounding_box) {
                    let entry = Arc::make_mut(entry);
                    maybe_removed = entry.remove(bounding_box);

                    if maybe_removed.is_some() {
                        if entry.len() < self.min_children {
                            entry_index = Some(idx);
                        }
                        break;
                    }
                }
            }

            let (removed, maybe_orphan_nodes) = maybe_removed?;

            if let Some(entry_index) = entry_index {
                let orphan = self.entries.remove(entry_index);

                match maybe_orphan_nodes {
                    Some(mut orphan_nodes) => {
                        orphan_nodes.push(orphan);
                        Some((removed, Some(orphan_nodes)))
                    }
                    None => Some((removed, Some(vec![orphan]))),
                }
            } else {
                Some((removed, maybe_orphan_nodes))
            }
        }
    }

    fn split(&mut self) -> (EntryPtr<B>, EntryPtr<B>) {
        let ((first_group, first_mbb), (second_group, second_mbb)) =
            split(&mut self.entries, self.min_children, self.split_strat);

        let first_group = Entry::Branch {
            mbb: first_mbb,
            child: Node {
                entries: first_group,
                level: self.level,
                min_children: self.min_children,
                max_children: self.max_children,
                split_strat: self.split_strat,
            },
        };

        let second_group = Entry::Branch {
            mbb: second_mbb,
            child: Node {
                entries: second_group,
                level: self.level,
                min_children: self.min_children,
                max_children: self.max_children,
                split_strat: self.split_strat,
            },
        };

        (Arc::new(first_group), Arc::new(second_group))
    }
}

fn split<B>(
    entries: &mut Vec<EntryPtr<B>>,
    min_children: usize,
    split_strat: SplitStrategy,
) -> (SplitGroup<B>, SplitGroup<B>)
where
    B: BoxBounded,
{
    let (first_seed_idx, second_seed_idx) = match split_strat {
        SplitStrategy::Linear => linear_pick_seeds(entries),
        SplitStrategy::Quadratic => quadratic_pick_seeds(entries),
    };

    // second_seed_idx > first_seed_idx
    let second_seed = entries.remove(second_seed_idx);
    let first_seed = entries.remove(first_seed_idx);

    let mut first_mbb = *first_seed.get_mbb();
    let mut first_group = vec![first_seed];

    let mut second_mbb = *second_seed.get_mbb();
    let mut second_group = vec![second_seed];

    while !entries.is_empty() {
        if entries.len() + first_group.len() == min_children {
            for item in entries.drain(..) {
                let expanded_rect = first_mbb.combine_boxes(item.get_mbb());

                first_mbb = expanded_rect;
                first_group.push(item);
            }
        } else if entries.len() + second_group.len() == min_children {
            for item in entries.drain(..) {
                let expanded_rect = second_mbb.combine_boxes(item.get_mbb());

                second_mbb = expanded_rect;
                second_group.push(item);
            }
        } else {
            let (idx, expanded_rect, group) = match split_strat {
                SplitStrategy::Linear => pick_next_linear(entries, &first_mbb),
                SplitStrategy::Quadratic => pick_next_quadratic(
                    entries,
                    &first_mbb,
                    &second_mbb,
                    first_group.len(),
                    second_group.len(),
                ),
            };

            let item = entries.remove(idx);

            match group {
                Group::First => {
                    first_mbb = expanded_rect;
                    first_group.push(item);
                }
                Group::Second => {
                    second_mbb = expanded_rect;
                    second_group.push(item);
                }
            };
        }
    }

    ((first_group, first_mbb), (second_group, second_mbb))
}

type EntryPtr<B> = Arc<Entry<B>>;
type MaybeOrphans<B> = Option<Vec<EntryPtr<B>>>;
type MaybeSplit<B> = Option<(EntryPtr<B>, EntryPtr<B>)>;
type SplitGroup<B> = (Vec<EntryPtr<B>>, Rect<<B as BoxBounded>::Point>);

#[derive(Debug, Clone)]
pub(in crate) enum Entry<B>
where
    B: BoxBounded,
{
    Leaf { item: B },
    Branch { mbb: Rect<B::Point>, child: Node<B> },
}

impl<B> Entry<B>
where
    B: BoxBounded,
{
    fn len(&self) -> usize {
        match self {
            Entry::Leaf { .. } => 0,
            Entry::Branch { child, .. } => child.num_entries(),
        }
    }

    fn search(&self, area: &Rect<B::Point>) -> Option<Vec<&B>> {
        match self {
            Entry::Branch { child, .. } => child.search(area),
            Entry::Leaf { .. } => unreachable!(),
        }
    }

    fn get_mbb(&self) -> &Rect<B::Point> {
        match self {
            Entry::Leaf { item } => item.get_mbb(),
            Entry::Branch { mbb, .. } => mbb,
        }
    }

    fn insert(
        &mut self,
        item: EntryPtr<B>,
        expanded_rect: Rect<B::Point>,
        level: usize,
    ) -> MaybeSplit<B> {
        match self {
            Entry::Branch { mbb, child } => {
                *mbb = expanded_rect;
                child.insert(item, level)
            }
            Entry::Leaf { .. } => unreachable!(),
        }
    }

    fn remove(&mut self, bounding_box: &Rect<B::Point>) -> Option<(B, MaybeOrphans<B>)> {
        match self {
            Entry::Branch { mbb, child } => {
                let (removed, orphan_nodes) = child.remove(bounding_box)?;

                let removed_mbb = removed.get_mbb();

                if removed_mbb.low.has_any_matching_coords(&mbb.low)
                    || removed_mbb.high.has_any_matching_coords(&mbb.high)
                {
                    let mut entries_iter = child.entries.iter();
                    let mut shrunken_mbb = *entries_iter.next().unwrap().get_mbb();
                    shrunken_mbb = entries_iter.fold(shrunken_mbb, |acc, entry| {
                        entry.get_mbb().combine_boxes(&acc)
                    });

                    *mbb = shrunken_mbb;
                }

                Some((removed, orphan_nodes))
            }

            Entry::Leaf { .. } => unreachable!(),
        }
    }
}
