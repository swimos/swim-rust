//! Immutable R-tree implementation.
//!
//! The crate provides traits for implementing custom 2D and 3D objects that can be stored in the R-tree.
pub use super::rect;
pub use crate::rtree::rectangles::*;
use num::traits::real::Real;
use num::traits::Pow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[macro_use]
mod rectangles;

#[cfg(test)]
mod tests;

/// The strategy that will be used to split the nodes of the [`RTree`](struct.RTree.html), once the maximum capacity is reached.
///
/// The two supported strategies run in linear and quadratic time.
#[derive(Debug, Clone, Copy)]
pub enum Strategy {
    Linear,
    Quadratic,
}

/// Immutable tree data structure for efficient storage and retrieval of multi-dimensional information.
/// Todo
#[derive(Debug, Clone)]
pub struct RTree<B>
where
    B: BoundingBox,
{
    root: Node<B>,
    len: usize,
}

impl<B> RTree<B>
where
    B: BoundingBox,
{
    ///Todo
    pub fn new(
        min_children: NonZeroUsize,
        max_children: NonZeroUsize,
        split_strat: Strategy,
    ) -> Self {
        check_children(&min_children, &max_children);

        RTree {
            root: Node::new_root(min_children.get(), max_children.get(), split_strat),
            len: 0,
        }
    }

    ///Todo
    pub fn len(&self) -> usize {
        self.len
    }

    ///Todo
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    ///Todo
    pub fn search(&self, area: &Rect<B::Point>) -> Option<Vec<&B>> {
        self.root.search(area)
    }

    ///Todo
    pub fn insert(&mut self, item: B) {
        self.internal_insert(Arc::new(Entry::Leaf { item }), 0);
        self.len += 1;
    }

    ///Todo
    pub fn remove(&mut self, bounding_box: &Rect<B::Point>) -> Option<B> {
        let (removed, maybe_orphan_nodes) = self.root.remove(bounding_box)?;
        self.len -= 1;

        if self.root.len() == 1 && !self.root.is_leaf() {
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

    ///Todo
    pub fn bulk_load(
        min_children: NonZeroUsize,
        max_children: NonZeroUsize,
        split_strat: Strategy,
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
        split_strat: Strategy,
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

            let mut slices = vec![entries];

            //Split and sort by every dimension after the first
            for dim in 1..coord_count {
                let entries_count = slices.get(0).unwrap().len();
                let coord_count = coord_count - dim + 1;
                let mut axis_slices = vec![];
                let slice_size = calculate_slice_size(node_capacity, coord_count, entries_count);

                for items in slices {
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

                    axis_slices.extend(into_slices(items, slice_size, sort_by_dim));
                }

                slices = axis_slices;
            }

            //Pack into entries
            entries = vec![];

            for slice in slices {
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

                entries.extend(into_slices(slice, node_capacity, construct_entry));
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

fn into_slices<Input, Output, F>(items: Vec<Input>, slice_size: usize, transform: F) -> Vec<Output>
where
    F: Fn(Vec<Input>) -> Output,
{
    let mut output = vec![];
    let mut slice = vec![];
    let total_size = items.len();

    for (i, item) in items.into_iter().enumerate() {
        slice.push(item);

        if (i + 1) % slice_size == 0 || (i + 1) == total_size {
            output.push(transform(slice));
            slice = vec![];
        }
    }

    output
}

fn calculate_slice_size(node_capacity: usize, coord_count: usize, entries_count: usize) -> usize {
    let leaf_pages = (entries_count as f64 / node_capacity as f64).ceil();

    let vertical_slices = if coord_count == 2 {
        leaf_pages.sqrt()
    } else if coord_count == 3 {
        leaf_pages.cbrt()
    } else {
        panic!("Only 2D and 3D data is supported!")
    };

    let slice_size = node_capacity * (vertical_slices.pow((coord_count - 1) as f64) as usize);
    slice_size as usize
}

fn check_children(min_children: &NonZeroUsize, max_children: &NonZeroUsize) {
    if min_children.get() > max_children.get() / 2 {
        panic!("The minimum number of children cannot be more than half of the maximum number of children.")
    }
}

#[derive(Debug, Clone)]
struct Node<B>
where
    B: BoundingBox,
{
    entries: Vec<EntryPtr<B>>,
    level: usize,
    min_children: usize,
    max_children: usize,
    split_strat: Strategy,
}

impl<B> Node<B>
where
    B: BoundingBox,
{
    fn new_root(min_children: usize, max_children: usize, split_strat: Strategy) -> Self {
        Node {
            entries: Vec::new(),
            level: 0,
            min_children,
            max_children,
            split_strat,
        }
    }

    fn len(&self) -> usize {
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
    split_strat: Strategy,
) -> (SplitGroup<B>, SplitGroup<B>)
where
    B: BoundingBox,
{
    let (first_seed_idx, second_seed_idx) = match split_strat {
        Strategy::Linear => linear_pick_seeds(entries),
        Strategy::Quadratic => quadratic_pick_seeds(entries),
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
                Strategy::Linear => pick_next_linear(entries, &first_mbb),
                Strategy::Quadratic => pick_next_quadratic(
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

fn quadratic_pick_seeds<B>(entries: &[EntryPtr<B>]) -> (usize, usize)
where
    B: BoundingBox,
{
    let mut first_idx = 0;
    let mut second_idx = 1;
    let mut max_diff = None;

    if entries.len() > 2 {
        for (i, first_item) in entries.iter().enumerate() {
            for (j, second_item) in entries.iter().enumerate().skip(i + 1) {
                let combined_rect = first_item.get_mbb().combine_boxes(second_item.get_mbb());
                let diff = combined_rect.measure()
                    - first_item.get_mbb().measure()
                    - second_item.get_mbb().measure();

                if let Some(max) = max_diff {
                    if diff > max {
                        max_diff = Some(diff);
                        first_idx = i;
                        second_idx = j;
                    }
                } else {
                    max_diff = Some(diff);
                }
            }
        }
    }

    (first_idx, second_idx)
}

fn pick_next_quadratic<B>(
    entries: &[EntryPtr<B>],
    first_mbb: &Rect<B::Point>,
    second_mbb: &Rect<B::Point>,
    first_group_size: usize,
    second_group_size: usize,
) -> (usize, Rect<B::Point>, Group)
where
    B: BoundingBox,
{
    let mut entries_iter = entries.iter();
    let item = entries_iter.next().unwrap();
    let mut item_idx = 0;

    let ((first_preference, first_expanded_rect), (second_preference, second_expanded_rect)) =
        calc_preferences(item, first_mbb, second_mbb);

    let mut max_preference_diff = (first_preference - second_preference).abs();

    let mut group = select_group::<B>(
        first_mbb,
        second_mbb,
        first_group_size,
        second_group_size,
        first_preference,
        second_preference,
    );

    let mut expanded_rect = match group {
        Group::First => first_expanded_rect,
        Group::Second => second_expanded_rect,
    };

    for (item, idx) in entries_iter.zip(1..) {
        let ((first_preference, first_expanded_rect), (second_preference, second_expanded_rect)) =
            calc_preferences(item, first_mbb, second_mbb);
        let preference_diff = (first_preference - second_preference).abs();

        if max_preference_diff <= preference_diff {
            max_preference_diff = preference_diff;
            item_idx = idx;

            group = select_group::<B>(
                first_mbb,
                second_mbb,
                first_group_size,
                second_group_size,
                first_preference,
                second_preference,
            );

            expanded_rect = match group {
                Group::First => first_expanded_rect,
                Group::Second => second_expanded_rect,
            };
        }
    }

    (item_idx, expanded_rect, group)
}

type PointType<B> = <<B as BoundingBox>::Point as Point>::Type;

fn linear_pick_seeds<B>(entries: &[EntryPtr<B>]) -> (usize, usize)
where
    B: BoundingBox,
{
    let mut first_idx = 0;
    let mut second_idx = 1;

    let mut dim_boundary_points: Vec<(PointType<B>, PointType<B>)> = vec![];
    let mut max_low_sides: Vec<(usize, PointType<B>)> = vec![];
    let mut min_high_sides: Vec<(usize, PointType<B>)> = vec![];

    if entries.len() > 2 {
        for (i, item) in entries.iter().enumerate() {
            let mbb = item.get_mbb();

            for dim in 0..mbb.get_coord_count() {
                let low_dim = mbb.get_low().get_nth_coord(dim).unwrap();
                let high_dim = mbb.get_high().get_nth_coord(dim).unwrap();

                match dim_boundary_points.get_mut(dim) {
                    Some((min_low, max_high)) => {
                        if low_dim < *min_low {
                            *min_low = low_dim
                        }

                        if high_dim > *max_high {
                            *max_high = high_dim
                        }
                    }
                    None => dim_boundary_points.push((low_dim, high_dim)),
                }

                match max_low_sides.get_mut(dim) {
                    Some((idx, max_low_dim)) => {
                        if low_dim > *max_low_dim {
                            *idx = i;
                            *max_low_dim = low_dim
                        }
                    }
                    None => max_low_sides.push((i, low_dim)),
                }

                match min_high_sides.get_mut(dim) {
                    Some((idx, min_high_dim)) => {
                        if high_dim < *min_high_dim {
                            *idx = i;
                            *min_high_dim = high_dim
                        }
                    }
                    None => min_high_sides.push((i, high_dim)),
                }
            }
        }

        let dim_lengths: Vec<_> = dim_boundary_points
            .into_iter()
            .map(|(low, high)| (high - low).abs())
            .collect();

        let side_separations: Vec<_> = max_low_sides
            .into_iter()
            .zip(min_high_sides.into_iter())
            .map(|((idx_low, low), (idx_high, high))| (idx_low, idx_high, (high - low).abs()))
            .collect();

        let normalised_separations: Vec<_> = side_separations
            .into_iter()
            .zip(dim_lengths.into_iter())
            .map(|((f, s, separation), dim_len)| (f, s, separation / dim_len))
            .collect();

        let max_separation = normalised_separations
            .into_iter()
            .max_by(|(_, _, norm_sep_1), (_, _, norm_sep_2)| {
                norm_sep_1.partial_cmp(norm_sep_2).unwrap()
            })
            .unwrap();

        first_idx = max_separation.0;
        second_idx = max_separation.1;
    }

    let (first_idx, second_idx) = match first_idx.cmp(&second_idx) {
        Ordering::Less => (first_idx, second_idx),
        Ordering::Greater => (second_idx, first_idx),
        Ordering::Equal if first_idx == 0 => (first_idx, 1),
        Ordering::Equal => (0, second_idx),
    };

    (first_idx, second_idx)
}

fn pick_next_linear<B>(
    entries: &[EntryPtr<B>],
    mbb: &Rect<B::Point>,
) -> (usize, Rect<B::Point>, Group)
where
    B: BoundingBox,
{
    (
        0,
        mbb.combine_boxes(entries.get(0).unwrap().get_mbb()),
        Group::First,
    )
}

type Preference<B> = (
    (PointType<B>, Rect<<B as BoundingBox>::Point>),
    (PointType<B>, Rect<<B as BoundingBox>::Point>),
);

fn calc_preferences<B>(
    item: &EntryPtr<B>,
    first_mbb: &Rect<B::Point>,
    second_mbb: &Rect<B::Point>,
) -> Preference<B>
where
    B: BoundingBox,
{
    let first_expanded_rect = first_mbb.combine_boxes(item.get_mbb());
    let first_diff = first_expanded_rect.measure() - first_mbb.measure();

    let second_expanded_rect = second_mbb.combine_boxes(item.get_mbb());
    let second_diff = second_expanded_rect.measure() - second_mbb.measure();

    (
        (first_diff, first_expanded_rect),
        (second_diff, second_expanded_rect),
    )
}

fn select_group<B>(
    first_mbb: &Rect<B::Point>,
    second_mbb: &Rect<B::Point>,
    first_group_size: usize,
    second_group_size: usize,
    first_diff: <B::Point as Point>::Type,
    second_diff: <B::Point as Point>::Type,
) -> Group
where
    B: BoundingBox,
{
    if first_diff < second_diff {
        Group::First
    } else if second_diff < first_diff {
        Group::Second
    } else if first_mbb.measure() < second_mbb.measure() {
        Group::First
    } else if second_mbb.measure() < first_mbb.measure() {
        Group::Second
    } else if first_group_size < second_group_size {
        Group::First
    } else if second_group_size < first_group_size {
        Group::Second
    } else {
        Group::First
    }
}

enum Group {
    First,
    Second,
}

type EntryPtr<B> = Arc<Entry<B>>;
type MaybeOrphans<B> = Option<Vec<EntryPtr<B>>>;
type MaybeSplit<B> = Option<(EntryPtr<B>, EntryPtr<B>)>;
type SplitGroup<B> = (Vec<EntryPtr<B>>, Rect<<B as BoundingBox>::Point>);

#[derive(Debug, Clone)]
enum Entry<B>
where
    B: BoundingBox,
{
    Leaf { item: B },
    Branch { mbb: Rect<B::Point>, child: Node<B> },
}

impl<B> Entry<B>
where
    B: BoundingBox,
{
    fn len(&self) -> usize {
        match self {
            Entry::Leaf { .. } => 0,
            Entry::Branch { child, .. } => child.len(),
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

                if removed_mbb.get_low().has_equal_cords(mbb.get_low())
                    || removed_mbb.get_high().has_equal_cords(mbb.get_high())
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
