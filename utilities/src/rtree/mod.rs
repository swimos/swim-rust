use crate::rtree::rect::{BoundingBox, Coordinate, Point, Rect};
use num::integer::sqrt;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[macro_use]
pub mod rect;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct RTree<C, P, B>
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    root: Node<C, P, B>,
    len: usize,
}

impl<C, P, B> RTree<C, P, B>
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    pub fn new(min_children: NonZeroUsize, max_children: NonZeroUsize) -> Self {
        if min_children.get() > max_children.get() / 2 {
            panic!("The minimum number of children cannot be more than half of the maximum number of children.")
        }

        RTree {
            root: Node::new(min_children.get(), max_children.get()),
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn search(&self, area: &Rect<C, P>) -> Option<Vec<B>> {
        self.root.search(area)
    }

    pub fn insert(&mut self, item: B) {
        self.internal_insert(Arc::new(Entry::Leaf { item }), 0);
        self.len += 1;
    }

    pub fn remove(&mut self, bounding_box: &Rect<C, P>) -> Option<B> {
        let (removed, maybe_orphan_nodes) = self.root.remove(bounding_box)?;
        self.len -= 1;

        if self.root.len() == 1 && !self.root.is_leaf() {
            let entry_ptr = self.root.entries.pop().unwrap();

            let entry = if Arc::strong_count(&entry_ptr) == 1 {
                Arc::try_unwrap(entry_ptr).unwrap_or_else(|_| unreachable!())
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

    pub fn bulk_load(
        min_children: NonZeroUsize,
        max_children: NonZeroUsize,
        items: Vec<B>,
    ) -> RTree<C, P, B> {
        if min_children.get() > max_children.get() / 2 {
            panic!("The minimum number of children cannot be more than half of the maximum number of children.")
        }

        let items_num = items.len();

        let items = items
            .into_iter()
            .map(|item| Arc::new(Entry::Leaf { item }))
            .collect();

        let root = if items_num > max_children.get() {
            RTree::internal_bulk_load(min_children.get(), max_children.get(), items, 0)
        } else {
            Node {
                entries: items,
                level: 0,
                min_children: min_children.get(),
                max_children: max_children.get(),
            }
        };

        RTree {
            root,
            len: items_num,
        }
    }

    //Todo refactor
    fn internal_bulk_load(
        min_children: usize,
        max_children: usize,
        mut entries: Vec<EntryPtr<C, P, B>>,
        mut level: usize,
    ) -> Node<C, P, B> {
        let mut items_num = entries.len();

        while items_num > max_children {
            // We choose to fill the nodes between the min and max capacity to avoid splits and merges
            let node_capacity = (max_children + min_children) / 2;
            let leaf_pages = items_num / node_capacity;
            let vertical_slices = sqrt(leaf_pages);
            let chunk_size = node_capacity * vertical_slices;

            // Sort all by x
            entries.sort_by(|first, second| {
                let first_center = first.get_mbb().get_center();
                let second_center = second.get_mbb().get_center();

                first_center
                    .get_nth_coord(0)
                    .partial_cmp(&second_center.get_nth_coord(0))
                    .unwrap()
            });

            let mut chunks = vec![];
            let mut chunk = vec![];
            let mut size = 0;

            //Split into chunks and sort them by y
            for item in entries {
                chunk.push(item);
                size += 1;

                //Sort by y
                if size >= chunk_size {
                    chunk.sort_by(|first, second| {
                        let first_center = first.get_mbb().get_center();
                        let second_center = second.get_mbb().get_center();

                        first_center
                            .get_nth_coord(1)
                            .partial_cmp(&second_center.get_nth_coord(1))
                            .unwrap()
                    });
                    chunks.push(chunk);
                    chunk = vec![];
                    size = 0;
                }
            }

            if size > 0 {
                chunk.sort_by(|first, second| {
                    let first_center = first.get_mbb().get_center();
                    let second_center = second.get_mbb().get_center();

                    first_center
                        .get_nth_coord(1)
                        .partial_cmp(&second_center.get_nth_coord(1))
                        .unwrap()
                });
                chunks.push(chunk);
            }

            //Separate into entries
            entries = vec![];

            for chunk in chunks {
                let mut items = vec![];
                let mut size = 0;
                let mut maybe_mbb = None;

                for item in chunk {
                    match maybe_mbb {
                        None => maybe_mbb = Some(item.get_mbb().clone()),
                        Some(mbb) => maybe_mbb = Some(mbb.combine_boxes(item.get_mbb())),
                    }

                    items.push(item);
                    size += 1;

                    if size >= node_capacity {
                        let node = Node {
                            entries: items,
                            level,
                            min_children,
                            max_children,
                        };

                        let entry = Arc::new(Entry::Branch {
                            mbb: maybe_mbb.unwrap(),
                            child: node,
                        });

                        entries.push(entry);
                        items = vec![];
                        maybe_mbb = None;
                        size = 0;
                    }
                }

                if size > 0 {
                    let node = Node {
                        entries: items,
                        level,
                        min_children,
                        max_children,
                    };

                    let entry = Arc::new(Entry::Branch {
                        mbb: maybe_mbb.unwrap(),
                        child: node,
                    });

                    entries.push(entry);
                }
            }

            level += 1;
            items_num = entries.len();
        }

        Node {
            entries,
            level,
            min_children,
            max_children,
        }
    }

    fn internal_insert(&mut self, item: EntryPtr<C, P, B>, level: usize) {
        if let Some((first_entry, second_entry)) = self.root.insert(item, level) {
            self.root = Node {
                entries: vec![first_entry, second_entry],
                level: self.root.level + 1,
                min_children: self.root.min_children,
                max_children: self.root.max_children,
            };
        }
    }
}

#[derive(Debug, Clone)]
struct Node<C, P, B>
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    entries: Vec<EntryPtr<C, P, B>>,
    level: usize,
    min_children: usize,
    max_children: usize,
}

impl<C, P, B> Node<C, P, B>
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    fn new(min_children: usize, max_children: usize) -> Self {
        Node {
            entries: Vec::new(),
            level: 0,
            min_children,
            max_children,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_leaf(&self) -> bool {
        self.level == 0
    }

    pub fn search(&self, area: &Rect<C, P>) -> Option<Vec<B>> {
        let mut found = vec![];

        if self.is_leaf() {
            for entry in &self.entries {
                match **entry {
                    Entry::Leaf { item: ref entry } if area.is_covering(entry.get_mbb()) => {
                        found.push(entry.clone());
                    }
                    _ => (),
                }
            }
        } else {
            for entry in &self.entries {
                if area.is_intersecting(entry.get_mbb()) {
                    match entry.clone().search(area) {
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

    fn insert(&mut self, item: EntryPtr<C, P, B>, level: usize) -> MaybeSplit<C, P, B> {
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

    fn remove(&mut self, bounding_box: &Rect<C, P>) -> Option<(B, MaybeOrphans<C, P, B>)> {
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
                Arc::try_unwrap(entry_ptr).unwrap_or_else(|_| unreachable!())
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

    fn split(&mut self) -> (EntryPtr<C, P, B>, EntryPtr<C, P, B>) {
        let ((first_group, first_mbb), (second_group, second_mbb)) =
            quadratic_split(&mut self.entries, self.min_children);

        let first_group = Entry::Branch {
            mbb: first_mbb,
            child: Node {
                entries: first_group,
                level: self.level,
                min_children: self.min_children,
                max_children: self.max_children,
            },
        };

        let second_group = Entry::Branch {
            mbb: second_mbb,
            child: Node {
                entries: second_group,
                level: self.level,
                min_children: self.min_children,
                max_children: self.max_children,
            },
        };

        (Arc::new(first_group), Arc::new(second_group))
    }
}

fn quadratic_split<C, P, B>(
    entries: &mut Vec<EntryPtr<C, P, B>>,
    min_children: usize,
) -> (SplitGroup<C, P, B>, SplitGroup<C, P, B>)
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    let (first_seed_idx, second_seed_idx) = pick_seeds(entries);

    let first_seed = entries.remove(first_seed_idx);
    let second_seed = entries.remove(second_seed_idx - 1);

    let mut first_mbb = first_seed.get_mbb().clone();
    let mut first_group = vec![first_seed];

    let mut second_mbb = second_seed.get_mbb().clone();
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
            let (idx, expanded_rect, group) = pick_next(
                entries,
                &first_mbb,
                &second_mbb,
                first_group.len(),
                second_group.len(),
            );

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

fn pick_seeds<C, P, B>(entries: &[EntryPtr<C, P, B>]) -> (usize, usize)
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
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

fn pick_next<C, P, B>(
    entries: &[EntryPtr<C, P, B>],
    first_mbb: &Rect<C, P>,
    second_mbb: &Rect<C, P>,
    first_group_size: usize,
    second_group_size: usize,
) -> (usize, Rect<C, P>, Group)
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    let mut entries_iter = entries.iter();
    let item = entries_iter.next().unwrap();
    let mut item_idx = 0;

    let (first_preference, second_preference, first_expanded_rect, second_expanded_rect) =
        calc_preferences(item, first_mbb, second_mbb);

    let mut max_preference_diff = (first_preference - second_preference).abs();

    let mut group = select_group(
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
        let (first_preference, second_preference, first_expanded_rect, second_expanded_rect) =
            calc_preferences(item, first_mbb, second_mbb);
        let preference_diff = (first_preference - second_preference).abs();

        if max_preference_diff <= preference_diff {
            max_preference_diff = preference_diff;
            item_idx = idx;

            group = select_group(
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

fn calc_preferences<C, P, B>(
    item: &EntryPtr<C, P, B>,
    first_mbb: &Rect<C, P>,
    second_mbb: &Rect<C, P>,
) -> (C, C, Rect<C, P>, Rect<C, P>)
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    let first_expanded_rect = first_mbb.combine_boxes(item.get_mbb());
    let first_diff = first_expanded_rect.measure() - first_mbb.measure();

    let second_expanded_rect = second_mbb.combine_boxes(item.get_mbb());
    let second_diff = second_expanded_rect.measure() - second_mbb.measure();

    (
        first_diff,
        second_diff,
        first_expanded_rect,
        second_expanded_rect,
    )
}

fn select_group<C, P>(
    first_mbb: &Rect<C, P>,
    second_mbb: &Rect<C, P>,
    first_group_size: usize,
    second_group_size: usize,
    first_diff: C,
    second_diff: C,
) -> Group
where
    C: Coordinate,
    P: Point<C>,
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

type EntryPtr<C, P, B> = Arc<Entry<C, P, B>>;
type MaybeOrphans<C, P, B> = Option<Vec<EntryPtr<C, P, B>>>;
type MaybeSplit<C, P, B> = Option<(EntryPtr<C, P, B>, EntryPtr<C, P, B>)>;
type SplitGroup<C, P, B> = (Vec<EntryPtr<C, P, B>>, Rect<C, P>);

#[derive(Debug, Clone)]
enum Entry<C, P, B>
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    Leaf {
        item: B,
    },
    Branch {
        mbb: Rect<C, P>,
        child: Node<C, P, B>,
    },
}

impl<C, P, B> Entry<C, P, B>
where
    C: Coordinate,
    P: Point<C>,
    B: BoundingBox<C, P>,
{
    fn len(&self) -> usize {
        match self {
            Entry::Leaf { .. } => 0,
            Entry::Branch { child, .. } => child.len(),
        }
    }

    fn search(&self, area: &Rect<C, P>) -> Option<Vec<B>> {
        match self {
            Entry::Branch { child, .. } => child.search(area),
            Entry::Leaf { .. } => unreachable!(),
        }
    }

    fn get_mbb(&self) -> &Rect<C, P> {
        match self {
            Entry::Leaf { item } => item.get_mbb(),
            Entry::Branch { mbb, .. } => mbb,
        }
    }

    fn insert(
        &mut self,
        item: EntryPtr<C, P, B>,
        expanded_rect: Rect<C, P>,
        level: usize,
    ) -> MaybeSplit<C, P, B> {
        match self {
            Entry::Branch { mbb, child } => {
                *mbb = expanded_rect;
                child.insert(item, level)
            }
            Entry::Leaf { .. } => unreachable!(),
        }
    }

    fn remove(&mut self, bounding_box: &Rect<C, P>) -> Option<(B, MaybeOrphans<C, P, B>)> {
        match self {
            Entry::Branch { mbb, child } => {
                let (removed, orphan_nodes) = child.remove(bounding_box)?;

                let removed_mbb = removed.get_mbb();

                if removed_mbb.get_low().has_equal_cords(mbb.get_low())
                    || removed_mbb.get_high().has_equal_cords(mbb.get_high())
                {
                    let mut entries_iter = child.entries.iter();
                    let mut shrunken_mbb = entries_iter.next().unwrap().get_mbb().clone();
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
