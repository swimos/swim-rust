use num::Signed;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct RTree<T, P, B>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
{
    root: Node<T, P, B>,
    len: usize,
}

impl<T, P, B> RTree<T, P, B>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
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

    pub fn insert(&mut self, item: B) {
        self.internal_insert(Arc::new(Entry::Leaf { item }), 0);
        self.len += 1;
    }

    pub fn remove(&mut self, bounding_box: &Rect<T, P>) -> Option<B> {
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

    fn internal_insert(&mut self, item: EntryPtr<T, P, B>, level: i32) {
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
struct Node<T, P, B>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
{
    entries: Vec<EntryPtr<T, P, B>>,
    level: i32,
    min_children: usize,
    max_children: usize,
}

impl<T, P, B> Node<T, P, B>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
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

    fn insert(
        &mut self,
        item: EntryPtr<T, P, B>,
        level: i32,
    ) -> Option<(EntryPtr<T, P, B>, EntryPtr<T, P, B>)> {
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

    fn remove(&mut self, bounding_box: &Rect<T, P>) -> Option<(B, Option<Vec<EntryPtr<T, P, B>>>)> {
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

    fn split(&mut self) -> (EntryPtr<T, P, B>, EntryPtr<T, P, B>) {
        let (first_group, second_group, first_mbb, second_mbb) =
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

fn quadratic_split<T, P, B>(
    entries: &mut Vec<EntryPtr<T, P, B>>,
    min_children: usize,
) -> (
    Vec<EntryPtr<T, P, B>>,
    Vec<EntryPtr<T, P, B>>,
    Rect<T, P>,
    Rect<T, P>,
)
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
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

    (first_group, second_group, first_mbb, second_mbb)
}

fn pick_seeds<T, P, B>(entries: &[EntryPtr<T, P, B>]) -> (usize, usize)
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
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

fn pick_next<T, P, B>(
    entries: &[EntryPtr<T, P, B>],
    first_mbb: &Rect<T, P>,
    second_mbb: &Rect<T, P>,
    first_group_size: usize,
    second_group_size: usize,
) -> (usize, Rect<T, P>, Group)
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
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

fn calc_preferences<T, P, B>(
    item: &EntryPtr<T, P, B>,
    first_mbb: &Rect<T, P>,
    second_mbb: &Rect<T, P>,
) -> (T, T, Rect<T, P>, Rect<T, P>)
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
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

fn select_group<T, P>(
    first_mbb: &Rect<T, P>,
    second_mbb: &Rect<T, P>,
    first_group_size: usize,
    second_group_size: usize,
    first_diff: T,
    second_diff: T,
) -> Group
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
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

type EntryPtr<T, P, B> = Arc<Entry<T, P, B>>;

#[derive(Debug, Clone)]
enum Entry<T, P, B>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
{
    Leaf {
        item: B,
    },
    Branch {
        mbb: Rect<T, P>,
        child: Node<T, P, B>,
    },
}

impl<T, P, B> Entry<T, P, B>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
    B: BoundingBox<T, P>,
{
    fn len(&self) -> usize {
        match self {
            Entry::Leaf { .. } => 0,
            Entry::Branch { child, .. } => child.len(),
        }
    }

    fn get_mbb(&self) -> &Rect<T, P> {
        match self {
            Entry::Leaf { item } => item.get_mbb(),
            Entry::Branch { mbb, .. } => mbb,
        }
    }

    fn insert(
        &mut self,
        item: EntryPtr<T, P, B>,
        expanded_rect: Rect<T, P>,
        level: i32,
    ) -> Option<(EntryPtr<T, P, B>, EntryPtr<T, P, B>)> {
        match self {
            Entry::Branch { mbb, child } => {
                *mbb = expanded_rect;
                child.insert(item, level)
            }
            Entry::Leaf { .. } => unreachable!(),
        }
    }

    fn remove(&mut self, bounding_box: &Rect<T, P>) -> Option<(B, Option<Vec<EntryPtr<T, P, B>>>)> {
        match self {
            Entry::Branch { mbb, child } => {
                let (removed, orphan_nodes) = child.remove(bounding_box)?;

                let removed_mbb = removed.get_mbb();

                if removed_mbb.lower_left.has_equal(&mbb.lower_left)
                    || removed_mbb.upper_right.has_equal(&mbb.upper_right)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rect<T, P>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
{
    lower_left: P,
    upper_right: P,
    phantom: PhantomData<T>,
}

impl<T, P> Rect<T, P>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
{
    pub fn new(lower_left: P, upper_right: P) -> Self {
        // Todo
        // if lower_left.x > upper_right.x || lower_left.y > upper_right.y {
        //     panic!("The first point must be the lower left and the second the upper right.")
        // }

        Rect {
            lower_left,
            upper_right,
            phantom: PhantomData,
        }
    }
}

impl<T, P> BoundingBox<T, P> for Rect<T, P>
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
{
    fn get_mbb(&self) -> &Rect<T, P> {
        self
    }

    fn measure(&self) -> T {
        self.upper_right.diff(&self.lower_left).multiply_coord()
    }

    fn combine_boxes<B: BoundingBox<T, P>>(&self, other: &B) -> Rect<T, P> {
        let other_mbb = other.get_mbb();

        let new_lower_left = self.lower_left.get_lowest(&other_mbb.lower_left);
        let new_upper_right = self.upper_right.get_highest(&other_mbb.upper_right);

        Rect::new(new_lower_left, new_upper_right)
    }

    fn is_covering<B: BoundingBox<T, P>>(&self, other: &B) -> bool {
        let other_mbb = other.get_mbb();

        self.lower_left.has_lower_cords(&other_mbb.lower_left)
            && self.upper_right.has_higher_cords(&other_mbb.upper_right)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Point2D<T: Ord + Clone> {
    x: T,
    y: T,
}

impl<T: Ord + Clone> Point2D<T> {
    pub fn new(x: T, y: T) -> Self {
        Point2D { x, y }
    }
}

impl<T: Ord + Copy + Clone + Signed> Point<T> for Point2D<T> {
    fn diff(&self, other: &Point2D<T>) -> Point2D<T> {
        Point2D {
            x: self.x - other.x,
            y: self.y - other.y,
        }
    }

    fn multiply_coord(&self) -> T {
        self.x * self.y
    }

    fn has_equal(&self, other: &Self) -> bool {
        self.x == other.x || self.y == other.y
    }

    fn get_lowest(&self, other: &Self) -> Self {
        let new_lower_x = if self.x > other.x { other.x } else { self.x };
        let new_lower_y = if self.y > other.y { other.y } else { self.y };

        Point2D {
            x: new_lower_x,
            y: new_lower_y,
        }
    }

    fn get_highest(&self, other: &Self) -> Self {
        let new_higher_x = if self.x > other.x { self.x } else { other.x };
        let new_higher_y = if self.y > other.y { self.y } else { other.y };

        Point2D {
            x: new_higher_x,
            y: new_higher_y,
        }
    }

    fn has_higher_cords(&self, other: &Self) -> bool {
        self.x >= other.x && self.y >= other.y
    }

    fn has_lower_cords(&self, other: &Self) -> bool {
        self.x <= other.x && self.y <= other.y
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Point3D<T: Ord + Clone> {
    x: T,
    y: T,
    z: T,
}

impl<T: Ord + Clone> Point3D<T> {
    pub fn new(x: T, y: T, z: T) -> Self {
        Point3D { x, y, z }
    }
}

impl<T: Ord + Copy + Clone + Signed> Point<T> for Point3D<T> {
    fn diff(&self, other: &Point3D<T>) -> Point3D<T> {
        Point3D {
            x: self.x - other.x,
            y: self.y - other.y,
            z: self.z - other.z,
        }
    }

    fn multiply_coord(&self) -> T {
        self.x * self.y * self.z
    }

    fn has_equal(&self, other: &Self) -> bool {
        self.x == other.x || self.y == other.y || self.z == other.z
    }

    fn get_lowest(&self, other: &Self) -> Self {
        let new_lower_x = if self.x > other.x { other.x } else { self.x };
        let new_lower_y = if self.y > other.y { other.y } else { self.y };
        let new_lower_z = if self.z > other.z { other.z } else { self.z };

        Point3D {
            x: new_lower_x,
            y: new_lower_y,
            z: new_lower_z,
        }
    }

    fn get_highest(&self, other: &Self) -> Self {
        let new_higher_x = if self.x > other.x { self.x } else { other.x };
        let new_higher_y = if self.y > other.y { self.y } else { other.y };
        let new_higher_z = if self.z > other.z { self.z } else { other.z };

        Point3D {
            x: new_higher_x,
            y: new_higher_y,
            z: new_higher_z,
        }
    }

    fn has_higher_cords(&self, other: &Self) -> bool {
        self.x >= other.x && self.y >= other.y && self.z >= other.z
    }

    fn has_lower_cords(&self, other: &Self) -> bool {
        self.x <= other.x && self.y <= other.y && self.z <= other.z
    }
}

pub trait Point<T>: Clone + PartialEq + Eq
where
    T: Ord + Copy + Clone + Signed,
{
    fn diff(&self, other: &Self) -> Self;

    fn multiply_coord(&self) -> T;

    fn has_equal(&self, other: &Self) -> bool;

    fn get_lowest(&self, other: &Self) -> Self;

    fn get_highest(&self, other: &Self) -> Self;

    fn has_higher_cords(&self, other: &Self) -> bool;

    fn has_lower_cords(&self, other: &Self) -> bool;
}

pub trait BoundingBox<T, P>: Clone
where
    T: Ord + Copy + Clone + Signed,
    P: Point<T>,
{
    fn get_mbb(&self) -> &Rect<T, P>;
    // Area for 2D shapes and volume for 3D.
    fn measure(&self) -> T;
    // Create a minimum bounding box that contains both items.
    fn combine_boxes<B: BoundingBox<T, P>>(&self, other: &B) -> Rect<T, P>;
    fn is_covering<B: BoundingBox<T, P>>(&self, other: &B) -> bool;
}
