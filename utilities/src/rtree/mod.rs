use std::num::NonZeroUsize;
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
struct RTree<T: Clone + BoundingBox + PartialEq> {
    root: Node<T>,
    len: usize,
}

impl<T> RTree<T>
where
    T: Clone + BoundingBox + PartialEq,
{
    fn new(min_children: NonZeroUsize, max_children: NonZeroUsize) -> Self {
        if min_children.get() > max_children.get() / 2 {
            panic!("The minimum number of children cannot be more than half of the maximum number of children.")
        }

        RTree {
            root: Node::new(min_children.get(), max_children.get()),
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn insert(&mut self, item: T) {
        self.internal_insert(Arc::new(Entry::Leaf { item }), 0);
        self.len = self.len + 1;
    }

    fn remove(&mut self, item: &T) -> Option<T> {
        let (removed, maybe_orphan_nodes) = self.root.remove(item)?;
        self.len = self.len - 1;

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

    fn internal_insert(&mut self, item: Arc<Entry<T>>, level: i32) {
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
struct Node<T: Clone + BoundingBox + PartialEq> {
    entries: Vec<Arc<Entry<T>>>,
    level: i32,
    min_children: usize,
    max_children: usize,
}

impl<T: Clone + BoundingBox + PartialEq> Node<T> {
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
        if self.level == 0 {
            true
        } else {
            false
        }
    }

    fn insert(
        &mut self,
        item: Arc<Entry<T>>,
        level: i32,
    ) -> Option<(Arc<Entry<T>>, Arc<Entry<T>>)> {
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
                    let mut min_diff = min_rect.area() - min_entry.get_mbb().area();

                    for (entry, idx) in entries_iter.zip(1..) {
                        let expanded_rect = entry.get_mbb().combine_boxes(item.get_mbb());
                        let diff = expanded_rect.area() - entry.get_mbb().area();

                        if diff < min_diff {
                            min_diff = diff;
                            min_rect = expanded_rect;
                            min_entry = entry;
                            min_entry_idx = idx;
                        }
                    }

                    let min_entry = Arc::make_mut(&mut min_entry);

                    match min_entry.insert(item, min_rect, level) {
                        Some((first_entry, second_entry)) => {
                            self.entries.remove(min_entry_idx);
                            self.entries.push(first_entry);
                            self.entries.push(second_entry);

                            if self.entries.len() > self.max_children {
                                let split_entries = self.split();
                                return Some(split_entries);
                            }
                        }
                        None => (),
                    }
                }
            }
        }
        None
    }

    fn remove(&mut self, item: &T) -> Option<(T, Option<Vec<Arc<Entry<T>>>>)> {
        if self.is_leaf() {
            //If this is leaf try to find the item
            let mut remove_idx = None;

            for (idx, entry) in self.entries.iter().enumerate() {
                match **entry {
                    Entry::Leaf { item: ref entry } if entry == item => {
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
                if entry.get_mbb().is_covering(item) {
                    let entry = Arc::make_mut(entry);
                    maybe_removed = entry.remove(item);

                    if maybe_removed.is_some() {
                        if entry.len() < self.min_children {
                            entry_index = Some(idx);
                        }
                        break;
                    }
                }
            }

            let (removed, maybe_orphan_nodes) = maybe_removed?;

            if entry_index.is_some() {
                let orphan = self.entries.remove(entry_index.unwrap());

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

    fn split(&mut self) -> (Arc<Entry<T>>, Arc<Entry<T>>) {
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

fn quadratic_split<T: BoundingBox + Clone + PartialEq>(
    entries: &mut Vec<Arc<Entry<T>>>,
    min_children: usize,
) -> (Vec<Arc<Entry<T>>>, Vec<Arc<Entry<T>>>, Rect, Rect) {
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

fn pick_seeds<T>(entries: &Vec<Arc<Entry<T>>>) -> (usize, usize)
where
    T: BoundingBox + Clone + PartialEq,
{
    let mut first_idx = 0;
    let mut second_idx = 1;
    let mut max_diff = i32::MIN;

    if entries.len() > 2 {
        for (i, first_item) in entries.iter().enumerate() {
            for (j, second_item) in entries.iter().enumerate().skip(i + 1) {
                let combined_rect = first_item.get_mbb().combine_boxes(second_item.get_mbb());
                let diff = combined_rect.area()
                    - first_item.get_mbb().area()
                    - second_item.get_mbb().area();

                if diff > max_diff {
                    max_diff = diff;
                    first_idx = i;
                    second_idx = j;
                }
            }
        }
    }

    (first_idx, second_idx)
}

fn pick_next<T>(
    entries: &Vec<Arc<Entry<T>>>,
    first_mbb: &Rect,
    second_mbb: &Rect,
    first_group_size: usize,
    second_group_size: usize,
) -> (usize, Rect, Group)
where
    T: BoundingBox + Clone + PartialEq,
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

fn calc_preferences<T: Clone + BoundingBox + PartialEq>(
    item: &Arc<Entry<T>>,
    first_mbb: &Rect,
    second_mbb: &Rect,
) -> (i32, i32, Rect, Rect) {
    let first_expanded_rect = first_mbb.combine_boxes(item.get_mbb());
    let first_diff = first_expanded_rect.area() - first_mbb.area();

    let second_expanded_rect = second_mbb.combine_boxes(item.get_mbb());
    let second_diff = second_expanded_rect.area() - second_mbb.area();

    (
        first_diff,
        second_diff,
        first_expanded_rect,
        second_expanded_rect,
    )
}

fn select_group(
    first_mbb: &Rect,
    second_mbb: &Rect,
    first_group_size: usize,
    second_group_size: usize,
    first_diff: i32,
    second_diff: i32,
) -> Group {
    if first_diff < second_diff {
        Group::First
    } else if second_diff < first_diff {
        Group::Second
    } else {
        if first_mbb.area() < second_mbb.area() {
            Group::First
        } else if second_mbb.area() < first_mbb.area() {
            Group::Second
        } else {
            if first_group_size < second_group_size {
                Group::First
            } else if second_group_size < first_group_size {
                Group::Second
            } else {
                Group::First
            }
        }
    }
}

enum Group {
    First,
    Second,
}

#[derive(Debug, Clone)]
enum Entry<T: Clone + BoundingBox + PartialEq> {
    Leaf { item: T },
    Branch { mbb: Rect, child: Node<T> },
}

impl<T: Clone + BoundingBox + PartialEq> Entry<T> {
    fn len(&self) -> usize {
        match self {
            Entry::Leaf { .. } => 0,
            Entry::Branch { child, .. } => child.len(),
        }
    }

    fn get_mbb(&self) -> &Rect {
        match self {
            Entry::Leaf { item } => item.get_mbb(),
            Entry::Branch { mbb, .. } => mbb,
        }
    }

    fn insert(
        &mut self,
        item: Arc<Entry<T>>,
        expanded_rect: Rect,
        level: i32,
    ) -> Option<(Arc<Entry<T>>, Arc<Entry<T>>)> {
        match self {
            Entry::Branch { mbb, child } => {
                *mbb = expanded_rect;
                child.insert(item, level)
            }
            Entry::Leaf { .. } => unreachable!(),
        }
    }

    fn remove(&mut self, item: &T) -> Option<(T, Option<Vec<Arc<Entry<T>>>>)> {
        match self {
            Entry::Branch { mbb, child } => {
                let (removed, orphan_nodes) = child.remove(item)?;

                let removed_mbb = removed.get_mbb();

                if removed_mbb.lower_left.x == mbb.lower_left.x
                    || removed_mbb.lower_left.y == mbb.lower_left.y
                    || removed_mbb.upper_right.x == mbb.upper_right.x
                    || removed_mbb.upper_right.y == mbb.upper_right.y
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
struct Rect {
    lower_left: Point,
    upper_right: Point,
}

impl Rect {
    fn new(lower_left: Point, upper_right: Point) -> Self {
        if lower_left.x > upper_right.x || lower_left.y > upper_right.y {
            panic!("The first point must be the lower left and the second the upper right.")
        }

        Rect {
            lower_left,
            upper_right,
        }
    }
}

impl BoundingBox for Rect {
    fn get_mbb(&self) -> &Rect {
        self
    }

    fn area(&self) -> i32 {
        (self.upper_right.x - self.lower_left.x) * (self.upper_right.y - self.lower_left.y)
    }

    fn combine_boxes<T: BoundingBox>(&self, other: &T) -> Rect {
        let other_mbb = other.get_mbb();

        let new_lower_left_x = if self.lower_left.x > other_mbb.lower_left.x {
            other_mbb.lower_left.x
        } else {
            self.lower_left.x
        };

        let new_lower_left_y = if self.lower_left.y > other_mbb.lower_left.y {
            other_mbb.lower_left.y
        } else {
            self.lower_left.y
        };

        let new_upper_right_x = if self.upper_right.x > other_mbb.upper_right.x {
            self.upper_right.x
        } else {
            other_mbb.upper_right.x
        };

        let new_upper_right_y = if self.upper_right.y > other_mbb.upper_right.y {
            self.upper_right.y
        } else {
            other_mbb.upper_right.y
        };

        Rect::new(
            Point::new(new_lower_left_x, new_lower_left_y),
            Point::new(new_upper_right_x, new_upper_right_y),
        )
    }

    fn is_covering<T: BoundingBox>(&self, other: &T) -> bool {
        let other_mbb = other.get_mbb();

        if self.lower_left.x > other_mbb.lower_left.x {
            return false;
        } else if self.lower_left.y > other_mbb.lower_left.y {
            return false;
        } else if self.upper_right.x < other_mbb.upper_right.x {
            return false;
        } else if self.upper_right.y < other_mbb.upper_right.y {
            return false;
        }

        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Point {
    x: i32,
    y: i32,
}

impl Point {
    fn new(x: i32, y: i32) -> Self {
        Point { x, y }
    }
}

trait BoundingBox {
    fn get_mbb(&self) -> &Rect;
    fn area(&self) -> i32;
    // Create a minimum bounding box that contains both items.
    fn combine_boxes<T: BoundingBox>(&self, other: &T) -> Rect;
    fn is_covering<T: BoundingBox>(&self, other: &T) -> bool;
}
