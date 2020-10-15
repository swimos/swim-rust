#[cfg(test)]
mod tests;

static MAX_CHILDREN: usize = 4;
static MIN_CHILDREN: usize = 2;

#[derive(Debug)]
struct RTree {
    root: Node,
}

impl RTree {
    fn new() -> Self {
        RTree {
            root: Node::Leaf(Vec::new(), 0),
        }
    }

    fn insert(&mut self, item: Rect) {
        if let Some((first_entry, second_entry)) = self.root.insert(item) {
            match self.root {
                Node::Branch(_, level) | Node::Leaf(_, level) => {
                    self.root = Node::Branch(vec![first_entry, second_entry], level + 1)
                }
            }
        }
    }

    fn remove(&mut self, item: &Rect) -> Option<Rect> {
        let (removed, maybe_orphan_nodes) = self.root.remove(item)?;

        if maybe_orphan_nodes.is_some() {
            for orphan_node in maybe_orphan_nodes? {
                match orphan_node {
                    Node::Branch(entries, _) => {
                        for entry in entries {
                            // Todo insert branch at appropriate level
                            // And do split if needed
                        }
                    }
                    Node::Leaf(entries, _) => {
                        for entry in entries {
                            self.insert(entry);
                        }
                    }
                }
            }
        }

        Some(removed)
    }
}

#[derive(Debug)]
enum Node {
    Branch(Vec<Entry>, i32),
    Leaf(Vec<Rect>, i32),
}

impl Node {
    fn len(&self) -> usize {
        match self {
            Node::Branch(entries, _) => entries.len(),
            Node::Leaf(entries, _) => entries.len(),
        }
    }

    fn insert(&mut self, item: Rect) -> Option<(Entry, Entry)> {
        match self {
            Node::Branch(entries, _) if !entries.is_empty() => {
                let mut entries_iter = entries.iter_mut();

                let mut min_entry = entries_iter.next().unwrap();
                let mut min_entry_idx = 0;
                let mut min_rect = min_entry.mbb.combine_boxes(&item);
                let mut min_diff = min_rect.area() - min_entry.mbb.area();

                for (entry, idx) in entries_iter.zip(1..) {
                    let expanded_rect = entry.mbb.combine_boxes(&item);
                    let diff = expanded_rect.area() - entry.mbb.area();

                    if diff < min_diff {
                        min_diff = diff;
                        min_rect = expanded_rect;
                        min_entry = entry;
                        min_entry_idx = idx;
                    }
                }

                match min_entry.insert(item, min_rect) {
                    Some((first_entry, second_entry)) => {
                        entries.remove(min_entry_idx);
                        entries.push(first_entry);
                        entries.push(second_entry);

                        if entries.len() > MAX_CHILDREN {
                            let split_entries = self.split();
                            return Some(split_entries);
                        }
                    }
                    None => (),
                }
            }
            Node::Leaf(entries, level) => {
                entries.push(item);

                if entries.len() > MAX_CHILDREN {
                    let split_entries = self.split();
                    return Some(split_entries);
                }
            }
            _ => unreachable!(),
        };
        None
    }

    fn remove(&mut self, item: &Rect) -> Option<(Rect, Option<Vec<Node>>)> {
        match self {
            Node::Branch(entries, _) => {
                let mut entry_index = None;
                let mut maybe_removed = None;

                for (idx, entry) in entries.iter_mut().enumerate() {
                    if entry.is_covering(item) {
                        maybe_removed = entry.remove(item);

                        if maybe_removed.is_some() {
                            if entry.len() < MIN_CHILDREN {
                                entry_index = Some(idx);
                            }
                            break;
                        }
                    }
                }

                let (removed, mut maybe_orphan_nodes) = maybe_removed?;

                if entry_index.is_some() {
                    let Entry {
                        mbb: _,
                        child: orphan,
                    } = entries.remove(entry_index.unwrap());

                    match maybe_orphan_nodes {
                        Some(mut orphan_nodes) => {
                            orphan_nodes.push(orphan);
                            Some((removed, Some(orphan_nodes)))
                        }
                        None => Some((removed, Some(vec![orphan]))),
                    }
                } else {
                    Some((removed, None))
                }
            }
            Node::Leaf(entries, _) => {
                let mut remove_idx = None;

                for (idx, entry) in entries.iter().enumerate() {
                    if entry == item {
                        remove_idx = Some(idx);
                        break;
                    }
                }
                Some((entries.remove(remove_idx?), None))
            }
        }
    }

    fn split(&mut self) -> (Entry, Entry) {
        match self {
            Node::Branch(entries, level) => {
                let (first_group, second_group, first_mbb, second_mbb) = quadratic_split(entries);

                let first_group = Entry {
                    mbb: first_mbb,
                    child: Node::Branch(first_group, *level),
                };

                let second_group = Entry {
                    mbb: second_mbb,
                    child: Node::Branch(second_group, *level),
                };

                (first_group, second_group)
            }
            Node::Leaf(entries, level) => {
                let (first_group, second_group, first_mbb, second_mbb) = quadratic_split(entries);

                let first_group = Entry {
                    mbb: first_mbb,
                    child: Node::Leaf(first_group, *level),
                };

                let second_group = Entry {
                    mbb: second_mbb,
                    child: Node::Leaf(second_group, *level),
                };

                (first_group, second_group)
            }
        }
    }
}

fn quadratic_split<T: BoundingBox>(entries: &mut Vec<T>) -> (Vec<T>, Vec<T>, Rect, Rect) {
    let (first_seed_idx, second_seed_idx) = pick_seeds(entries);

    let first_seed = entries.remove(first_seed_idx);
    let second_seed = entries.remove(second_seed_idx - 1);

    let mut first_mbb = first_seed.get_mbb().clone();
    let mut first_group = vec![first_seed];

    let mut second_mbb = second_seed.get_mbb().clone();
    let mut second_group = vec![second_seed];

    while !entries.is_empty() {
        if entries.len() + first_group.len() == MIN_CHILDREN {
            for item in entries.drain(..) {
                let expanded_rect = first_mbb.combine_boxes(&item);

                first_mbb = expanded_rect;
                first_group.push(item);
            }
        } else if entries.len() + second_group.len() == MIN_CHILDREN {
            for item in entries.drain(..) {
                let expanded_rect = second_mbb.combine_boxes(&item);

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

fn pick_seeds<T: BoundingBox>(entries: &Vec<T>) -> (usize, usize) {
    let mut first_idx = 0;
    let mut second_idx = 1;
    let mut max_diff = i32::MIN;

    if entries.len() > 2 {
        for (i, first_rect) in entries.iter().enumerate() {
            for (j, second_rect) in entries.iter().enumerate().skip(i + 1) {
                let combined_rect = first_rect.combine_boxes::<T>(second_rect);
                let diff = combined_rect.area() - first_rect.area() - second_rect.area();

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

fn pick_next<T: BoundingBox>(
    entries: &Vec<T>,
    first_mbb: &Rect,
    second_mbb: &Rect,
    first_group_size: usize,
    second_group_size: usize,
) -> (usize, Rect, Group) {
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

fn calc_preferences<T: BoundingBox>(
    item: &T,
    first_mbb: &Rect,
    second_mbb: &Rect,
) -> (i32, i32, Rect, Rect) {
    let first_expanded_rect = first_mbb.combine_boxes::<T>(item);
    let first_diff = first_expanded_rect.area() - first_mbb.area();

    let second_expanded_rect = second_mbb.combine_boxes::<T>(item);
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

#[derive(Debug)]
struct Entry {
    mbb: Rect,
    child: Node,
}

impl Entry {
    fn len(&self) -> usize {
        self.child.len()
    }

    fn insert(&mut self, item: Rect, expanded_rect: Rect) -> Option<(Entry, Entry)> {
        self.mbb = expanded_rect;
        self.child.insert(item)
    }

    fn remove(&mut self, item: &Rect) -> Option<(Rect, Option<Vec<Node>>)> {
        let (removed, orphan_nodes) = self.child.remove(item)?;

        let removed_mbb = removed.get_mbb();

        if removed_mbb.lower_left.x == self.mbb.lower_left.x
            || removed_mbb.lower_left.y == self.mbb.lower_left.y
            || removed_mbb.upper_right.x == self.mbb.upper_right.x
            || removed_mbb.upper_right.y == self.mbb.upper_right.y
        {
            let shrunken_mbb = match &self.child {
                //Todo refactor and add length checks
                Node::Branch(entries, _) => {
                    let mut entries_iter = entries.iter();
                    let shrunken_mbb = entries_iter.next().unwrap().mbb.clone();
                    entries_iter.fold(shrunken_mbb, |acc, entry| entry.mbb.combine_boxes(&acc))
                }
                Node::Leaf(entries, _) => {
                    let mut entries_iter = entries.iter();
                    let shrunken_mbb = entries_iter.next().unwrap().clone();
                    entries_iter.fold(shrunken_mbb, |acc, entry| entry.combine_boxes(&acc))
                }
            };

            self.mbb = shrunken_mbb;
        }

        Some((removed, orphan_nodes))
    }
}

impl BoundingBox for Entry {
    fn get_mbb(&self) -> &Rect {
        &self.mbb
    }

    fn area(&self) -> i32 {
        self.mbb.area()
    }

    fn combine_boxes<T: BoundingBox>(&self, other: &T) -> Rect {
        self.mbb.combine_boxes(other.get_mbb())
    }

    fn is_covering<T: BoundingBox>(&self, other: &T) -> bool {
        self.mbb.is_covering(other.get_mbb())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Rect {
    lower_left: Point,
    upper_right: Point,
}

impl Rect {
    fn new(lower_left: Point, upper_right: Point) -> Self {
        //Todo check if the positions are correct
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
