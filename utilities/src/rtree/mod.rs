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
            root: Node::Leaf(Vec::new()),
        }
    }

    fn insert(&mut self, item: Rect) {
        if let Some((first_entry, second_entry)) = self.root.insert(item) {
            self.root = Node::Branch(vec![first_entry, second_entry])
        }
    }
}

#[derive(Debug)]
enum Node {
    Branch(Vec<Entry>),
    Leaf(Vec<Rect>),
}

impl Node {
    fn insert(&mut self, item: Rect) -> Option<(Entry, Entry)> {
        match self {
            Node::Branch(entries) => {
                let mut entries_iter = entries.iter_mut();

                let mut min_entry = entries_iter.next().unwrap();
                let mut min_entry_idx = 0;
                let mut min_rect = min_entry.mbb.combine_boxes(&item);
                let mut min_diff = min_rect.area() - min_entry.mbb.area();

                for (idx, entry) in entries_iter.enumerate() {
                    let expanded_rect = entry.mbb.combine_boxes(&item);
                    let diff = expanded_rect.area() - entry.mbb.area();

                    if diff < min_diff {
                        min_diff = diff;
                        min_rect = expanded_rect;
                        min_entry = entry;
                        min_entry_idx = idx + 1;
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
            Node::Leaf(entries) => {
                entries.push(item);

                if entries.len() > MAX_CHILDREN {
                    let split_entries = self.split();
                    return Some(split_entries);
                }
            }
        };
        None
    }

    fn split(&mut self) -> (Entry, Entry) {
        match self {
            Node::Branch(entries) => {
                let (first_seed_idx, second_seed_idx) = pick_seeds(entries);

                let first_seed = entries.remove(first_seed_idx);
                let second_seed = entries.remove(second_seed_idx - 1);

                let mut first_group = Entry {
                    mbb: first_seed.mbb.clone(),
                    child: Node::Branch(vec![first_seed]),
                };

                let mut second_group = Entry {
                    mbb: second_seed.mbb.clone(),
                    child: Node::Branch(vec![second_seed]),
                };

                //Todo refactor
                while !entries.is_empty() {
                    if entries.len() + first_group.child.len() == MIN_CHILDREN {
                        for item in entries.drain(..) {
                            let expanded_rect = first_group.mbb.combine_boxes(&item.mbb);

                            first_group.mbb = expanded_rect;
                            match &mut first_group.child {
                                Node::Branch(entries) => entries.push(item),
                                Node::Leaf(_) => unreachable!(),
                            }
                        }
                    } else if entries.len() + second_group.child.len() == MIN_CHILDREN {
                        for item in entries.drain(..) {
                            let expanded_rect = second_group.mbb.combine_boxes(&item.mbb);
                            second_group.mbb = expanded_rect;
                            match &mut second_group.child {
                                Node::Branch(entries) => entries.push(item),
                                Node::Leaf(_) => unreachable!(),
                            }
                        }
                    } else {
                        let (idx, expanded_rect, group) =
                            pick_next(entries, &mut first_group, &mut second_group);

                        let item = entries.remove(idx);

                        match group {
                            Group::First => {
                                // first_group.insert(item, expanded_rect);
                                first_group.mbb = expanded_rect;
                                match &mut first_group.child {
                                    Node::Branch(entries) => entries.push(item),
                                    Node::Leaf(_) => unreachable!(),
                                }
                            }
                            Group::Second => {
                                second_group.mbb = expanded_rect;
                                match &mut second_group.child {
                                    Node::Branch(entries) => entries.push(item),
                                    Node::Leaf(_) => unreachable!(),
                                }
                            }
                        };
                    }
                }
                (first_group, second_group)
            }
            Node::Leaf(entries) => {
                let (first_seed_idx, second_seed_idx) = pick_seeds(entries);

                let first_seed = entries.remove(first_seed_idx);
                let second_seed = entries.remove(second_seed_idx - 1);

                let mut first_group = Entry {
                    mbb: first_seed.clone(),
                    child: Node::Leaf(vec![first_seed]),
                };

                let mut second_group = Entry {
                    mbb: second_seed.clone(),
                    child: Node::Leaf(vec![second_seed]),
                };

                //Todo refactor
                while !entries.is_empty() {
                    if entries.len() + first_group.child.len() == MIN_CHILDREN {
                        for item in entries.drain(..) {
                            let expanded_rect = first_group.mbb.combine_boxes(&item);
                            first_group.insert(item, expanded_rect);
                        }
                    } else if entries.len() + second_group.child.len() == MIN_CHILDREN {
                        for item in entries.drain(..) {
                            let expanded_rect = second_group.mbb.combine_boxes(&item);
                            second_group.insert(item, expanded_rect);
                        }
                    } else {
                        let (idx, expanded_rect, group) =
                            pick_next(entries, &mut first_group, &mut second_group);

                        let item = entries.remove(idx);

                        match group {
                            Group::First => first_group.insert(item, expanded_rect),
                            Group::Second => second_group.insert(item, expanded_rect),
                        };
                    }
                }

                (first_group, second_group)
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Node::Branch(entries) => entries.len(),
            Node::Leaf(entries) => entries.len(),
        }
    }
}

fn pick_seeds<T: BoundingBox>(entries: &Vec<T>) -> (usize, usize) {
    let length = entries.len();

    let mut first_idx = 0;
    let mut second_idx = 1;
    let mut max_diff = i32::MIN;

    if length > 2 {
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
    first_group: &Entry,
    second_group: &Entry,
) -> (usize, Rect, Group) {
    let mut max_preference = 0;
    let mut item_idx = 0;
    let mut group = Group::First;
    let mut expanded_rect: Option<Rect> = None;

    for (idx, item) in entries.iter().enumerate() {
        let first_expanded_rect = first_group.mbb.combine_boxes::<T>(item);
        let first_diff = first_expanded_rect.area() - first_group.mbb.area();

        let second_expanded_rect = second_group.mbb.combine_boxes::<T>(item);
        let second_diff = second_expanded_rect.area() - second_group.mbb.area();

        let preference = (first_diff - second_diff).abs();
        if max_preference <= preference {
            max_preference = preference;
            item_idx = idx;
            group = select_group(first_group, second_group, first_diff, second_diff);

            match group {
                Group::First => expanded_rect = Some(first_expanded_rect),
                Group::Second => expanded_rect = Some(second_expanded_rect),
            }
        }
    }

    //Todo remove unwrap
    (item_idx, expanded_rect.unwrap(), group)
}

fn select_group(
    first_group: &Entry,
    second_group: &Entry,
    first_diff: i32,
    second_diff: i32,
) -> Group {
    if first_diff < second_diff {
        Group::First
    } else if second_diff < first_diff {
        Group::Second
    } else {
        if first_group.mbb.area() < second_group.mbb.area() {
            Group::First
        } else if second_group.mbb.area() < first_group.mbb.area() {
            Group::Second
        } else {
            if first_group.child.len() < second_group.child.len() {
                Group::First
            } else if second_group.child.len() < first_group.child.len() {
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
    fn insert(&mut self, item: Rect, expanded_rect: Rect) -> Option<(Entry, Entry)> {
        self.mbb = expanded_rect;
        self.child.insert(item)
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
}

#[derive(Debug, Clone)]
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

    // Create the minimum bounding box that contains both.
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
}

#[derive(Debug, Clone)]
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
    fn combine_boxes<T: BoundingBox>(&self, other: &T) -> Rect;
}
