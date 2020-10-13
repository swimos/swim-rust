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
                let (mut min_diff, mut min_rect) = min_entry.mbb.expand(&item);

                for (idx, entry) in entries_iter.enumerate() {
                    let (diff, expanded_rect) = entry.mbb.expand(&item);

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
                let mut rects: Vec<&Rect> = entries.iter().map(|entry| &entry.mbb).collect();
                let (first_seed_idx, second_seed_idx) = pick_seeds(&rects);

                rects.remove(first_seed_idx);
                rects.remove(second_seed_idx - 1);
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
                            let (_, expanded_rect) = first_group.mbb.expand(&item.mbb);

                            first_group.mbb = expanded_rect;
                            match &mut first_group.child {
                                Node::Branch(entries) => entries.push(item),
                                Node::Leaf(_) => unreachable!(),
                            }
                        }
                    } else if entries.len() + second_group.child.len() == MIN_CHILDREN {
                        for item in entries.drain(..) {
                            let (_, expanded_rect) = second_group.mbb.expand(&item.mbb);
                            second_group.mbb = expanded_rect;
                            match &mut second_group.child {
                                Node::Branch(entries) => entries.push(item),
                                Node::Leaf(_) => unreachable!(),
                            }
                        }
                    } else {
                        let mut rects: Vec<&Rect> =
                            entries.iter().map(|entry| &entry.mbb).collect();

                        let (idx, expanded_rect, group) =
                            pick_next(&rects, &mut first_group, &mut second_group);

                        rects.remove(idx);
                        let item = entries.remove(idx);

                        match group {
                            Group::First => {
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
                let mut rects: Vec<&Rect> = entries.iter().collect();
                let (first_seed_idx, second_seed_idx) = pick_seeds(&rects);

                rects.remove(first_seed_idx);
                rects.remove(second_seed_idx - 1);
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
                            let (_, expanded_rect) = first_group.mbb.expand(&item);
                            first_group.insert(item, expanded_rect);
                        }
                    } else if entries.len() + second_group.child.len() == MIN_CHILDREN {
                        for item in entries.drain(..) {
                            let (_, expanded_rect) = second_group.mbb.expand(&item);
                            second_group.insert(item, expanded_rect);
                        }
                    } else {
                        let mut rects: Vec<&Rect> = entries.iter().collect();

                        let (idx, expanded_rect, group) =
                            pick_next(&rects, &mut first_group, &mut second_group);

                        rects.remove(idx);
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

fn pick_seeds(rects: &Vec<&Rect>) -> (usize, usize) {
    let length = rects.len();

    let mut first_idx = 0;
    let mut second_idx = 1;
    let mut max_diff = i32::MIN;

    if length > 2 {
        for (i, first_rect) in rects.iter().enumerate() {
            for (j, second_rect) in rects.iter().enumerate().skip(i + 1) {
                let combined_rect = first_rect.combine(second_rect);
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

fn pick_next(
    rects: &Vec<&Rect>,
    first_group: &Entry,
    second_group: &Entry,
) -> (usize, Rect, Group) {
    let mut max_preference = 0;
    let mut item_idx = 0;
    let mut group = Group::First;
    let mut expanded_rect: Option<Rect> = None;

    for (idx, item) in rects.iter().enumerate() {
        let (first_diff, first_expanded_rect) = first_group.mbb.expand(item);
        let (second_diff, second_expanded_rect) = second_group.mbb.expand(item);

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

    // Create the smallest rectangle that contains both.
    fn combine(&self, other: &Rect) -> Rect {
        let new_lower_left_x = if self.lower_left.x > other.lower_left.x {
            other.lower_left.x
        } else {
            self.lower_left.x
        };

        let new_lower_left_y = if self.lower_left.y > other.lower_left.y {
            other.lower_left.y
        } else {
            self.lower_left.y
        };

        let new_upper_right_x = if self.upper_right.x > other.upper_right.x {
            self.upper_right.x
        } else {
            other.upper_right.x
        };

        let new_upper_right_y = if self.upper_right.y > other.upper_right.y {
            self.upper_right.y
        } else {
            other.upper_right.y
        };

        Rect::new(
            Point::new(new_lower_left_x, new_lower_left_y),
            Point::new(new_upper_right_x, new_upper_right_y),
        )
    }

    // Expand this rectangle in order to include the other rectangle.
    fn expand(&self, other: &Rect) -> (i32, Rect) {
        let mut new_lower_left = self.lower_left.clone();
        let mut new_upper_right = self.upper_right.clone();

        if self.lower_left.x > other.lower_left.x {
            new_lower_left.x = other.lower_left.x;
        }

        if self.upper_right.x < other.upper_right.x {
            new_upper_right.x = other.upper_right.x
        }

        if self.lower_left.y > other.lower_left.y {
            new_lower_left.y = other.lower_left.y;
        }

        if self.upper_right.y < other.upper_right.y {
            new_upper_right.y = other.upper_right.y
        }

        let expanded = Rect::new(new_lower_left, new_upper_right);

        (expanded.area() - self.area(), expanded)
    }

    fn area(&self) -> i32 {
        (self.upper_right.x - self.lower_left.x) * (self.upper_right.y - self.lower_left.y)
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
