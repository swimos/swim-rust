#[cfg(test)]
mod tests;

static MAX_CHILDREN: usize = 10;
static MIN_CHILDREN: usize = 5;

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
                        min_entry_idx = idx;
                    }
                }

                match min_entry.insert(item, min_rect) {
                    Some((first_entry, second_entry)) => {
                        entries.remove(min_entry_idx);
                        entries.push(first_entry);
                        entries.push(second_entry);

                        if entries.len() > MAX_CHILDREN {
                            //Split node
                            //let (L: Entry{mbb: min_rect, child: Branch(Vec<Entry>), LL: Entry{mbb: min_rect, child: Branch(Vec<Entry>) = split_node(self);
                            //Some(L, LL)
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
                // quadratic_split(&mut rects);
                unimplemented!()
            }
            Node::Leaf(entries) => quadratic_split(entries),
        }
    }

    fn len(&self) -> usize {
        match self {
            Node::Branch(entries) => entries.len(),
            Node::Leaf(entries) => entries.len(),
        }
    }
}

fn quadratic_split(rects: &mut Vec<Rect>) -> (Entry, Entry) {
    let (first_seed_idx, second_seed_idx) = pick_seeds(rects);

    let first_seed = rects.remove(first_seed_idx);
    let second_seed = rects.remove(second_seed_idx - 1);

    let mut first_group = Entry {
        mbb: first_seed.clone(),
        //Todo should this be a leaf?
        child: Node::Leaf(vec![first_seed]),
    };

    let mut second_group = Entry {
        mbb: second_seed.clone(),
        child: Node::Leaf(vec![second_seed]),
    };

    while !rects.is_empty() {
        if MIN_CHILDREN - first_group.child.len() == rects.len() {
            for item in rects.drain(..) {
                let (_, expanded_rect) = first_group.mbb.expand(&item);
                first_group.insert(item, expanded_rect);
            }
        } else if MIN_CHILDREN - second_group.child.len() == rects.len() {
            for item in rects.drain(..) {
                let (_, expanded_rect) = second_group.mbb.expand(&item);
                second_group.insert(item, expanded_rect);
            }
        } else {
            place_next(rects, &mut first_group, &mut second_group)
        }
    }

    (first_group, second_group)
}

fn pick_seeds(rects: &Vec<Rect>) -> (usize, usize) {
    let length = rects.len();

    let mut first_idx = 0;
    let mut second_idx = 1;
    let mut max_diff = i32::MIN;

    if length > 2 {
        for (i, first_rect) in rects.iter().enumerate() {
            for (j, second_rect) in rects.iter().skip(i + 1).enumerate() {
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

fn place_next(rects: &mut Vec<Rect>, first_group: &mut Entry, second_group: &mut Entry) {
    let mut max_preference = 0;
    let mut item_idx = 0;
    let mut group = Group::First;
    let mut expanded_rect: Option<Rect> = None;

    for (idx, item) in rects.iter().skip(1).enumerate() {
        let (first_diff, first_expanded_rect) = first_group.mbb.expand(item);
        let (second_diff, second_expanded_rect) = second_group.mbb.expand(item);

        let preference = (first_diff - second_diff).abs();

        if max_preference < preference {
            max_preference = preference;
            item_idx = idx;
            group = select_group(first_group, second_group, first_diff, second_diff);

            match group {
                Group::First => expanded_rect = Some(first_expanded_rect),
                Group::Second => expanded_rect = Some(second_expanded_rect),
            }
        }
    }

    let item = rects.remove(item_idx);

    match group {
        Group::First => first_group.insert(item, expanded_rect.unwrap()),
        Group::Second => second_group.insert(item, expanded_rect.unwrap()),
    };
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
    fn new(item: Rect) -> Self {
        Entry {
            mbb: item,
            child: Node::Leaf(Vec::new()),
        }
    }

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
