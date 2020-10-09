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
                    // Split node
                    //let (L: Entry{mbb: min_rect, child: Leaf(Vec<Rect>), LL: Entry{mbb: min_rect, child: Leaf(Vec<Rect>) = split_node(self);
                    //Some(L, LL)
                }
            }
        };
        None
    }

    fn split(&mut self) -> (Entry, Entry) {
        match self {
            Node::Branch(entries) => {
                let mut rects: Vec<&Rect> = entries.iter().map(|entry| &entry.mbb).collect();
                quadratic_split(&mut rects);
            }
            Node::Leaf(entries) => {
                let mut rects: Vec<&Rect> = entries.iter().collect();
                quadratic_split(&mut rects);
            }
        }
        unimplemented!()
    }
}

fn quadratic_split(rects: &mut Vec<&Rect>) {
    let (first_seed_idx, second_seed_idx) = pick_seeds(rects);

    //Todo fix
    let first_seed = rects.remove(first_seed_idx);
    let second_seed = rects.remove(second_seed_idx);

    let first_group = vec![first_seed];
    let second_group = vec![second_seed];

    if !rects.is_empty() {

        // if first_group.len()
    }

    unimplemented!()
}

fn pick_seeds(rects: &Vec<&Rect>) -> (usize, usize) {
    //compare rect[n] with rect[n+1] for all n from 0 to N
    //Calculate area when expanding the smaller one to the bigger one and substract the areas of the two rectangles
    // choose the one that wastes the most space
    unimplemented!()
}

fn pick_next(rects: &Vec<&Rect>) -> usize {
    unimplemented!();
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

#[derive(Debug)]
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
