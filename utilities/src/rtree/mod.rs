#[cfg(test)]
mod tests;

#[derive(Debug)]
struct RTree {
    max_children: usize,
    min_children: usize,
    root: Vec<Node>,
}

impl RTree {
    fn new(max_children: usize, min_children: usize) -> Self {
        if min_children > max_children / 2 {
            //Todo maybe change this to a macro to have a compile time error.
            panic!("The minimum number of children nodes must be less than half of the maximum number of children nodes!")
        }

        RTree {
            max_children,
            min_children,
            root: Vec::new(),
        }
    }

    fn add(&mut self, item: Rect) {
        if self.root.len() < self.max_children {
            self.root.push(Node::new(item, self.max_children))
        } else {
            //Todo split
            unimplemented!();
        }
    }

    fn is_leaf(&self) -> bool {
        self.root.is_empty()
    }
}

#[derive(Debug)]
struct Node {
    mbb: Rect,
    capacity: usize,
    children: Option<Vec<Node>>,
}

impl Node {
    fn new(mbb: Rect, capacity: usize) -> Self {
        Node {
            mbb,
            capacity,
            children: None,
        }
    }

    fn add(&mut self, item: Rect) {
        match &mut self.children {
            Some(nodes) => {
                if nodes.len() == self.capacity {
                    unimplemented!();
                } else {
                    nodes.push(Node::new(item, self.capacity))
                }
            }
            None => {
                let children = vec![Node::new(item, self.capacity)];
                self.children = Some(children);
            }
        }
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
}

#[derive(Debug)]
struct Point {
    x: i32,
    y: i32,
}

impl Point {
    fn new(x: i32, y: i32) -> Self {
        Point { x, y }
    }
}
