use crate::rtree::{Point, RTree, Rect};

#[test]
fn rtree_insert() {
    let first = Rect::new(Point::new(10, 10), Point::new(15, 20));
    let second = Rect::new(Point::new(8, 5), Point::new(18, 23));

    let diff = first.expand(&second);
    eprintln!("diff = {:?}", diff);

    // let mut rtree = RTree::new(10, 5);
    // rtree.add(Rect::new(Point::new(5, 5), Point::new(25, 25)));
    // rtree.add(Rect::new(Point::new(10, 10), Point::new(25, 25)));
    // eprintln!("rtree = {:?}", rtree);
}
