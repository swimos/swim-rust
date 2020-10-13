use crate::rtree::{Point, RTree, Rect};

#[test]
fn rtree_insert() {
    let first = Rect::new(Point::new(0, 0), Point::new(10, 10));
    let second = Rect::new(Point::new(12, 0), Point::new(15, 15));
    let third = Rect::new(Point::new(7, 7), Point::new(14, 14));
    let fourth = Rect::new(Point::new(10, 11), Point::new(11, 12));
    let fifth = Rect::new(Point::new(4, 4), Point::new(5, 6));
    let sixth = Rect::new(Point::new(4, 9), Point::new(5, 11));
    let seventh = Rect::new(Point::new(13, 0), Point::new(14, 1));
    let eighth = Rect::new(Point::new(13, 13), Point::new(16, 16));
    let ninth = Rect::new(Point::new(2, 13), Point::new(4, 16));
    let tenth = Rect::new(Point::new(2, 2), Point::new(3, 3));
    let eleventh = Rect::new(Point::new(10, 0), Point::new(12, 5));
    let twelfth = Rect::new(Point::new(7, 3), Point::new(8, 6));

    let mut tree = RTree::new();
    tree.insert(first);
    tree.insert(second);
    tree.insert(third);
    tree.insert(fourth);
    tree.insert(fifth);
    tree.insert(sixth);
    tree.insert(seventh);
    tree.insert(eighth);
    tree.insert(ninth);
    tree.insert(tenth);
    tree.insert(eleventh);
    tree.insert(twelfth);

    eprintln!("{:#?}", tree);
}
