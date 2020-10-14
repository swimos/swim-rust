use crate::rtree::{Point, RTree, Rect};
use std::fs;

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

    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/0.txt").unwrap()
    );

    tree.insert(first);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/1.txt").unwrap()
    );

    tree.insert(second);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2.txt").unwrap()
    );

    tree.insert(third);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3.txt").unwrap()
    );

    tree.insert(fourth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/4.txt").unwrap()
    );

    tree.insert(fifth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/5.txt").unwrap()
    );

    tree.insert(sixth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/6.txt").unwrap()
    );

    tree.insert(seventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/7.txt").unwrap()
    );

    tree.insert(eighth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/8.txt").unwrap()
    );

    tree.insert(ninth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/9.txt").unwrap()
    );

    tree.insert(tenth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/10.txt").unwrap()
    );

    tree.insert(eleventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/11.txt").unwrap()
    );

    tree.insert(twelfth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/12.txt").unwrap()
    )
}
