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

    tree.insert(first.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/1.txt").unwrap()
    );

    tree.insert(second.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/2.txt").unwrap()
    );

    tree.insert(third.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/3.txt").unwrap()
    );

    tree.insert(fourth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/4.txt").unwrap()
    );

    tree.insert(fifth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/5.txt").unwrap()
    );

    tree.insert(sixth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/6.txt").unwrap()
    );

    tree.insert(seventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/7.txt").unwrap()
    );

    tree.insert(eighth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/8.txt").unwrap()
    );

    tree.insert(ninth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/9.txt").unwrap()
    );

    tree.insert(tenth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/10.txt").unwrap()
    );

    tree.insert(eleventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/11.txt").unwrap()
    );

    tree.insert(twelfth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/add/12.txt").unwrap()
    );

    tree.remove(&first);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/1.txt").unwrap()
    );

    tree.remove(&second);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/2.txt").unwrap()
    );

    tree.remove(&third);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/3.txt").unwrap()
    );

    tree.remove(&fourth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/4.txt").unwrap()
    );

    tree.remove(&fifth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/5.txt").unwrap()
    );

    tree.remove(&sixth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/6.txt").unwrap()
    );

    tree.remove(&seventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/7.txt").unwrap()
    );

    tree.remove(&eighth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/8.txt").unwrap()
    );

    tree.remove(&ninth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/9.txt").unwrap()
    );

    tree.remove(&tenth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/10.txt").unwrap()
    );

    tree.remove(&eleventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/remove/11.txt").unwrap()
    );

    tree.remove(&twelfth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/0.txt").unwrap()
    );
}
