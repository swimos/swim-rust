use crate::rtree::rect::{Point2D, Point3D};
use crate::rtree::{BoundingBox, RTree, Rect, Strategy};
use std::fs;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

//Todo add unit tests for search
// let found = tree.search(&rect!((7, 0), (14, 15)));
//
// eprintln!("found = {:#?}", found);
// assert_eq!(found.unwrap().len(), 5);

#[test]
fn bulk_load_5_node_test() {
    let rects = vec![
        rect!((0.0, 0.0), (10.0, 10.0)),
        rect!((12.0, 0.0), (15.0, 15.0)),
        rect!((7.0, 7.0), (14.0, 14.0)),
        rect!((10.0, 11.0), (11.0, 12.0)),
        rect!((4.0, 4.0), (5.0, 6.0)),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        Strategy::Quadratic,
        rects,
    );

    assert_eq!(
        format!("{:#?}", rtree),
        fs::read_to_string("src/rtree/resources/bulk_load/5-node.txt").unwrap()
    );
}

#[test]
fn bulk_load_12_node_test() {
    let rects = vec![
        rect!((0.0, 0.0), (10.0, 10.0)),
        rect!((12.0, 0.0), (15.0, 15.0)),
        rect!((7.0, 7.0), (14.0, 14.0)),
        rect!((10.0, 11.0), (11.0, 12.0)),
        rect!((4.0, 4.0), (5.0, 6.0)),
        rect!((4.0, 9.0), (5.0, 11.0)),
        rect!((13.0, 0.0), (14.0, 1.0)),
        rect!((13.0, 13.0), (16.0, 16.0)),
        rect!((2.0, 13.0), (4.0, 16.0)),
        rect!((2.0, 2.0), (3.0, 3.0)),
        rect!((10.0, 0.0), (12.0, 5.0)),
        rect!((7.0, 3.0), (8.0, 6.0)),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        Strategy::Quadratic,
        rects,
    );

    assert_eq!(
        format!("{:#?}", rtree),
        fs::read_to_string("src/rtree/resources/bulk_load/12-node.txt").unwrap()
    );
}

#[test]
fn rtree_2d_test() {
    let first = rect!((0.5, 0.5), (10.5, 10.5));
    let second = rect!((12.5, 0.5), (15.5, 15.5));
    let third = rect!((7.5, 7.5), (14.5, 14.5));
    let fourth = rect!((10.5, 11.5), (11.5, 12.5));
    let fifth = rect!((4.5, 4.5), (5.5, 6.5));
    let sixth = rect!((4.5, 9.5), (5.5, 11.5));
    let seventh = rect!((13.5, 0.5), (14.5, 1.5));
    let eighth = rect!((13.5, 13.5), (16.5, 16.5));
    let ninth = rect!((2.5, 13.5), (4.5, 16.5));
    let tenth = rect!((2.5, 2.5), (3.5, 3.5));
    let eleventh = rect!((10.5, 0.5), (12.5, 5.5));
    let twelfth = rect!((7.5, 3.5), (8.5, 6.5));

    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        Strategy::Quadratic,
    );

    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/0.txt").unwrap()
    );

    tree.insert(first.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/1.txt").unwrap()
    );

    tree.insert(second.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/2.txt").unwrap()
    );

    tree.insert(third.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/3.txt").unwrap()
    );

    tree.insert(fourth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/4.txt").unwrap()
    );

    tree.insert(fifth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/5.txt").unwrap()
    );

    tree.insert(sixth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/6.txt").unwrap()
    );

    tree.insert(seventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/7.txt").unwrap()
    );

    tree.insert(eighth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/8.txt").unwrap()
    );

    tree.insert(ninth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/9.txt").unwrap()
    );

    tree.insert(tenth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/10.txt").unwrap()
    );

    tree.insert(eleventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/11.txt").unwrap()
    );

    tree.insert(twelfth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/add/12.txt").unwrap()
    );

    tree.remove(&first);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/1.txt").unwrap()
    );

    tree.remove(&second);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/2.txt").unwrap()
    );

    tree.remove(&third);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/3.txt").unwrap()
    );

    tree.remove(&fourth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/4.txt").unwrap()
    );

    tree.remove(&fifth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/5.txt").unwrap()
    );

    tree.remove(&sixth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/6.txt").unwrap()
    );

    tree.remove(&seventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/7.txt").unwrap()
    );

    tree.remove(&eighth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/8.txt").unwrap()
    );

    tree.remove(&ninth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/9.txt").unwrap()
    );

    tree.remove(&tenth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/10.txt").unwrap()
    );

    tree.remove(&eleventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/remove/11.txt").unwrap()
    );

    tree.remove(&twelfth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/0.txt").unwrap()
    );
}

#[test]
fn rtree_3d_test() {
    let first = rect!((0.0, 0.0, 0.0), (10.0, 10.0, 10.0));
    let second = rect!((12.0, 0.0, 0.0), (15.0, 15.0, 10.0));
    let third = rect!((7.0, 7.0, 0.0), (14.0, 14.0, 10.0));
    let fourth = rect!((10.0, 11.0, 0.0), (11.0, 12.0, 10.0));
    let fifth = rect!((4.0, 4.0, 0.0), (5.0, 6.0, 10.0));
    let sixth = rect!((4.0, 9.0, 0.0), (5.0, 11.0, 10.0));
    let seventh = rect!((13.0, 0.0, 0.0), (14.0, 1.0, 10.0));
    let eighth = rect!((13.0, 13.0, 0.0), (16.0, 16.0, 10.0));
    let ninth = rect!((2.0, 13.0, 0.0), (4.0, 16.0, 10.0));
    let tenth = rect!((2.0, 2.0, 0.0), (3.0, 3.0, 10.0));
    let eleventh = rect!((10.0, 0.0, 0.0), (12.0, 5.0, 10.0));
    let twelfth = rect!((7.0, 3.0, 0.0), (8.0, 6.0, 10.0));

    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        Strategy::Quadratic,
    );
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/0.txt").unwrap()
    );

    tree.insert(first.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/1.txt").unwrap()
    );

    tree.insert(second.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/2.txt").unwrap()
    );

    tree.insert(third.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/3.txt").unwrap()
    );

    tree.insert(fourth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/4.txt").unwrap()
    );

    tree.insert(fifth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/5.txt").unwrap()
    );

    tree.insert(sixth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/6.txt").unwrap()
    );

    tree.insert(seventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/7.txt").unwrap()
    );

    tree.insert(eighth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/8.txt").unwrap()
    );

    tree.insert(ninth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/9.txt").unwrap()
    );

    tree.insert(tenth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/10.txt").unwrap()
    );

    tree.insert(eleventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/11.txt").unwrap()
    );

    tree.insert(twelfth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/add/12.txt").unwrap()
    );

    tree.remove(&first);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/1.txt").unwrap()
    );

    tree.remove(&second);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/2.txt").unwrap()
    );

    tree.remove(&third);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/3.txt").unwrap()
    );

    tree.remove(&fourth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/4.txt").unwrap()
    );

    tree.remove(&fifth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/5.txt").unwrap()
    );

    tree.remove(&sixth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/6.txt").unwrap()
    );

    tree.remove(&seventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/7.txt").unwrap()
    );

    tree.remove(&eighth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/8.txt").unwrap()
    );

    tree.remove(&ninth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/9.txt").unwrap()
    );

    tree.remove(&tenth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/10.txt").unwrap()
    );

    tree.remove(&eleventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/remove/11.txt").unwrap()
    );

    tree.remove(&twelfth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/3d/0.txt").unwrap()
    );
}

#[test]
fn insert_no_clones_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        Strategy::Quadratic,
    );
    let clone_count = CloneCount::new();
    let first = rect!((0.0, 0.0), (10.0, 10.0));
    let second = rect!((12.0, 0.0), (15.0, 15.0));

    tree.insert(CloneTracker::new(first, clone_count.clone()));
    assert_eq!(clone_count.get(), 0);

    let cloned_tree = tree.clone();

    tree.insert(CloneTracker::new(second, clone_count.clone()));
    assert_eq!(clone_count.get(), 0);

    assert_eq!(tree.len(), 2);
    assert_eq!(cloned_tree.len(), 1);
}

#[test]
fn clone_on_remove_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        Strategy::Quadratic,
    );
    let clone_count = CloneCount::new();

    let first = rect!((0.0, 0.0), (10.0, 10.0));

    tree.insert(CloneTracker::new(first.clone(), clone_count.clone()));
    assert_eq!(clone_count.get(), 0);

    let cloned_tree = tree.clone();

    tree.remove(&first);
    assert_eq!(clone_count.get(), 1);

    assert_eq!(tree.len(), 0);
    assert_eq!(cloned_tree.len(), 1);
}

#[test]
fn split_no_clones_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        Strategy::Quadratic,
    );
    let clone_count = CloneCount::new();

    let first = rect!((0.0, 0.0), (10.0, 10.0));
    let second = rect!((12.0, 0.0), (15.0, 15.0));
    let third = rect!((7.0, 7.0), (14.0, 14.0));
    let fourth = rect!((10.0, 11.0), (11.0, 12.0));
    let fifth = rect!((4.0, 4.0), (5.0, 6.0));

    tree.insert(CloneTracker::new(first, clone_count.clone()));
    assert_eq!(clone_count.get(), 0);

    let first_cloned_tree = tree.clone();

    tree.insert(CloneTracker::new(second, clone_count.clone()));
    assert_eq!(clone_count.get(), 0);

    let second_cloned_tree = tree.clone();

    tree.insert(CloneTracker::new(third, clone_count.clone()));
    assert_eq!(clone_count.get(), 0);

    let third_cloned_tree = tree.clone();

    tree.insert(CloneTracker::new(fourth, clone_count.clone()));
    assert_eq!(clone_count.get(), 0);

    let fourth_cloned_tree = tree.clone();

    tree.insert(CloneTracker::new(fifth, clone_count.clone()));
    assert_eq!(clone_count.get(), 0);

    assert_eq!(tree.len(), 5);
    assert_eq!(first_cloned_tree.len(), 1);
    assert_eq!(second_cloned_tree.len(), 2);
    assert_eq!(third_cloned_tree.len(), 3);
    assert_eq!(fourth_cloned_tree.len(), 4);
}

#[test]
fn clone_on_merge_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        Strategy::Quadratic,
    );
    let clone_count = CloneCount::new();

    let first = rect!((0.0, 0.0), (10.0, 10.0));
    let second = rect!((12.0, 0.0), (15.0, 15.0));
    let third = rect!((7.0, 7.0), (14.0, 14.0));
    let fourth = rect!((10.0, 11.0), (11.0, 12.0));
    let fifth = rect!((4.0, 4.0), (5.0, 6.0));

    tree.insert(CloneTracker::new(first.clone(), clone_count.clone()));
    tree.insert(CloneTracker::new(second.clone(), clone_count.clone()));
    tree.insert(CloneTracker::new(third.clone(), clone_count.clone()));
    tree.insert(CloneTracker::new(fourth.clone(), clone_count.clone()));
    tree.insert(CloneTracker::new(fifth.clone(), clone_count.clone()));

    assert_eq!(clone_count.get(), 0);

    let first_cloned_tree = tree.clone();

    tree.remove(&first);
    assert_eq!(clone_count.get(), 1);

    let second_cloned_tree = tree.clone();

    tree.remove(&second);
    assert_eq!(clone_count.get(), 2);

    let third_cloned_tree = tree.clone();

    tree.remove(&third);
    assert_eq!(clone_count.get(), 3);

    let fourth_cloned_tree = tree.clone();

    tree.remove(&fourth);
    assert_eq!(clone_count.get(), 4);

    let fifth_cloned_tree = tree.clone();

    tree.remove(&fifth);
    assert_eq!(clone_count.get(), 5);

    assert_eq!(tree.len(), 0);
    assert_eq!(first_cloned_tree.len(), 5);
    assert_eq!(second_cloned_tree.len(), 4);
    assert_eq!(third_cloned_tree.len(), 3);
    assert_eq!(fourth_cloned_tree.len(), 2);
    assert_eq!(fifth_cloned_tree.len(), 1);
}

#[test]
fn clone_tracker_test() {
    let first = rect!((0.0, 0.0), (10.0, 10.0));
    let clone_count = CloneCount::new();

    let first_clone_tracker = CloneTracker::new(first.clone(), clone_count.clone());

    assert_eq!(clone_count.get(), 0);
    let _ = first_clone_tracker.clone();
    assert_eq!(clone_count.get(), 1);
    let _ = first_clone_tracker.clone();
    assert_eq!(clone_count.get(), 2);
}

#[derive(Debug)]
struct CloneTracker {
    mbb: Rect<Point2D<f64>>,
    clone_count: CloneCount,
}

impl CloneTracker {
    fn new(rect: Rect<Point2D<f64>>, clone_count: CloneCount) -> Self {
        CloneTracker {
            mbb: rect,
            clone_count,
        }
    }
}

impl Clone for CloneTracker {
    fn clone(&self) -> Self {
        self.clone_count.inc();

        CloneTracker {
            mbb: self.mbb.clone(),
            clone_count: self.clone_count.clone(),
        }
    }
}

impl BoundingBox for CloneTracker {
    type Point = Point2D<f64>;

    fn get_mbb(&self) -> &Rect<Self::Point> {
        &self.mbb
    }

    fn get_center(&self) -> Self::Point {
        self.mbb.get_center()
    }

    fn measure(&self) -> f64 {
        self.mbb.measure()
    }

    fn combine_boxes<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> Rect<Self::Point> {
        self.mbb.combine_boxes(other)
    }

    fn is_covering<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> bool {
        self.mbb.is_covering(other)
    }

    fn is_intersecting<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> bool {
        self.mbb.is_intersecting(other)
    }
}

#[derive(Debug, Clone)]
struct CloneCount(Arc<Mutex<i32>>);

impl CloneCount {
    fn new() -> Self {
        CloneCount(Arc::new(Mutex::new(0)))
    }

    fn inc(&self) {
        *self.0.lock().unwrap() += 1;
    }

    fn get(&self) -> i32 {
        *self.0.lock().unwrap()
    }
}
