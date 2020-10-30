use crate::rtree::{BoundingBox, Point2D, RTree, Rect};
use std::fs;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

#[test]
fn rtree_test() {
    let first = Rect::new(Point2D::new(0, 0), Point2D::new(10, 10));
    let second = Rect::new(Point2D::new(12, 0), Point2D::new(15, 15));
    let third = Rect::new(Point2D::new(7, 7), Point2D::new(14, 14));
    let fourth = Rect::new(Point2D::new(10, 11), Point2D::new(11, 12));
    let fifth = Rect::new(Point2D::new(4, 4), Point2D::new(5, 6));
    let sixth = Rect::new(Point2D::new(4, 9), Point2D::new(5, 11));
    let seventh = Rect::new(Point2D::new(13, 0), Point2D::new(14, 1));
    let eighth = Rect::new(Point2D::new(13, 13), Point2D::new(16, 16));
    let ninth = Rect::new(Point2D::new(2, 13), Point2D::new(4, 16));
    let tenth = Rect::new(Point2D::new(2, 2), Point2D::new(3, 3));
    let eleventh = Rect::new(Point2D::new(10, 0), Point2D::new(12, 5));
    let twelfth = Rect::new(Point2D::new(7, 3), Point2D::new(8, 6));

    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());

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

#[test]
fn insert_no_clones_test() {
    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());
    let clone_count = CloneCount::new();
    let first = Rect::new(Point2D::new(0, 0), Point2D::new(10, 10));
    let second = Rect::new(Point2D::new(12, 0), Point2D::new(15, 15));

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
    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());
    let clone_count = CloneCount::new();

    let first = Rect::new(Point2D::new(0, 0), Point2D::new(10, 10));

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
    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());
    let clone_count = CloneCount::new();

    let first = Rect::new(Point2D::new(0, 0), Point2D::new(10, 10));
    let second = Rect::new(Point2D::new(12, 0), Point2D::new(15, 15));
    let third = Rect::new(Point2D::new(7, 7), Point2D::new(14, 14));
    let fourth = Rect::new(Point2D::new(10, 11), Point2D::new(11, 12));
    let fifth = Rect::new(Point2D::new(4, 4), Point2D::new(5, 6));

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
    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());
    let clone_count = CloneCount::new();

    let first = Rect::new(Point2D::new(0, 0), Point2D::new(10, 10));
    let second = Rect::new(Point2D::new(12, 0), Point2D::new(15, 15));
    let third = Rect::new(Point2D::new(7, 7), Point2D::new(14, 14));
    let fourth = Rect::new(Point2D::new(10, 11), Point2D::new(11, 12));
    let fifth = Rect::new(Point2D::new(4, 4), Point2D::new(5, 6));

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
    let first = Rect::new(Point2D::new(0, 0), Point2D::new(10, 10));
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
    mbb: Rect<i32, Point2D<i32>>,
    clone_count: CloneCount,
}

impl CloneTracker {
    fn new(rect: Rect<i32, Point2D<i32>>, clone_count: CloneCount) -> Self {
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

impl BoundingBox<i32, Point2D<i32>> for CloneTracker {
    fn get_mbb(&self) -> &Rect<i32, Point2D<i32>> {
        &self.mbb
    }

    fn measure(&self) -> i32 {
        self.mbb.measure()
    }

    fn combine_boxes<T: BoundingBox<i32, Point2D<i32>>>(
        &self,
        other: &T,
    ) -> Rect<i32, Point2D<i32>> {
        self.mbb.combine_boxes(other)
    }

    fn is_covering<T: BoundingBox<i32, Point2D<i32>>>(&self, other: &T) -> bool {
        self.mbb.is_covering(other)
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
