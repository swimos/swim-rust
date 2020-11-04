use crate::rtree::rect::{Point2D, Point3D};
use crate::rtree::{BoundingBox, RTree, Rect, bulk_load};
use std::fs;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

//Todo add unit tests for search
// let found = tree.search(&rect!((7, 0), (14, 15)));
//
// eprintln!("found = {:#?}", found);
// assert_eq!(found.unwrap().len(), 5);

#[test]
fn bulk_load_test() {
    let rects: Vec<Rect<i32, Point2D<i32>>> = vec![
        rect!((0, 0), (10, 10)),
        rect!((12, 0), (15, 15)),
        rect!((7, 7), (14, 14)),
        rect!((10, 11), (11, 12)),
        rect!((4, 4), (5, 6)),
        rect!((4, 9), (5, 11)),
        rect!((13, 0), (14, 1)),
        rect!((13, 13), (16, 16)),
        rect!((2, 13), (4, 16)),
        rect!((2, 2), (3, 3)),
        rect!((10, 0), (12, 5)),
        rect!((7, 3), (8, 6)),
    ];

    let rtree = bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        rects,
    );

    eprintln!("rtree = {:#?}", rtree);
}

#[test]
fn rtree_2d_i32_test() {
    let first = rect!((0, 0), (10, 10));
    let second = rect!((12, 0), (15, 15));
    let third = rect!((7, 7), (14, 14));
    let fourth = rect!((10, 11), (11, 12));
    let fifth = rect!((4, 4), (5, 6));
    let sixth = rect!((4, 9), (5, 11));
    let seventh = rect!((13, 0), (14, 1));
    let eighth = rect!((13, 13), (16, 16));
    let ninth = rect!((2, 13), (4, 16));
    let tenth = rect!((2, 2), (3, 3));
    let eleventh = rect!((10, 0), (12, 5));
    let twelfth = rect!((7, 3), (8, 6));

    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());

    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/0.txt").unwrap()
    );

    tree.insert(first.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/1.txt").unwrap()
    );

    tree.insert(second.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/2.txt").unwrap()
    );

    tree.insert(third.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/3.txt").unwrap()
    );

    tree.insert(fourth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/4.txt").unwrap()
    );

    tree.insert(fifth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/5.txt").unwrap()
    );

    tree.insert(sixth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/6.txt").unwrap()
    );

    tree.insert(seventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/7.txt").unwrap()
    );

    tree.insert(eighth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/8.txt").unwrap()
    );

    tree.insert(ninth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/9.txt").unwrap()
    );

    tree.insert(tenth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/10.txt").unwrap()
    );

    tree.insert(eleventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/11.txt").unwrap()
    );

    tree.insert(twelfth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/add/12.txt").unwrap()
    );

    tree.remove(&first);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/1.txt").unwrap()
    );

    tree.remove(&second);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/2.txt").unwrap()
    );

    tree.remove(&third);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/3.txt").unwrap()
    );

    tree.remove(&fourth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/4.txt").unwrap()
    );

    tree.remove(&fifth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/5.txt").unwrap()
    );

    tree.remove(&sixth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/6.txt").unwrap()
    );

    tree.remove(&seventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/7.txt").unwrap()
    );

    tree.remove(&eighth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/8.txt").unwrap()
    );

    tree.remove(&ninth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/9.txt").unwrap()
    );

    tree.remove(&tenth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/10.txt").unwrap()
    );

    tree.remove(&eleventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/remove/11.txt").unwrap()
    );

    tree.remove(&twelfth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/i32/0.txt").unwrap()
    );
}

#[test]
fn rtree_2d_f32_test() {
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

    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());

    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/0.txt").unwrap()
    );

    tree.insert(first.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/1.txt").unwrap()
    );

    tree.insert(second.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/2.txt").unwrap()
    );

    tree.insert(third.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/3.txt").unwrap()
    );

    tree.insert(fourth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/4.txt").unwrap()
    );

    tree.insert(fifth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/5.txt").unwrap()
    );

    tree.insert(sixth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/6.txt").unwrap()
    );

    tree.insert(seventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/7.txt").unwrap()
    );

    tree.insert(eighth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/8.txt").unwrap()
    );

    tree.insert(ninth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/9.txt").unwrap()
    );

    tree.insert(tenth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/10.txt").unwrap()
    );

    tree.insert(eleventh.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/11.txt").unwrap()
    );

    tree.insert(twelfth.clone());
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/add/12.txt").unwrap()
    );

    tree.remove(&first);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/1.txt").unwrap()
    );

    tree.remove(&second);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/2.txt").unwrap()
    );

    tree.remove(&third);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/3.txt").unwrap()
    );

    tree.remove(&fourth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/4.txt").unwrap()
    );

    tree.remove(&fifth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/5.txt").unwrap()
    );

    tree.remove(&sixth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/6.txt").unwrap()
    );

    tree.remove(&seventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/7.txt").unwrap()
    );

    tree.remove(&eighth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/8.txt").unwrap()
    );

    tree.remove(&ninth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/9.txt").unwrap()
    );

    tree.remove(&tenth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/10.txt").unwrap()
    );

    tree.remove(&eleventh);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/remove/11.txt").unwrap()
    );

    tree.remove(&twelfth);
    assert_eq!(
        format!("{:#?}", tree),
        fs::read_to_string("src/rtree/resources/2d/f32/0.txt").unwrap()
    );
}

#[test]
fn rtree_3d_test() {
    let first = rect!((0, 0, 0), (10, 10, 10));
    let second = rect!((12, 0, 0), (15, 15, 10));
    let third = rect!((7, 7, 0), (14, 14, 10));
    let fourth = rect!((10, 11, 0), (11, 12, 10));
    let fifth = rect!((4, 4, 0), (5, 6, 10));
    let sixth = rect!((4, 9, 0), (5, 11, 10));
    let seventh = rect!((13, 0, 0), (14, 1, 10));
    let eighth = rect!((13, 13, 0), (16, 16, 10));
    let ninth = rect!((2, 13, 0), (4, 16, 10));
    let tenth = rect!((2, 2, 0), (3, 3, 10));
    let eleventh = rect!((10, 0, 0), (12, 5, 10));
    let twelfth = rect!((7, 3, 0), (8, 6, 10));

    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());
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
    let mut tree = RTree::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap());
    let clone_count = CloneCount::new();
    let first = rect!((0, 0), (10, 10));
    let second = rect!((12, 0), (15, 15));

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

    let first = rect!((0, 0), (10, 10));

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

    let first = rect!((0, 0), (10, 10));
    let second = rect!((12, 0), (15, 15));
    let third = rect!((7, 7), (14, 14));
    let fourth = rect!((10, 11), (11, 12));
    let fifth = rect!((4, 4), (5, 6));

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

    let first = rect!((0, 0), (10, 10));
    let second = rect!((12, 0), (15, 15));
    let third = rect!((7, 7), (14, 14));
    let fourth = rect!((10, 11), (11, 12));
    let fifth = rect!((4, 4), (5, 6));

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
    let first = rect!((0, 0), (10, 10));
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

    fn is_intersecting<B: BoundingBox<i32, Point2D<i32>>>(&self, other: &B) -> bool {
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
