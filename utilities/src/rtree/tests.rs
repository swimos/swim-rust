// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::rtree::rectangles::{Point2D, Point3D};
use crate::rtree::{BoxBounded, Label, RTree, RTreeError, Rect, SplitStrategy};
use std::fs;
use std::num::NonZeroUsize;
use std::ops::Sub;
use std::sync::{Arc, Mutex};

fn test_tree<B: BoxBounded, L: Label>(mut tree: RTree<L, B>, entries: Vec<(L, B)>, path: String) {
    assert_eq!(
        format!("{:#?}", tree.root),
        fs::read_to_string(format!("{}/add/0.txt", path)).unwrap()
    );
    assert_eq!(tree.len(), 0);

    for (idx, (label, item)) in entries.clone().into_iter().enumerate() {
        tree.insert(label, item).unwrap();

        assert_eq!(
            format!("{:#?}", tree.root),
            fs::read_to_string(format!("{}/add/{}.txt", path, idx + 1)).unwrap()
        );
        assert_eq!(tree.len(), idx + 1);
    }

    let full_tree_len = tree.len();

    for (idx, (label, item)) in entries.into_iter().enumerate() {
        let removed_item = tree.remove(&label).unwrap();

        assert_eq!(removed_item.get_mbb(), item.get_mbb());

        assert_eq!(
            format!("{:#?}", tree.root),
            fs::read_to_string(format!("{}/remove/{}.txt", path, idx + 1)).unwrap()
        );
        assert_eq!(tree.len(), full_tree_len - idx - 1);
    }
}

fn build_2d_search_tree() -> RTree<String, Rect<Point2D<f64>>> {
    let items = vec![
        ("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0))),
        ("Second".to_string(), rect!((12.0, 0.0), (15.0, 15.0))),
        ("Third".to_string(), rect!((7.0, 7.0), (14.0, 14.0))),
        ("Fourth".to_string(), rect!((10.0, 11.0), (11.0, 12.0))),
        ("Fifth".to_string(), rect!((4.0, 4.0), (5.0, 6.0))),
        ("Sixth".to_string(), rect!((4.0, 9.0), (5.0, 11.0))),
        ("Seventh".to_string(), rect!((13.0, 0.0), (14.0, 1.0))),
        ("Eighth".to_string(), rect!((13.0, 13.0), (16.0, 16.0))),
        ("Ninth".to_string(), rect!((2.0, 13.0), (4.0, 16.0))),
        ("Tenth".to_string(), rect!((2.0, 2.0), (3.0, 3.0))),
        ("Eleventh".to_string(), rect!((10.0, 0.0), (12.0, 5.0))),
        ("Twelfth".to_string(), rect!((7.0, 3.0), (8.0, 6.0))),
    ];

    RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
        items,
    )
    .unwrap()
}

fn build_3d_search_tree() -> RTree<String, Rect<Point3D<f64>>> {
    let items = vec![
        (
            "First".to_string(),
            rect!((0.0, 0.0, 0.0), (10.0, 10.0, 10.0)),
        ),
        (
            "Second".to_string(),
            rect!((12.0, 0.0, 0.0), (15.0, 10.0, 15.0)),
        ),
        (
            "Third".to_string(),
            rect!((7.0, 0.0, 7.0), (14.0, 10.0, 14.0)),
        ),
        (
            "Fourth".to_string(),
            rect!((10.0, 0.0, 11.0), (11.0, 10.0, 12.0)),
        ),
        (
            "Fifth".to_string(),
            rect!((4.0, 0.0, 4.0), (5.0, 10.0, 6.0)),
        ),
        (
            "Sixth".to_string(),
            rect!((4.0, 0.0, 9.0), (5.0, 10.0, 11.0)),
        ),
        (
            "Seventh".to_string(),
            rect!((13.0, 0.0, 0.0), (14.0, 10.0, 1.0)),
        ),
        (
            "Eighth".to_string(),
            rect!((13.0, 0.0, 13.0), (16.0, 10.0, 16.0)),
        ),
        (
            "Ninth".to_string(),
            rect!((2.0, 0.0, 13.0), (4.0, 10.0, 16.0)),
        ),
        (
            "Tenth".to_string(),
            rect!((2.0, 0.0, 2.0), (3.0, 10.0, 3.0)),
        ),
        (
            "Eleventh".to_string(),
            rect!((10.0, 0.0, 0.0), (12.0, 10.0, 5.0)),
        ),
        (
            "Twelfth".to_string(),
            rect!((7.0, 0.0, 3.0), (8.0, 10.0, 6.0)),
        ),
    ];

    RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
        items,
    )
    .unwrap()
}

#[test]
fn rtree_2d_linear_test() {
    let items = vec![
        ("First".to_string(), rect!((0.5, 0.5), (10.5, 10.5))),
        ("Second".to_string(), rect!((12.5, 0.5), (15.5, 15.5))),
        ("Third".to_string(), rect!((7.5, 7.5), (14.5, 14.5))),
        ("Fourth".to_string(), rect!((10.5, 11.5), (11.5, 12.5))),
        ("Fifth".to_string(), rect!((4.5, 4.5), (5.5, 6.5))),
        ("Sixth".to_string(), rect!((4.5, 9.5), (5.5, 11.5))),
        ("Seventh".to_string(), rect!((13.5, 0.5), (14.5, 1.5))),
        ("Eighth".to_string(), rect!((13.5, 13.5), (16.5, 16.5))),
        ("Ninth".to_string(), rect!((2.5, 13.5), (4.5, 16.5))),
        ("Tenth".to_string(), rect!((2.5, 2.5), (3.5, 3.5))),
        ("Eleventh".to_string(), rect!((3.5, 6.5), (4.5, 11.5))),
        ("Twelfth".to_string(), rect!((0.5, 0.5), (5.5, 5.5))),
    ];

    let tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Linear,
    )
    .unwrap();

    test_tree(tree, items, String::from("src/rtree/resources/2d/linear"));
}

#[test]
fn rtree_3d_linear_test() {
    let items = vec![
        (
            "First".to_string(),
            rect!((0.5, 10.5, 0.5), (10.5, 12.6, 10.5)),
        ),
        (
            "Second".to_string(),
            rect!((12.5, 8.5, 0.5), (15.5, 10.6, 15.5)),
        ),
        (
            "Third".to_string(),
            rect!((7.5, 10.5, 7.5), (14.5, 12.6, 14.5)),
        ),
        (
            "Fourth".to_string(),
            rect!((10.5, 8.5, 11.5), (11.5, 10.6, 12.5)),
        ),
        (
            "Fifth".to_string(),
            rect!((4.5, 10.5, 4.5), (5.5, 12.6, 6.5)),
        ),
        (
            "Sixth".to_string(),
            rect!((4.5, 8.5, 9.5), (5.5, 10.6, 11.5)),
        ),
        (
            "Seventh".to_string(),
            rect!((13.5, 10.5, 0.5), (14.5, 12.6, 1.5)),
        ),
        (
            "Eighth".to_string(),
            rect!((13.5, 8.5, 13.5), (16.5, 10.6, 16.5)),
        ),
        (
            "Ninth".to_string(),
            rect!((2.5, 10.5, 13.5), (4.5, 12.6, 16.5)),
        ),
        (
            "Tenth".to_string(),
            rect!((2.5, 11.5, 2.5), (3.5, 11.6, 3.5)),
        ),
        (
            "Eleventh".to_string(),
            rect!((3.5, 10.5, 6.5), (4.5, 12.6, 11.5)),
        ),
        (
            "Twelfth".to_string(),
            rect!((0.5, 10.5, 0.5), (5.5, 12.6, 5.5)),
        ),
    ];

    let tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Linear,
    )
    .unwrap();

    test_tree(tree, items, String::from("src/rtree/resources/3d/linear"));
}

#[test]
fn rtree_2d_quadratic_test() {
    let items = vec![
        ("First".to_string(), rect!((0.5, 0.5), (10.5, 10.5))),
        ("Second".to_string(), rect!((12.5, 0.5), (15.5, 15.5))),
        ("Third".to_string(), rect!((7.5, 7.5), (14.5, 14.5))),
        ("Fourth".to_string(), rect!((10.5, 11.5), (11.5, 12.5))),
        ("Fifth".to_string(), rect!((4.5, 4.5), (5.5, 6.5))),
        ("Sixth".to_string(), rect!((4.5, 9.5), (5.5, 11.5))),
        ("Seventh".to_string(), rect!((13.5, 0.5), (14.5, 1.5))),
        ("Eighth".to_string(), rect!((13.5, 13.5), (16.5, 16.5))),
        ("Ninth".to_string(), rect!((2.5, 13.5), (4.5, 16.5))),
        ("Tenth".to_string(), rect!((2.5, 2.5), (3.5, 3.5))),
        ("Eleventh".to_string(), rect!((10.5, 0.5), (12.5, 5.5))),
        ("Twelfth".to_string(), rect!((7.5, 3.5), (8.5, 6.5))),
    ];

    let tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();

    test_tree(
        tree,
        items,
        String::from("src/rtree/resources/2d/quadratic/"),
    );
}

#[test]
fn rtree_3d_quadratic_test() {
    let items = vec![
        (
            "First".to_string(),
            rect!((0.0, 0.0, 0.0), (10.0, 10.0, 10.0)),
        ),
        (
            "Second".to_string(),
            rect!((12.0, 0.0, 0.0), (15.0, 10.0, 15.0)),
        ),
        (
            "Third".to_string(),
            rect!((7.0, 0.0, 7.0), (14.0, 10.0, 14.0)),
        ),
        (
            "Fourth".to_string(),
            rect!((10.0, 0.0, 11.0), (11.0, 10.0, 12.0)),
        ),
        (
            "Fifth".to_string(),
            rect!((4.0, 0.0, 4.0), (5.0, 10.0, 6.0)),
        ),
        (
            "Sixth".to_string(),
            rect!((4.0, 0.0, 9.0), (5.0, 10.0, 11.0)),
        ),
        (
            "Seventh".to_string(),
            rect!((13.0, 0.0, 0.0), (14.0, 10.0, 1.0)),
        ),
        (
            "Eighth".to_string(),
            rect!((13.0, 0.0, 13.0), (16.0, 10.0, 16.0)),
        ),
        (
            "Ninth".to_string(),
            rect!((2.0, 0.0, 13.0), (4.0, 10.0, 16.0)),
        ),
        (
            "Tenth".to_string(),
            rect!((2.0, 0.0, 2.0), (3.0, 10.0, 3.0)),
        ),
        (
            "Eleventh".to_string(),
            rect!((10.0, 0.0, 0.0), (12.0, 10.0, 5.0)),
        ),
        (
            "Twelfth".to_string(),
            rect!((7.0, 0.0, 3.0), (8.0, 10.0, 6.0)),
        ),
    ];

    let tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();

    test_tree(
        tree,
        items,
        String::from("src/rtree/resources/3d/quadratic/"),
    );
}

#[test]
fn bulk_load_3_node_2d_test() {
    let rects = vec![
        ("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0))),
        ("Second".to_string(), rect!((12.0, 0.0), (15.0, 15.0))),
        ("Third".to_string(), rect!((7.0, 7.0), (14.0, 14.0))),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
        rects,
    )
    .unwrap();

    assert_eq!(
        format!("{:#?}", rtree.root),
        fs::read_to_string("src/rtree/resources/bulk_load/3-node-2d.txt").unwrap()
    );
}

#[test]
fn bulk_load_5_node_2d_test() {
    let items = vec![
        ("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0))),
        ("Second".to_string(), rect!((12.0, 0.0), (15.0, 15.0))),
        ("Third".to_string(), rect!((7.0, 7.0), (14.0, 14.0))),
        ("Fourth".to_string(), rect!((10.0, 11.0), (11.0, 12.0))),
        ("Fifth".to_string(), rect!((4.0, 4.0), (5.0, 6.0))),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
        items,
    )
    .unwrap();

    assert_eq!(
        format!("{:#?}", rtree.root),
        fs::read_to_string("src/rtree/resources/bulk_load/5-node-2d.txt").unwrap()
    );
}

#[test]
fn bulk_load_12_node_2d_test() {
    let items = vec![
        ("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0))),
        ("Second".to_string(), rect!((12.0, 0.0), (15.0, 15.0))),
        ("Third".to_string(), rect!((7.0, 7.0), (14.0, 14.0))),
        ("Fourth".to_string(), rect!((10.0, 11.0), (11.0, 12.0))),
        ("Fifth".to_string(), rect!((4.0, 4.0), (5.0, 6.0))),
        ("Sixth".to_string(), rect!((4.0, 9.0), (5.0, 11.0))),
        ("Seventh".to_string(), rect!((13.0, 0.0), (14.0, 1.0))),
        ("Eighth".to_string(), rect!((13.0, 13.0), (16.0, 16.0))),
        ("Ninth".to_string(), rect!((2.0, 13.0), (4.0, 16.0))),
        ("Tenth".to_string(), rect!((2.0, 2.0), (3.0, 3.0))),
        ("Eleventh".to_string(), rect!((10.0, 0.0), (12.0, 5.0))),
        ("Twelfth".to_string(), rect!((7.0, 3.0), (8.0, 6.0))),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
        items,
    )
    .unwrap();

    assert_eq!(
        format!("{:#?}", rtree.root),
        fs::read_to_string("src/rtree/resources/bulk_load/12-node-2d.txt").unwrap()
    );
}

#[test]
fn bulk_load_24_node_3d_test() {
    let items = vec![
        (
            "First".to_string(),
            rect!((0.0, 0.0, 7.0), (10.0, 10.0, 15.0)),
        ),
        (
            "Second".to_string(),
            rect!((12.0, 0.0, 3.0), (15.0, 15.0, 11.0)),
        ),
        (
            "Third".to_string(),
            rect!((7.0, 7.0, 3.0), (14.0, 14.0, 7.0)),
        ),
        (
            "Fourth".to_string(),
            rect!((10.0, 11.0, 4.0), (11.0, 12.0, 9.0)),
        ),
        ("Fifth".to_string(), rect!((4.0, 4.0, 3.0), (5.0, 6.0, 5.0))),
        (
            "Sixth".to_string(),
            rect!((4.0, 9.0, 1.0), (5.0, 11.0, 6.0)),
        ),
        (
            "Seventh".to_string(),
            rect!((13.0, 0.0, 19.0), (14.0, 1.0, 20.0)),
        ),
        (
            "Eighth".to_string(),
            rect!((13.0, 13.0, 7.0), (16.0, 16.0, 11.0)),
        ),
        (
            "Ninth".to_string(),
            rect!((2.0, 13.0, 11.0), (4.0, 16.0, 14.0)),
        ),
        (
            "Tenth".to_string(),
            rect!((2.0, 2.0, 10.0), (3.0, 3.0, 22.0)),
        ),
        (
            "Eleventh".to_string(),
            rect!((10.0, 0.0, 13.0), (12.0, 5.0, 17.0)),
        ),
        (
            "Twelfth".to_string(),
            rect!((7.0, 3.0, 11.0), (8.0, 6.0, 19.0)),
        ),
        (
            "Thirteenth".to_string(),
            rect!((1.0, 1.0, 0.0), (11.0, 11.0, 4.0)),
        ),
        (
            "Fourteenth".to_string(),
            rect!((13.0, 1.0, 10.0), (16.0, 16.0, 31.0)),
        ),
        (
            "Fifteenth".to_string(),
            rect!((8.0, 8.0, 1.0), (15.0, 15.0, 5.0)),
        ),
        (
            "Sixteenth".to_string(),
            rect!((11.0, 12.0, 10.0), (12.0, 13.0, 12.0)),
        ),
        (
            "Seventeenth".to_string(),
            rect!((5.0, 5.0, 7.0), (6.0, 7.0, 11.0)),
        ),
        (
            "Eighteenth".to_string(),
            rect!((5.0, 10.0, 15.0), (6.0, 12.0, 16.0)),
        ),
        (
            "Nineteenth".to_string(),
            rect!((14.0, 1.0, 14.0), (15.0, 2.0, 15.0)),
        ),
        (
            "Twentieth".to_string(),
            rect!((14.0, 14.0, 13.0), (17.0, 17.0, 15.0)),
        ),
        (
            "Twenty-first".to_string(),
            rect!((3.0, 14.0, 11.0), (5.0, 17.0, 17.0)),
        ),
        (
            "Twenty-second".to_string(),
            rect!((3.0, 3.0, 9.0), (4.0, 4.0, 14.0)),
        ),
        (
            "Twenty-third".to_string(),
            rect!((11.0, 1.0, 5.0), (13.0, 6.0, 15.0)),
        ),
        (
            "Twenty-fourth".to_string(),
            rect!((8.0, 4.0, 3.0), (9.0, 7.0, 13.0)),
        ),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
        items,
    )
    .unwrap();

    assert_eq!(
        format!("{:#?}", rtree.root),
        fs::read_to_string("src/rtree/resources/bulk_load/24-node-3d.txt").unwrap()
    );
}

#[test]
fn search_no_results_2d_test() {
    let tree = build_2d_search_tree();
    let found = tree.search(&rect!((6.0, 11.0), (7.0, 13.0)));
    assert!(found.is_none());
}

#[test]
fn search_single_result_2d_test() {
    let tree = build_2d_search_tree();
    let found = tree.search(&rect!((6.0, 1.0), (9.0, 6.0))).unwrap();
    assert_eq!(found.len(), 1);
}

#[test]
fn search_multiple_results_2d_test() {
    let tree = build_2d_search_tree();
    let found = tree.search(&rect!((7.0, 0.0), (14.0, 15.0))).unwrap();
    assert_eq!(found.len(), 5);
}

#[test]
fn search_no_results_3d_test() {
    let tree = build_3d_search_tree();
    let found = tree.search(&rect!((0.0, 15.0, 0.0), (20.0, 20.0, 20.0)));
    assert!(found.is_none());
}

#[test]
fn search_single_result_3d_test() {
    let tree = build_3d_search_tree();
    let found = tree
        .search(&rect!((10.0, 0.0, 11.0), (11.0, 10.0, 12.0)))
        .unwrap();
    assert_eq!(found.len(), 1);
}

#[test]
fn search_multiple_results_3d_test() {
    let tree = build_3d_search_tree();
    let found = tree
        .search(&rect!((0.0, 0.0, 0.0), (20.0, 20.0, 20.0)))
        .unwrap();
    assert_eq!(found.len(), 12);
}

#[test]
fn tree_iterator_test() {
    let items = vec![
        ("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0))),
        ("Second".to_string(), rect!((12.0, 0.0), (15.0, 15.0))),
        ("Third".to_string(), rect!((7.0, 7.0), (14.0, 14.0))),
        ("Sixth".to_string(), rect!((10.0, 11.0), (11.0, 12.0))),
        ("Twelfth".to_string(), rect!((7.0, 3.0), (8.0, 6.0))),
    ];

    let tree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
        items.clone(),
    )
    .unwrap();

    for (label, item) in tree.iter() {
        assert!(items.contains(&(label.clone(), item.clone())));
    }
}

#[test]
fn tree_immutable_test() {
    let mut tree = build_2d_search_tree();

    let mut tree_clone_1 = tree.clone();
    tree.remove(&"First".to_string()).unwrap();
    tree.remove(&"Third".to_string()).unwrap();
    tree.remove(&"Fifth".to_string()).unwrap();
    tree.remove(&"Seventh".to_string()).unwrap();
    tree.remove(&"Ninth".to_string()).unwrap();
    tree.remove(&"Eleventh".to_string()).unwrap();
    let mut tree_clone_2 = tree.clone();
    tree.remove(&"Second".to_string()).unwrap();
    tree.remove(&"Fourth".to_string()).unwrap();
    tree.remove(&"Sixth".to_string()).unwrap();
    tree.remove(&"Eighth".to_string()).unwrap();
    tree.remove(&"Tenth".to_string()).unwrap();
    tree.remove(&"Twelfth".to_string()).unwrap();

    assert_eq!(tree.len(), 0);
    assert_eq!(tree_clone_1.len(), 12);
    assert_eq!(tree_clone_2.len(), 6);

    tree_clone_1.remove(&"Second".to_string()).unwrap();
    tree_clone_1.remove(&"Fourth".to_string()).unwrap();
    tree_clone_1.remove(&"Sixth".to_string()).unwrap();
    tree_clone_1.remove(&"Eighth".to_string()).unwrap();
    tree_clone_1.remove(&"Tenth".to_string()).unwrap();
    tree_clone_1.remove(&"Twelfth".to_string()).unwrap();

    assert_eq!(tree.len(), 0);
    assert_eq!(tree_clone_1.len(), 6);
    assert_eq!(tree_clone_2.len(), 6);

    tree_clone_2.remove(&"Second".to_string()).unwrap();
    tree_clone_2.remove(&"Fourth".to_string()).unwrap();
    tree_clone_2.remove(&"Sixth".to_string()).unwrap();
    tree_clone_2.remove(&"Eighth".to_string()).unwrap();
    tree_clone_2.remove(&"Tenth".to_string()).unwrap();
    tree_clone_2.remove(&"Twelfth".to_string()).unwrap();

    assert_eq!(tree.len(), 0);
    assert_eq!(tree_clone_1.len(), 6);
    assert_eq!(tree_clone_2.len(), 0);
}

#[test]
fn tree_insert_same_bb() {
    let mut tree = RTree::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(50).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();

    tree.insert("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0)))
        .unwrap();

    tree.insert("Second".to_string(), rect!((0.0, 0.0), (10.0, 10.0)))
        .unwrap();

    assert_eq!(tree.len(), 2)
}

#[test]
fn tree_remove_same_bb_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(50).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();

    tree.insert("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0)))
        .unwrap();

    tree.insert("Second".to_string(), rect!((0.0, 0.0), (10.0, 10.0)))
        .unwrap();

    let item = tree.remove(&"Second".to_string()).unwrap();

    assert_eq!(tree.len(), 1);
    assert_eq!(tree.iter().next().unwrap().0, "First");
    assert_eq!(item.get_mbb().high, Point2D::new(10.0, 10.0));
    assert_eq!(item.get_mbb().low, Point2D::new(0.0, 0.0));
}

#[test]
fn tree_remove_missing_label_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(50).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();

    tree.insert("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0)))
        .unwrap();

    tree.insert("Second".to_string(), rect!((0.0, 0.0), (10.0, 10.0)))
        .unwrap();

    let item = tree.remove(&"Fourth".to_string());

    assert!(item.is_none());
    assert_eq!(tree.len(), 2);
}

#[test]
fn tree_children_size_error_test() {
    let tree: Result<RTree<String, Rect<Point2D<f64>>>, RTreeError<String>> = RTree::new(
        NonZeroUsize::new(10).unwrap(),
        NonZeroUsize::new(19).unwrap(),
        SplitStrategy::Quadratic,
    );

    assert!(matches!(tree, Err(RTreeError::ChildrenSizeError)));

    let tree: Result<RTree<String, Rect<Point2D<f64>>>, RTreeError<String>> = RTree::new(
        NonZeroUsize::new(50).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        SplitStrategy::Quadratic,
    );

    assert!(matches!(tree, Err(RTreeError::ChildrenSizeError)));
}

#[test]
fn tree_insert_same_labels_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(50).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();

    tree.insert("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0)))
        .unwrap();

    let result = tree.insert("First".to_string(), rect!((0.0, 0.0), (20.0, 20.0)));

    if let Err(RTreeError::DuplicateLabelError(label)) = result {
        assert_eq!(label, "First")
    } else {
        panic!("Expected duplicate label error!")
    }
}

#[test]
fn tree_bulk_load_same_labels_test() {
    let items = vec![
        ("First".to_string(), rect!((0.0, 0.0), (10.0, 10.0))),
        ("Second".to_string(), rect!((12.0, 0.0), (15.0, 15.0))),
        ("Third".to_string(), rect!((7.0, 7.0), (14.0, 14.0))),
        ("Third".to_string(), rect!((10.0, 11.0), (11.0, 12.0))),
        ("Twelfth".to_string(), rect!((7.0, 3.0), (8.0, 6.0))),
    ];

    let result = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
        items,
    );

    if let Err(RTreeError::DuplicateLabelError(label)) = result {
        assert_eq!(label, "Third")
    } else {
        panic!("Expected duplicate label error!")
    }
}

#[test]
fn insert_no_clones_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();

    let clone_count = CloneCount::new();
    let first = rect!((0.0, 0.0), (10.0, 10.0));
    let second = rect!((12.0, 0.0), (15.0, 15.0));

    tree.insert(
        "First".to_string(),
        CloneTracker::new(first, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let cloned_tree = tree.clone();

    tree.insert(
        "Second".to_string(),
        CloneTracker::new(second, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    assert_eq!(tree.len(), 2);
    assert_eq!(cloned_tree.len(), 1);
}

#[test]
fn clone_on_remove_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();
    let clone_count = CloneCount::new();

    let first = rect!((0.0, 0.0), (10.0, 10.0));

    tree.insert(
        "First".to_string(),
        CloneTracker::new(first.clone(), clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let cloned_tree = tree.clone();

    tree.remove(&"First".to_string()).unwrap();
    assert_eq!(clone_count.get(), 1);

    assert_eq!(tree.len(), 0);
    assert_eq!(cloned_tree.len(), 1);
}

#[test]
fn linear_split_no_clones_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Linear,
    )
    .unwrap();
    let clone_count = CloneCount::new();

    let first = rect!((0.0, 0.0), (10.0, 10.0));
    let second = rect!((12.0, 0.0), (15.0, 15.0));
    let third = rect!((7.0, 7.0), (14.0, 14.0));
    let fourth = rect!((10.0, 11.0), (11.0, 12.0));
    let fifth = rect!((4.0, 4.0), (5.0, 6.0));

    tree.insert(
        "First".to_string(),
        CloneTracker::new(first, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let first_cloned_tree = tree.clone();

    tree.insert(
        "Second".to_string(),
        CloneTracker::new(second, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let second_cloned_tree = tree.clone();

    tree.insert(
        "Third".to_string(),
        CloneTracker::new(third, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let third_cloned_tree = tree.clone();

    tree.insert(
        "Fourth".to_string(),
        CloneTracker::new(fourth, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let fourth_cloned_tree = tree.clone();

    tree.insert(
        "Fifth".to_string(),
        CloneTracker::new(fifth, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    assert_eq!(tree.len(), 5);
    assert_eq!(first_cloned_tree.len(), 1);
    assert_eq!(second_cloned_tree.len(), 2);
    assert_eq!(third_cloned_tree.len(), 3);
    assert_eq!(fourth_cloned_tree.len(), 4);
}

#[test]
fn quadratic_split_no_clones_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Quadratic,
    )
    .unwrap();
    let clone_count = CloneCount::new();

    let first = rect!((0.0, 0.0), (10.0, 10.0));
    let second = rect!((12.0, 0.0), (15.0, 15.0));
    let third = rect!((7.0, 7.0), (14.0, 14.0));
    let fourth = rect!((10.0, 11.0), (11.0, 12.0));
    let fifth = rect!((4.0, 4.0), (5.0, 6.0));

    tree.insert(
        "First".to_string(),
        CloneTracker::new(first, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let first_cloned_tree = tree.clone();

    tree.insert(
        "Second".to_string(),
        CloneTracker::new(second, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let second_cloned_tree = tree.clone();

    tree.insert(
        "Third".to_string(),
        CloneTracker::new(third, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let third_cloned_tree = tree.clone();

    tree.insert(
        "Fourth".to_string(),
        CloneTracker::new(fourth, clone_count.clone()),
    )
    .unwrap();
    assert_eq!(clone_count.get(), 0);

    let fourth_cloned_tree = tree.clone();

    tree.insert(
        "Fifth".to_string(),
        CloneTracker::new(fifth, clone_count.clone()),
    )
    .unwrap();
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
        SplitStrategy::Quadratic,
    )
    .unwrap();
    let clone_count = CloneCount::new();

    let first = rect!((0.0, 0.0), (10.0, 10.0));
    let second = rect!((12.0, 0.0), (15.0, 15.0));
    let third = rect!((7.0, 7.0), (14.0, 14.0));
    let fourth = rect!((10.0, 11.0), (11.0, 12.0));
    let fifth = rect!((4.0, 4.0), (5.0, 6.0));

    tree.insert(
        "First".to_string(),
        CloneTracker::new(first.clone(), clone_count.clone()),
    )
    .unwrap();
    tree.insert(
        "Second".to_string(),
        CloneTracker::new(second.clone(), clone_count.clone()),
    )
    .unwrap();
    tree.insert(
        "Third".to_string(),
        CloneTracker::new(third.clone(), clone_count.clone()),
    )
    .unwrap();
    tree.insert(
        "Fourth".to_string(),
        CloneTracker::new(fourth.clone(), clone_count.clone()),
    )
    .unwrap();
    tree.insert(
        "Fifth".to_string(),
        CloneTracker::new(fifth.clone(), clone_count.clone()),
    )
    .unwrap();

    assert_eq!(clone_count.get(), 0);

    let first_cloned_tree = tree.clone();

    tree.remove(&"First".to_string()).unwrap();
    assert_eq!(clone_count.get(), 1);

    let second_cloned_tree = tree.clone();

    tree.remove(&"Second".to_string()).unwrap();
    assert_eq!(clone_count.get(), 2);

    let third_cloned_tree = tree.clone();

    tree.remove(&"Third".to_string()).unwrap();
    assert_eq!(clone_count.get(), 3);

    let fourth_cloned_tree = tree.clone();

    tree.remove(&"Fourth".to_string()).unwrap();
    assert_eq!(clone_count.get(), 4);

    let fifth_cloned_tree = tree.clone();

    tree.remove(&"Fifth".to_string()).unwrap();
    assert_eq!(clone_count.get(), 5);

    assert_eq!(tree.len(), 0);
    assert_eq!(first_cloned_tree.len(), 5);
    assert_eq!(second_cloned_tree.len(), 4);
    assert_eq!(third_cloned_tree.len(), 3);
    assert_eq!(fourth_cloned_tree.len(), 2);
    assert_eq!(fifth_cloned_tree.len(), 1);
}

#[test]
fn bulk_load_no_clone() {
    let clone_count = CloneCount::new();

    let items = vec![
        (
            "First".to_string(),
            CloneTracker::new(rect!((0.0, 0.0), (10.0, 10.0)), clone_count.clone()),
        ),
        (
            "Second".to_string(),
            CloneTracker::new(rect!((12.0, 0.0), (15.0, 15.0)), clone_count.clone()),
        ),
        (
            "Third".to_string(),
            CloneTracker::new(rect!((7.0, 7.0), (14.0, 14.0)), clone_count.clone()),
        ),
        (
            "Fourth".to_string(),
            CloneTracker::new(rect!((10.0, 11.0), (11.0, 12.0)), clone_count.clone()),
        ),
        (
            "Fifth".to_string(),
            CloneTracker::new(rect!((4.0, 4.0), (5.0, 6.0)), clone_count.clone()),
        ),
        (
            "Sixth".to_string(),
            CloneTracker::new(rect!((4.0, 9.0), (5.0, 11.0)), clone_count.clone()),
        ),
        (
            "Seventh".to_string(),
            CloneTracker::new(rect!((13.0, 0.0), (14.0, 1.0)), clone_count.clone()),
        ),
        (
            "Eighth".to_string(),
            CloneTracker::new(rect!((13.0, 13.0), (16.0, 16.0)), clone_count.clone()),
        ),
        (
            "Ninth".to_string(),
            CloneTracker::new(rect!((2.0, 13.0), (4.0, 16.0)), clone_count.clone()),
        ),
        (
            "Tenth".to_string(),
            CloneTracker::new(rect!((2.0, 2.0), (3.0, 3.0)), clone_count.clone()),
        ),
        (
            "Eleventh".to_string(),
            CloneTracker::new(rect!((10.0, 0.0), (12.0, 5.0)), clone_count.clone()),
        ),
        (
            "Twelfth".to_string(),
            CloneTracker::new(rect!((7.0, 3.0), (8.0, 6.0)), clone_count.clone()),
        ),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Linear,
        items,
    )
    .unwrap();

    assert_eq!(clone_count.get(), 0);
    assert_eq!(rtree.len(), 12);
}

#[test]
fn search_single_no_clone() {
    let clone_count = CloneCount::new();

    let items = vec![
        (
            "First".to_string(),
            CloneTracker::new(rect!((0.0, 0.0), (10.0, 10.0)), clone_count.clone()),
        ),
        (
            "Second".to_string(),
            CloneTracker::new(rect!((12.0, 0.0), (15.0, 15.0)), clone_count.clone()),
        ),
        (
            "Third".to_string(),
            CloneTracker::new(rect!((7.0, 7.0), (14.0, 14.0)), clone_count.clone()),
        ),
        (
            "Fourth".to_string(),
            CloneTracker::new(rect!((10.0, 11.0), (11.0, 12.0)), clone_count.clone()),
        ),
        (
            "Fifth".to_string(),
            CloneTracker::new(rect!((4.0, 4.0), (5.0, 6.0)), clone_count.clone()),
        ),
        (
            "Sixth".to_string(),
            CloneTracker::new(rect!((4.0, 9.0), (5.0, 11.0)), clone_count.clone()),
        ),
        (
            "Seventh".to_string(),
            CloneTracker::new(rect!((13.0, 0.0), (14.0, 1.0)), clone_count.clone()),
        ),
        (
            "Eighth".to_string(),
            CloneTracker::new(rect!((13.0, 13.0), (16.0, 16.0)), clone_count.clone()),
        ),
        (
            "Ninth".to_string(),
            CloneTracker::new(rect!((2.0, 13.0), (4.0, 16.0)), clone_count.clone()),
        ),
        (
            "Tenth".to_string(),
            CloneTracker::new(rect!((2.0, 2.0), (3.0, 3.0)), clone_count.clone()),
        ),
        (
            "Eleventh".to_string(),
            CloneTracker::new(rect!((10.0, 0.0), (12.0, 5.0)), clone_count.clone()),
        ),
        (
            "Twelfth".to_string(),
            CloneTracker::new(rect!((7.0, 3.0), (8.0, 6.0)), clone_count.clone()),
        ),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Linear,
        items,
    )
    .unwrap();

    let found = rtree.search(&rect!((2.0, 2.0), (4.0, 4.0))).unwrap();
    assert_eq!(clone_count.get(), 0);
    assert_eq!(found.len(), 1);
}

#[test]
fn search_multiple_no_clone() {
    let clone_count = CloneCount::new();

    let items = vec![
        (
            "First".to_string(),
            CloneTracker::new(rect!((0.0, 0.0), (10.0, 10.0)), clone_count.clone()),
        ),
        (
            "Second".to_string(),
            CloneTracker::new(rect!((12.0, 0.0), (15.0, 15.0)), clone_count.clone()),
        ),
        (
            "Third".to_string(),
            CloneTracker::new(rect!((7.0, 7.0), (14.0, 14.0)), clone_count.clone()),
        ),
        (
            "Fourth".to_string(),
            CloneTracker::new(rect!((10.0, 11.0), (11.0, 12.0)), clone_count.clone()),
        ),
        (
            "Fifth".to_string(),
            CloneTracker::new(rect!((4.0, 4.0), (5.0, 6.0)), clone_count.clone()),
        ),
        (
            "Sixth".to_string(),
            CloneTracker::new(rect!((4.0, 9.0), (5.0, 11.0)), clone_count.clone()),
        ),
        (
            "Seventh".to_string(),
            CloneTracker::new(rect!((13.0, 0.0), (14.0, 1.0)), clone_count.clone()),
        ),
        (
            "Eighth".to_string(),
            CloneTracker::new(rect!((13.0, 13.0), (16.0, 16.0)), clone_count.clone()),
        ),
        (
            "Ninth".to_string(),
            CloneTracker::new(rect!((2.0, 13.0), (4.0, 16.0)), clone_count.clone()),
        ),
        (
            "Tenth".to_string(),
            CloneTracker::new(rect!((2.0, 2.0), (3.0, 3.0)), clone_count.clone()),
        ),
        (
            "Eleventh".to_string(),
            CloneTracker::new(rect!((10.0, 0.0), (12.0, 5.0)), clone_count.clone()),
        ),
        (
            "Twelfth".to_string(),
            CloneTracker::new(rect!((7.0, 3.0), (8.0, 6.0)), clone_count.clone()),
        ),
    ];

    let rtree = RTree::bulk_load(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Linear,
        items,
    )
    .unwrap();

    let found = rtree.search(&rect!((2.0, 2.0), (10.0, 10.0))).unwrap();
    assert_eq!(clone_count.get(), 0);
    assert_eq!(found.len(), 3);
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

impl BoxBounded for CloneTracker {
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

#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
struct TestPoint4D {}

impl Sub for TestPoint4D {
    type Output = TestPoint4D;

    fn sub(self, _rhs: Self) -> Self::Output {
        unimplemented!()
    }
}
