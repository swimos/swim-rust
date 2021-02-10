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
use crate::rtree::{BoxBounded, RTree, Rect, SplitStrategy};
use std::fs;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

#[test]
fn foo_test() {
    let mut tree = RTree::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        SplitStrategy::Linear,
    );

    tree.insert("First".to_string(), rect!((0.5, 0.5), (10.5, 10.5)));

    eprintln!("tree = {:#?}", tree);

    // let rects = vec![
    //     rect!((0.5, 0.5), (10.5, 10.5)),
    //     rect!((12.5, 0.5), (15.5, 15.5)),
    //     rect!((7.5, 7.5), (14.5, 14.5)),
    //     rect!((10.5, 11.5), (11.5, 12.5)),
    //     rect!((4.5, 4.5), (5.5, 6.5)),
    //     rect!((4.5, 9.5), (5.5, 11.5)),
    //     rect!((13.5, 0.5), (14.5, 1.5)),
    //     rect!((13.5, 13.5), (16.5, 16.5)),
    //     rect!((2.5, 13.5), (4.5, 16.5)),
    //     rect!((2.5, 2.5), (3.5, 3.5)),
    //     rect!((3.5, 6.5), (4.5, 11.5)),
    //     rect!((0.5, 0.5), (5.5, 5.5)),
    // ];
}

// fn test_tree<B: BoxBounded>(mut tree: RTree<B>, rects: Vec<B>, path: String) {
//     assert_eq!(
//         format!("{:#?}", tree),
//         fs::read_to_string(format!("{}/add/0.txt", path)).unwrap()
//     );
//
//     for (idx, rect) in rects.clone().into_iter().enumerate() {
//         tree.insert(rect);
//         assert_eq!(
//             format!("{:#?}", tree),
//             fs::read_to_string(format!("{}/add/{}.txt", path, idx + 1)).unwrap()
//         );
//     }
//
//     for (idx, rect) in rects.into_iter().enumerate() {
//         tree.remove(&rect.get_mbb());
//         assert_eq!(
//             format!("{:#?}", tree),
//             fs::read_to_string(format!("{}/remove/{}.txt", path, idx + 1)).unwrap()
//         );
//     }
// }
//
// fn build_2d_search_tree() -> RTree<Rect<Point2D<f64>>> {
//     let rects = vec![
//         rect!((0.0, 0.0), (10.0, 10.0)),
//         rect!((12.0, 0.0), (15.0, 15.0)),
//         rect!((7.0, 7.0), (14.0, 14.0)),
//         rect!((10.0, 11.0), (11.0, 12.0)),
//         rect!((4.0, 4.0), (5.0, 6.0)),
//         rect!((4.0, 9.0), (5.0, 11.0)),
//         rect!((13.0, 0.0), (14.0, 1.0)),
//         rect!((13.0, 13.0), (16.0, 16.0)),
//         rect!((2.0, 13.0), (4.0, 16.0)),
//         rect!((2.0, 2.0), (3.0, 3.0)),
//         rect!((10.0, 0.0), (12.0, 5.0)),
//         rect!((7.0, 3.0), (8.0, 6.0)),
//     ];
//
//     RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//         rects,
//     )
// }
//
// fn build_3d_search_tree() -> RTree<Rect<Point3D<f64>>> {
//     let rects = vec![
//         rect!((0.0, 0.0, 0.0), (10.0, 10.0, 10.0)),
//         rect!((12.0, 0.0, 0.0), (15.0, 10.0, 15.0)),
//         rect!((7.0, 0.0, 7.0), (14.0, 10.0, 14.0)),
//         rect!((10.0, 0.0, 11.0), (11.0, 10.0, 12.0)),
//         rect!((4.0, 0.0, 4.0), (5.0, 10.0, 6.0)),
//         rect!((4.0, 0.0, 9.0), (5.0, 10.0, 11.0)),
//         rect!((13.0, 0.0, 0.0), (14.0, 10.0, 1.0)),
//         rect!((13.0, 0.0, 13.0), (16.0, 10.0, 16.0)),
//         rect!((2.0, 0.0, 13.0), (4.0, 10.0, 16.0)),
//         rect!((2.0, 0.0, 2.0), (3.0, 10.0, 3.0)),
//         rect!((10.0, 0.0, 0.0), (12.0, 10.0, 5.0)),
//         rect!((7.0, 0.0, 3.0), (8.0, 10.0, 6.0)),
//     ];
//
//     RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//         rects,
//     )
// }
//
// #[test]
// fn rtree_2d_linear_test() {
//     let rects = vec![
//         rect!((0.5, 0.5), (10.5, 10.5)),
//         rect!((12.5, 0.5), (15.5, 15.5)),
//         rect!((7.5, 7.5), (14.5, 14.5)),
//         rect!((10.5, 11.5), (11.5, 12.5)),
//         rect!((4.5, 4.5), (5.5, 6.5)),
//         rect!((4.5, 9.5), (5.5, 11.5)),
//         rect!((13.5, 0.5), (14.5, 1.5)),
//         rect!((13.5, 13.5), (16.5, 16.5)),
//         rect!((2.5, 13.5), (4.5, 16.5)),
//         rect!((2.5, 2.5), (3.5, 3.5)),
//         rect!((3.5, 6.5), (4.5, 11.5)),
//         rect!((0.5, 0.5), (5.5, 5.5)),
//     ];
//
//     let tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Linear,
//     );
//
//     test_tree(tree, rects, String::from("src/rtree/resources/2d/linear"));
// }
//
// #[test]
// fn rtree_3d_linear_test() {
//     let rects = vec![
//         rect!((0.5, 10.5, 0.5), (10.5, 12.6, 10.5)),
//         rect!((12.5, 8.5, 0.5), (15.5, 10.6, 15.5)),
//         rect!((7.5, 10.5, 7.5), (14.5, 12.6, 14.5)),
//         rect!((10.5, 8.5, 11.5), (11.5, 10.6, 12.5)),
//         rect!((4.5, 10.5, 4.5), (5.5, 12.6, 6.5)),
//         rect!((4.5, 8.5, 9.5), (5.5, 10.6, 11.5)),
//         rect!((13.5, 10.5, 0.5), (14.5, 12.6, 1.5)),
//         rect!((13.5, 8.5, 13.5), (16.5, 10.6, 16.5)),
//         rect!((2.5, 10.5, 13.5), (4.5, 12.6, 16.5)),
//         rect!((2.5, 11.5, 2.5), (3.5, 11.6, 3.5)),
//         rect!((3.5, 10.5, 6.5), (4.5, 12.6, 11.5)),
//         rect!((0.5, 10.5, 0.5), (5.5, 12.6, 5.5)),
//     ];
//
//     let tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Linear,
//     );
//
//     test_tree(tree, rects, String::from("src/rtree/resources/3d/linear"));
// }
//
// #[test]
// fn rtree_2d_quadratic_test() {
//     let rects = vec![
//         rect!((0.5, 0.5), (10.5, 10.5)),
//         rect!((12.5, 0.5), (15.5, 15.5)),
//         rect!((7.5, 7.5), (14.5, 14.5)),
//         rect!((10.5, 11.5), (11.5, 12.5)),
//         rect!((4.5, 4.5), (5.5, 6.5)),
//         rect!((4.5, 9.5), (5.5, 11.5)),
//         rect!((13.5, 0.5), (14.5, 1.5)),
//         rect!((13.5, 13.5), (16.5, 16.5)),
//         rect!((2.5, 13.5), (4.5, 16.5)),
//         rect!((2.5, 2.5), (3.5, 3.5)),
//         rect!((10.5, 0.5), (12.5, 5.5)),
//         rect!((7.5, 3.5), (8.5, 6.5)),
//     ];
//
//     let tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//     );
//
//     test_tree(
//         tree,
//         rects,
//         String::from("src/rtree/resources/2d/quadratic/"),
//     );
// }
//
// #[test]
// fn rtree_3d_quadratic_test() {
//     let rects = vec![
//         rect!((0.0, 0.0, 0.0), (10.0, 10.0, 10.0)),
//         rect!((12.0, 0.0, 0.0), (15.0, 10.0, 15.0)),
//         rect!((7.0, 0.0, 7.0), (14.0, 10.0, 14.0)),
//         rect!((10.0, 0.0, 11.0), (11.0, 10.0, 12.0)),
//         rect!((4.0, 0.0, 4.0), (5.0, 10.0, 6.0)),
//         rect!((4.0, 0.0, 9.0), (5.0, 10.0, 11.0)),
//         rect!((13.0, 0.0, 0.0), (14.0, 10.0, 1.0)),
//         rect!((13.0, 0.0, 13.0), (16.0, 10.0, 16.0)),
//         rect!((2.0, 0.0, 13.0), (4.0, 10.0, 16.0)),
//         rect!((2.0, 0.0, 2.0), (3.0, 10.0, 3.0)),
//         rect!((10.0, 0.0, 0.0), (12.0, 10.0, 5.0)),
//         rect!((7.0, 0.0, 3.0), (8.0, 10.0, 6.0)),
//     ];
//
//     let tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//     );
//
//     test_tree(
//         tree,
//         rects,
//         String::from("src/rtree/resources/3d/quadratic/"),
//     );
// }
//
// #[test]
// fn bulk_load_3_node_2d_test() {
//     let rects = vec![
//         rect!((0.0, 0.0), (10.0, 10.0)),
//         rect!((12.0, 0.0), (15.0, 15.0)),
//         rect!((7.0, 7.0), (14.0, 14.0)),
//     ];
//
//     let rtree = RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//         rects,
//     );
//
//     assert_eq!(
//         format!("{:#?}", rtree),
//         fs::read_to_string("src/rtree/resources/bulk_load/3-node-2d.txt").unwrap()
//     );
// }
//
// #[test]
// fn bulk_load_5_node_2d_test() {
//     let rects = vec![
//         rect!((0.0, 0.0), (10.0, 10.0)),
//         rect!((12.0, 0.0), (15.0, 15.0)),
//         rect!((7.0, 7.0), (14.0, 14.0)),
//         rect!((10.0, 11.0), (11.0, 12.0)),
//         rect!((4.0, 4.0), (5.0, 6.0)),
//     ];
//
//     let rtree = RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//         rects,
//     );
//
//     assert_eq!(
//         format!("{:#?}", rtree),
//         fs::read_to_string("src/rtree/resources/bulk_load/5-node-2d.txt").unwrap()
//     );
// }
//
// #[test]
// fn bulk_load_12_node_2d_test() {
//     let rects = vec![
//         rect!((0.0, 0.0), (10.0, 10.0)),
//         rect!((12.0, 0.0), (15.0, 15.0)),
//         rect!((7.0, 7.0), (14.0, 14.0)),
//         rect!((10.0, 11.0), (11.0, 12.0)),
//         rect!((4.0, 4.0), (5.0, 6.0)),
//         rect!((4.0, 9.0), (5.0, 11.0)),
//         rect!((13.0, 0.0), (14.0, 1.0)),
//         rect!((13.0, 13.0), (16.0, 16.0)),
//         rect!((2.0, 13.0), (4.0, 16.0)),
//         rect!((2.0, 2.0), (3.0, 3.0)),
//         rect!((10.0, 0.0), (12.0, 5.0)),
//         rect!((7.0, 3.0), (8.0, 6.0)),
//     ];
//
//     let rtree = RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//         rects,
//     );
//
//     assert_eq!(
//         format!("{:#?}", rtree),
//         fs::read_to_string("src/rtree/resources/bulk_load/12-node-2d.txt").unwrap()
//     );
// }
//
// #[test]
// fn bulk_load_24_node_3d_test() {
//     let rects = vec![
//         rect!((0.0, 0.0, 7.0), (10.0, 10.0, 15.0)),
//         rect!((12.0, 0.0, 3.0), (15.0, 15.0, 11.0)),
//         rect!((7.0, 7.0, 3.0), (14.0, 14.0, 7.0)),
//         rect!((10.0, 11.0, 4.0), (11.0, 12.0, 9.0)),
//         rect!((4.0, 4.0, 3.0), (5.0, 6.0, 5.0)),
//         rect!((4.0, 9.0, 1.0), (5.0, 11.0, 6.0)),
//         rect!((13.0, 0.0, 19.0), (14.0, 1.0, 20.0)),
//         rect!((13.0, 13.0, 7.0), (16.0, 16.0, 11.0)),
//         rect!((2.0, 13.0, 11.0), (4.0, 16.0, 14.0)),
//         rect!((2.0, 2.0, 10.0), (3.0, 3.0, 22.0)),
//         rect!((10.0, 0.0, 13.0), (12.0, 5.0, 17.0)),
//         rect!((7.0, 3.0, 11.0), (8.0, 6.0, 19.0)),
//         rect!((1.0, 1.0, 0.0), (11.0, 11.0, 4.0)),
//         rect!((13.0, 1.0, 10.0), (16.0, 16.0, 31.0)),
//         rect!((8.0, 8.0, 1.0), (15.0, 15.0, 5.0)),
//         rect!((11.0, 12.0, 10.0), (12.0, 13.0, 12.0)),
//         rect!((5.0, 5.0, 7.0), (6.0, 7.0, 11.0)),
//         rect!((5.0, 10.0, 15.0), (6.0, 12.0, 16.0)),
//         rect!((14.0, 1.0, 14.0), (15.0, 2.0, 15.0)),
//         rect!((14.0, 14.0, 13.0), (17.0, 17.0, 15.0)),
//         rect!((3.0, 14.0, 11.0), (5.0, 17.0, 17.0)),
//         rect!((3.0, 3.0, 9.0), (4.0, 4.0, 14.0)),
//         rect!((11.0, 1.0, 5.0), (13.0, 6.0, 15.0)),
//         rect!((8.0, 4.0, 3.0), (9.0, 7.0, 13.0)),
//     ];
//
//     let rtree = RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//         rects,
//     );
//
//     assert_eq!(
//         format!("{:#?}", rtree),
//         fs::read_to_string("src/rtree/resources/bulk_load/24-node-3d.txt").unwrap()
//     );
// }
//
// #[test]
// fn search_no_results_2d_test() {
//     let tree = build_2d_search_tree();
//     let found = tree.search(&rect!((6.0, 11.0), (7.0, 13.0)));
//     assert!(found.is_none());
// }
//
// #[test]
// fn search_single_result_2d_test() {
//     let tree = build_2d_search_tree();
//     let found = tree.search(&rect!((6.0, 1.0), (9.0, 6.0))).unwrap();
//     assert_eq!(found.len(), 1);
// }
//
// #[test]
// fn search_multiple_results_2d_test() {
//     let tree = build_2d_search_tree();
//     let found = tree.search(&rect!((7.0, 0.0), (14.0, 15.0))).unwrap();
//     assert_eq!(found.len(), 5);
// }
//
// #[test]
// fn search_no_results_3d_test() {
//     let tree = build_3d_search_tree();
//     let found = tree.search(&rect!((0.0, 15.0, 0.0), (20.0, 20.0, 20.0)));
//     assert!(found.is_none());
// }
//
// #[test]
// fn search_single_result_3d_test() {
//     let tree = build_3d_search_tree();
//     let found = tree
//         .search(&rect!((10.0, 0.0, 11.0), (11.0, 10.0, 12.0)))
//         .unwrap();
//     assert_eq!(found.len(), 1);
// }
//
// #[test]
// fn search_multiple_results_3d_test() {
//     let tree = build_3d_search_tree();
//     let found = tree
//         .search(&rect!((0.0, 0.0, 0.0), (20.0, 20.0, 20.0)))
//         .unwrap();
//     assert_eq!(found.len(), 12);
// }
//
// #[test]
// fn insert_no_clones_test() {
//     let mut tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//     );
//     let clone_count = CloneCount::new();
//     let first = rect!((0.0, 0.0), (10.0, 10.0));
//     let second = rect!((12.0, 0.0), (15.0, 15.0));
//
//     tree.insert(CloneTracker::new(first, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(second, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     assert_eq!(tree.len(), 2);
//     assert_eq!(cloned_tree.len(), 1);
// }
//
// #[test]
// fn clone_on_remove_test() {
//     let mut tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//     );
//     let clone_count = CloneCount::new();
//
//     let first = rect!((0.0, 0.0), (10.0, 10.0));
//
//     tree.insert(CloneTracker::new(first.clone(), clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let cloned_tree = tree.clone();
//
//     tree.remove(&first);
//     assert_eq!(clone_count.get(), 1);
//
//     assert_eq!(tree.len(), 0);
//     assert_eq!(cloned_tree.len(), 1);
// }
//
// #[test]
// fn linear_split_no_clones_test() {
//     let mut tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Linear,
//     );
//     let clone_count = CloneCount::new();
//
//     let first = rect!((0.0, 0.0), (10.0, 10.0));
//     let second = rect!((12.0, 0.0), (15.0, 15.0));
//     let third = rect!((7.0, 7.0), (14.0, 14.0));
//     let fourth = rect!((10.0, 11.0), (11.0, 12.0));
//     let fifth = rect!((4.0, 4.0), (5.0, 6.0));
//
//     tree.insert(CloneTracker::new(first, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let first_cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(second, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let second_cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(third, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let third_cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(fourth, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let fourth_cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(fifth, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     assert_eq!(tree.len(), 5);
//     assert_eq!(first_cloned_tree.len(), 1);
//     assert_eq!(second_cloned_tree.len(), 2);
//     assert_eq!(third_cloned_tree.len(), 3);
//     assert_eq!(fourth_cloned_tree.len(), 4);
// }
//
// #[test]
// fn quadratic_split_no_clones_test() {
//     let mut tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//     );
//     let clone_count = CloneCount::new();
//
//     let first = rect!((0.0, 0.0), (10.0, 10.0));
//     let second = rect!((12.0, 0.0), (15.0, 15.0));
//     let third = rect!((7.0, 7.0), (14.0, 14.0));
//     let fourth = rect!((10.0, 11.0), (11.0, 12.0));
//     let fifth = rect!((4.0, 4.0), (5.0, 6.0));
//
//     tree.insert(CloneTracker::new(first, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let first_cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(second, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let second_cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(third, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let third_cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(fourth, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     let fourth_cloned_tree = tree.clone();
//
//     tree.insert(CloneTracker::new(fifth, clone_count.clone()));
//     assert_eq!(clone_count.get(), 0);
//
//     assert_eq!(tree.len(), 5);
//     assert_eq!(first_cloned_tree.len(), 1);
//     assert_eq!(second_cloned_tree.len(), 2);
//     assert_eq!(third_cloned_tree.len(), 3);
//     assert_eq!(fourth_cloned_tree.len(), 4);
// }
//
// #[test]
// fn clone_on_merge_test() {
//     let mut tree = RTree::new(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Quadratic,
//     );
//     let clone_count = CloneCount::new();
//
//     let first = rect!((0.0, 0.0), (10.0, 10.0));
//     let second = rect!((12.0, 0.0), (15.0, 15.0));
//     let third = rect!((7.0, 7.0), (14.0, 14.0));
//     let fourth = rect!((10.0, 11.0), (11.0, 12.0));
//     let fifth = rect!((4.0, 4.0), (5.0, 6.0));
//
//     tree.insert(CloneTracker::new(first.clone(), clone_count.clone()));
//     tree.insert(CloneTracker::new(second.clone(), clone_count.clone()));
//     tree.insert(CloneTracker::new(third.clone(), clone_count.clone()));
//     tree.insert(CloneTracker::new(fourth.clone(), clone_count.clone()));
//     tree.insert(CloneTracker::new(fifth.clone(), clone_count.clone()));
//
//     assert_eq!(clone_count.get(), 0);
//
//     let first_cloned_tree = tree.clone();
//
//     tree.remove(&first);
//     assert_eq!(clone_count.get(), 1);
//
//     let second_cloned_tree = tree.clone();
//
//     tree.remove(&second);
//     assert_eq!(clone_count.get(), 2);
//
//     let third_cloned_tree = tree.clone();
//
//     tree.remove(&third);
//     assert_eq!(clone_count.get(), 3);
//
//     let fourth_cloned_tree = tree.clone();
//
//     tree.remove(&fourth);
//     assert_eq!(clone_count.get(), 4);
//
//     let fifth_cloned_tree = tree.clone();
//
//     tree.remove(&fifth);
//     assert_eq!(clone_count.get(), 5);
//
//     assert_eq!(tree.len(), 0);
//     assert_eq!(first_cloned_tree.len(), 5);
//     assert_eq!(second_cloned_tree.len(), 4);
//     assert_eq!(third_cloned_tree.len(), 3);
//     assert_eq!(fourth_cloned_tree.len(), 2);
//     assert_eq!(fifth_cloned_tree.len(), 1);
// }
//
// #[test]
// fn bulk_load_no_clone() {
//     let clone_count = CloneCount::new();
//
//     let items = vec![
//         CloneTracker::new(rect!((0.0, 0.0), (10.0, 10.0)), clone_count.clone()),
//         CloneTracker::new(rect!((12.0, 0.0), (15.0, 15.0)), clone_count.clone()),
//         CloneTracker::new(rect!((7.0, 7.0), (14.0, 14.0)), clone_count.clone()),
//         CloneTracker::new(rect!((10.0, 11.0), (11.0, 12.0)), clone_count.clone()),
//         CloneTracker::new(rect!((4.0, 4.0), (5.0, 6.0)), clone_count.clone()),
//         CloneTracker::new(rect!((4.0, 9.0), (5.0, 11.0)), clone_count.clone()),
//         CloneTracker::new(rect!((13.0, 0.0), (14.0, 1.0)), clone_count.clone()),
//         CloneTracker::new(rect!((13.0, 13.0), (16.0, 16.0)), clone_count.clone()),
//         CloneTracker::new(rect!((2.0, 13.0), (4.0, 16.0)), clone_count.clone()),
//         CloneTracker::new(rect!((2.0, 2.0), (3.0, 3.0)), clone_count.clone()),
//         CloneTracker::new(rect!((10.0, 0.0), (12.0, 5.0)), clone_count.clone()),
//         CloneTracker::new(rect!((7.0, 3.0), (8.0, 6.0)), clone_count.clone()),
//     ];
//
//     let rtree = RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Linear,
//         items,
//     );
//
//     assert_eq!(clone_count.get(), 0);
//     assert_eq!(rtree.len(), 12);
// }
//
// #[test]
// fn search_single_no_clone() {
//     let clone_count = CloneCount::new();
//
//     let items = vec![
//         CloneTracker::new(rect!((0.0, 0.0), (10.0, 10.0)), clone_count.clone()),
//         CloneTracker::new(rect!((12.0, 0.0), (15.0, 15.0)), clone_count.clone()),
//         CloneTracker::new(rect!((7.0, 7.0), (14.0, 14.0)), clone_count.clone()),
//         CloneTracker::new(rect!((10.0, 11.0), (11.0, 12.0)), clone_count.clone()),
//         CloneTracker::new(rect!((4.0, 4.0), (5.0, 6.0)), clone_count.clone()),
//         CloneTracker::new(rect!((4.0, 9.0), (5.0, 11.0)), clone_count.clone()),
//         CloneTracker::new(rect!((13.0, 0.0), (14.0, 1.0)), clone_count.clone()),
//         CloneTracker::new(rect!((13.0, 13.0), (16.0, 16.0)), clone_count.clone()),
//         CloneTracker::new(rect!((2.0, 13.0), (4.0, 16.0)), clone_count.clone()),
//         CloneTracker::new(rect!((2.0, 2.0), (3.0, 3.0)), clone_count.clone()),
//         CloneTracker::new(rect!((10.0, 0.0), (12.0, 5.0)), clone_count.clone()),
//         CloneTracker::new(rect!((7.0, 3.0), (8.0, 6.0)), clone_count.clone()),
//     ];
//
//     let rtree = RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Linear,
//         items,
//     );
//
//     let found = rtree.search(&rect!((2.0, 2.0), (4.0, 4.0))).unwrap();
//     assert_eq!(clone_count.get(), 0);
//     assert_eq!(found.len(), 1);
// }
//
// #[test]
// fn search_multiple_no_clone() {
//     let clone_count = CloneCount::new();
//
//     let items = vec![
//         CloneTracker::new(rect!((0.0, 0.0), (10.0, 10.0)), clone_count.clone()),
//         CloneTracker::new(rect!((12.0, 0.0), (15.0, 15.0)), clone_count.clone()),
//         CloneTracker::new(rect!((7.0, 7.0), (14.0, 14.0)), clone_count.clone()),
//         CloneTracker::new(rect!((10.0, 11.0), (11.0, 12.0)), clone_count.clone()),
//         CloneTracker::new(rect!((4.0, 4.0), (5.0, 6.0)), clone_count.clone()),
//         CloneTracker::new(rect!((4.0, 9.0), (5.0, 11.0)), clone_count.clone()),
//         CloneTracker::new(rect!((13.0, 0.0), (14.0, 1.0)), clone_count.clone()),
//         CloneTracker::new(rect!((13.0, 13.0), (16.0, 16.0)), clone_count.clone()),
//         CloneTracker::new(rect!((2.0, 13.0), (4.0, 16.0)), clone_count.clone()),
//         CloneTracker::new(rect!((2.0, 2.0), (3.0, 3.0)), clone_count.clone()),
//         CloneTracker::new(rect!((10.0, 0.0), (12.0, 5.0)), clone_count.clone()),
//         CloneTracker::new(rect!((7.0, 3.0), (8.0, 6.0)), clone_count.clone()),
//     ];
//
//     let rtree = RTree::bulk_load(
//         NonZeroUsize::new(2).unwrap(),
//         NonZeroUsize::new(4).unwrap(),
//         SplitStrategy::Linear,
//         items,
//     );
//
//     let found = rtree.search(&rect!((2.0, 2.0), (10.0, 10.0))).unwrap();
//     assert_eq!(clone_count.get(), 0);
//     assert_eq!(found.len(), 3);
// }
//
// #[test]
// fn clone_tracker_test() {
//     let first = rect!((0.0, 0.0), (10.0, 10.0));
//     let clone_count = CloneCount::new();
//
//     let first_clone_tracker = CloneTracker::new(first.clone(), clone_count.clone());
//
//     assert_eq!(clone_count.get(), 0);
//     let _ = first_clone_tracker.clone();
//     assert_eq!(clone_count.get(), 1);
//     let _ = first_clone_tracker.clone();
//     assert_eq!(clone_count.get(), 2);
// }
//
// #[derive(Debug)]
// struct CloneTracker {
//     mbb: Rect<Point2D<f64>>,
//     clone_count: CloneCount,
// }
//
// impl CloneTracker {
//     fn new(rect: Rect<Point2D<f64>>, clone_count: CloneCount) -> Self {
//         CloneTracker {
//             mbb: rect,
//             clone_count,
//         }
//     }
// }
//
// impl Clone for CloneTracker {
//     fn clone(&self) -> Self {
//         self.clone_count.inc();
//
//         CloneTracker {
//             mbb: self.mbb.clone(),
//             clone_count: self.clone_count.clone(),
//         }
//     }
// }
//
// impl BoxBounded for CloneTracker {
//     type Point = Point2D<f64>;
//
//     fn get_mbb(&self) -> &Rect<Self::Point> {
//         &self.mbb
//     }
//
//     fn get_center(&self) -> Self::Point {
//         self.mbb.get_center()
//     }
//
//     fn measure(&self) -> f64 {
//         self.mbb.measure()
//     }
// }
//
// #[derive(Debug, Clone)]
// struct CloneCount(Arc<Mutex<i32>>);
//
// impl CloneCount {
//     fn new() -> Self {
//         CloneCount(Arc::new(Mutex::new(0)))
//     }
//
//     fn inc(&self) {
//         *self.0.lock().unwrap() += 1;
//     }
//
//     fn get(&self) -> i32 {
//         *self.0.lock().unwrap()
//     }
// }
