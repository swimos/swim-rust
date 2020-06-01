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

use super::*;

#[test]
#[should_panic]
fn too_large1() {
    let mut mask = FrameMask::new();
    mask.insert(128);
}

#[test]
#[should_panic]
fn too_large2() {
    let mut mask = FrameMask::new();
    mask.insert(200);
}

#[test]
fn insert_and_contains() {
    let mut mask = FrameMask::new();
    for i in 0..MAX_SIZE {
        assert!(!mask.contains(i));
        mask.insert(i);
        assert!(mask.contains(i));
        mask.insert(i);
        assert!(mask.contains(i));
    }
}

#[test]
fn iterate_empty() {
    let mask = FrameMask::new();
    let contents = mask.iter().collect::<Vec<_>>();
    assert!(contents.is_empty());
}

#[test]
fn only_first_iter() {
    let mut mask = FrameMask::new();
    mask.insert(0);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![0]);
}

#[test]
fn only_last_iter() {
    let mut mask = FrameMask::new();
    mask.insert(127);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![127]);
}

#[test]
fn only_internal_iter() {
    let mut mask = FrameMask::new();
    mask.insert(34);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![34]);
}

#[test]
fn initial_block_iter() {
    let mut mask = FrameMask::new();
    mask.insert(0);
    mask.insert(1);
    mask.insert(2);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![0, 1, 2]);
}

#[test]
fn end_block_iter() {
    let mut mask = FrameMask::new();
    mask.insert(125);
    mask.insert(126);
    mask.insert(127);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![125, 126, 127]);
}

#[test]
fn internal_block_iter() {
    let mut mask = FrameMask::new();
    mask.insert(12);
    mask.insert(13);
    mask.insert(14);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![12, 13, 14]);
}

#[test]
fn two_blocks_iter() {
    let mut mask = FrameMask::new();
    mask.insert(12);
    mask.insert(13);
    mask.insert(14);
    mask.insert(16);
    mask.insert(17);

    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![12, 13, 14, 16, 17]);
}

#[test]
fn all_iter() {
    let mut mask = FrameMask::new();
    for i in 0..MAX_SIZE {
        mask.insert(i);
    }
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, (0..MAX_SIZE).into_iter().collect::<Vec<_>>());
}
