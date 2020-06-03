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
fn too_large_read1() {
    let mut mask = FrameMask::new();
    mask.read(64);
}

#[test]
#[should_panic]
fn too_large_write1() {
    let mut mask = FrameMask::new();
    mask.write(64);
}

#[test]
#[should_panic]
fn too_large_read2() {
    let mut mask = FrameMask::new();
    mask.read(200);
}

#[test]
#[should_panic]
fn too_large_write2() {
    let mut mask = FrameMask::new();
    mask.write(200);
}

#[test]
fn read_and_get() {
    let mut mask = FrameMask::new();
    for i in 0..MAX_SIZE {
        assert!(mask.get(i).is_none());
        mask.read(i);
        assert_eq!(mask.get(i), Some(ReadWrite::Read));
        mask.read(i);
        assert_eq!(mask.get(i), Some(ReadWrite::Read));
    }
}

#[test]
fn write_and_get() {
    let mut mask = FrameMask::new();
    for i in 0..MAX_SIZE {
        assert!(mask.get(i).is_none());
        mask.write(i);
        assert_eq!(mask.get(i), Some(ReadWrite::Write));
        mask.write(i);
        assert_eq!(mask.get(i), Some(ReadWrite::Write));
    }
}

#[test]
fn read_after_write_noop() {
    let mut mask = FrameMask::new();
    for i in 0..MAX_SIZE {
        assert!(mask.get(i).is_none());
        mask.write(i);
        assert_eq!(mask.get(i), Some(ReadWrite::Write));
        mask.read(i);
        assert_eq!(mask.get(i), Some(ReadWrite::Write));
    }
}

#[test]
fn write_after_read_stacks() {
    let mut mask = FrameMask::new();
    for i in 0..MAX_SIZE {
        assert!(mask.get(i).is_none());
        mask.read(i);
        assert_eq!(mask.get(i), Some(ReadWrite::Read));
        mask.write(i);
        assert_eq!(mask.get(i), Some(ReadWrite::ReadWrite));
    }
}

#[test]
fn iterate_empty() {
    let mask = FrameMask::new();
    let contents = mask.iter().collect::<Vec<_>>();
    assert!(contents.is_empty());
}

fn single_item_iter(i: usize) {
    let mut mask = FrameMask::new();
    mask.read(i);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![(i, ReadWrite::Read)]);

    let mut mask = FrameMask::new();
    mask.write(i);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![(i, ReadWrite::Write)]);

    let mut mask = FrameMask::new();
    mask.read(i);
    mask.write(i);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents, vec![(i, ReadWrite::ReadWrite)]);
}

#[test]
fn only_first_iter() {
    single_item_iter(0);
}

#[test]
fn only_last_iter() {
    single_item_iter(MAX_SIZE - 1);
}

#[test]
fn only_internal_iter() {
    single_item_iter(34);
}

#[test]
fn initial_block_iter() {
    let mut mask = FrameMask::new();
    mask.read(0);
    mask.read(1);
    mask.write(1);
    mask.write(2);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(
        contents,
        vec![
            (0, ReadWrite::Read),
            (1, ReadWrite::ReadWrite),
            (2, ReadWrite::Write)
        ]
    );
}

#[test]
fn end_block_iter() {
    let mut mask = FrameMask::new();
    mask.write(MAX_SIZE - 3);
    mask.read(MAX_SIZE - 2);
    mask.write(MAX_SIZE - 2);
    mask.read(MAX_SIZE - 1);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(
        contents,
        vec![
            (MAX_SIZE - 3, ReadWrite::Write),
            (MAX_SIZE - 2, ReadWrite::ReadWrite),
            (MAX_SIZE - 1, ReadWrite::Read)
        ]
    );
}

#[test]
fn internal_block_iter() {
    let mut mask = FrameMask::new();
    mask.write(12);
    mask.write(13);
    mask.write(14);
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(
        contents,
        vec![
            (12, ReadWrite::Write),
            (13, ReadWrite::Write),
            (14, ReadWrite::Write)
        ]
    );
}

#[test]
fn two_blocks_iter() {
    let mut mask = FrameMask::new();
    mask.read(12);
    mask.write(13);
    mask.read(14);
    mask.write(14);
    mask.write(16);
    mask.read(17);

    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(
        contents,
        vec![
            (12, ReadWrite::Read),
            (13, ReadWrite::Write),
            (14, ReadWrite::ReadWrite),
            (16, ReadWrite::Write),
            (17, ReadWrite::Read)
        ]
    );
}

#[test]
fn all_iter() {
    let mut mask = FrameMask::new();
    for i in 0..MAX_SIZE {
        let m = (i % 7) % 3;
        match m {
            0 => {
                mask.read(i);
            }
            1 => {
                mask.write(i);
            }
            _ => {
                mask.read(i);
                mask.write(i);
            }
        }
    }
    let contents = mask.iter().collect::<Vec<_>>();
    assert_eq!(contents.len(), MAX_SIZE);
    let mut prev: Option<usize> = None;
    for (i, rw) in contents.into_iter() {
        match &mut prev {
            Some(p) => {
                assert_eq!(i, *p + 1);
                *p += 1;
            }
            _ => {
                assert_eq!(i, 0);
                prev = Some(0);
            }
        }
        let m = (i % 7) % 3;
        match m {
            0 => {
                assert_eq!(rw, ReadWrite::Read);
            }
            1 => {
                assert_eq!(rw, ReadWrite::Write);
            }
            _ => {
                assert_eq!(rw, ReadWrite::ReadWrite);
            }
        }
    }
}
