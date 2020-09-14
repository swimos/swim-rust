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

use crate::model::parser::TokenStr;

use super::*;

const INPUT1: &str = "abcd";
const INPUT2: &str = "abcd efgh ij";

fn take_without_mark<T: TokenStr, B: TokenBuffer<T>, F>(factory: F)
where
    F: Fn() -> B,
{
    let mut buffer = factory();
    buffer.update(Some((0, 'a')));
    buffer.update(Some((1, 'b')));

    let t1 = buffer.take(0, 1).into();

    assert_eq!(t1, "a".to_owned());

    buffer.update(Some((2, 'c')));
    buffer.update(Some((3, 'd')));

    let t2 = buffer.take_all(2).into();

    assert_eq!(t2, "c".to_owned());

    buffer.update(None);

    let t2 = buffer.take_all(3).into();
    assert_eq!(t2, "d".to_owned());
}

fn take_by_ref_without_mark<T: TokenStr, B: TokenBuffer<T>, F>(factory: F)
where
    F: Fn() -> B,
{
    let mut buffer = factory();
    buffer.update(Some((0, 'a')));
    buffer.update(Some((1, 'b')));

    assert_eq!(buffer.take_ref(0, 1), "a");

    buffer.update(Some((2, 'c')));
    buffer.update(Some((3, 'd')));

    assert_eq!(buffer.take_all_ref(2), "c");

    buffer.update(None);

    assert_eq!(buffer.take_all_ref(3), "d");
}

fn take_with_mark<T: TokenStr, B: TokenBuffer<T>, F>(factory: F)
where
    F: Fn() -> B,
{
    let mut buffer = factory();
    buffer.update(Some((0, 'a')));
    buffer.update(Some((1, 'b')));
    buffer.mark(true);
    buffer.update(Some((2, 'c')));
    buffer.update(Some((3, 'd')));
    buffer.update(Some((4, ' ')));

    let t1 = buffer.take(0, 4).into();

    assert_eq!(t1, "abcd".to_owned());

    buffer.update(Some((5, 'e')));
    buffer.update(Some((6, 'f')));
    buffer.mark(false);
    buffer.update(Some((7, 'g')));
    buffer.update(Some((8, 'h')));
    buffer.update(Some((9, ' ')));

    let t2 = buffer.take_all(6).into();

    assert_eq!(t2, "fgh".to_owned());

    buffer.update(Some((10, 'i')));
    buffer.update(Some((11, 'j')));
    buffer.mark(true);
    buffer.update(None);

    let t3 = buffer.take_all(10).into();

    assert_eq!(t3, "ij".to_owned());
}

fn take_by_ref_with_mark<T: TokenStr, B: TokenBuffer<T>, F>(factory: F)
where
    F: Fn() -> B,
{
    let mut buffer = factory();
    buffer.update(Some((0, 'a')));
    buffer.update(Some((1, 'b')));
    buffer.mark(true);
    buffer.update(Some((2, 'c')));
    buffer.update(Some((3, 'd')));
    buffer.update(Some((4, ' ')));

    assert_eq!(buffer.take_ref(0, 4), "abcd");

    buffer.update(Some((5, 'e')));
    buffer.update(Some((6, 'f')));
    buffer.mark(false);
    buffer.update(Some((7, 'g')));
    buffer.update(Some((8, 'h')));
    buffer.update(Some((9, ' ')));

    assert_eq!(buffer.take_all_ref(6), "fgh");

    buffer.update(Some((10, 'i')));
    buffer.update(Some((11, 'j')));
    buffer.mark(true);
    buffer.update(None);

    assert_eq!(buffer.take_all_ref(10), "ij");
}

#[test]
fn take_without_mark_in_mem() {
    take_without_mark(|| InMemoryInput::new(INPUT1))
}

#[test]
fn take_without_mark_buffered() {
    take_without_mark(TokenAccumulator::new)
}

#[test]
fn take_by_ref_without_mark_in_mem() {
    take_by_ref_without_mark(|| InMemoryInput::new(INPUT1))
}

#[test]
fn take_by_ref_without_mark_buffered() {
    take_by_ref_without_mark(TokenAccumulator::new)
}

#[test]
fn take_with_mark_in_mem() {
    take_with_mark(|| InMemoryInput::new(INPUT2))
}

#[test]
fn take_with_mark_buffered() {
    take_with_mark(TokenAccumulator::new)
}

#[test]
fn take_by_ref_with_mark_in_mem() {
    take_by_ref_with_mark(|| InMemoryInput::new(INPUT2))
}

#[test]
fn take_by_ref_with_mark_buffered() {
    take_by_ref_with_mark(TokenAccumulator::new)
}
