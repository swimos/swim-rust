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
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use std::num::NonZeroUsize;

#[test]
fn identity_iteratee() {
    let mut iteratee = identity::<i32>();

    assert_that!(iteratee.feed(7), eq(Some(7)));
    assert_that!(iteratee.feed(4), eq(Some(4)));
    assert_that!(iteratee.feed(1), eq(Some(1)));
    assert_that!(iteratee.feed(3), eq(Some(3)));

    assert_that!(iteratee.flush(), none());
}

#[test]
fn never_iteratee() {
    let mut iteratee = never::<i32>();

    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(3), none());

    assert_that!(iteratee.flush(), none());
}

#[test]
fn unfold_iteratee() {
    let mut iteratee = unfold((0, 0), |state, n: i32| {
        let (count, sum) = state;
        *count = *count + 1;
        *sum = *sum + n;
        if *count == 4 {
            Some(*sum)
        } else {
            None
        }
    });

    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(3), eq(Some(15)));

    assert_that!(iteratee.flush(), none());
}

#[test]
fn unfold_iteratee_with_flush() {
    let mut iteratee = unfold_with_flush(
        (0, 0),
        |state, n: i32| {
            let (count, sum) = state;
            *count = *count + 1;
            *sum = *sum + n;
            if *count == 4 {
                Some(*sum)
            } else {
                None
            }
        },
        |state| Some(state.1 + 1),
    );

    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(3), eq(Some(15)));

    assert_that!(iteratee.flush(), eq(Some(16)));
}

#[test]
fn collect_to_vector() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec::<i32>(size);

    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(-1), none());
    assert_that!(iteratee.feed(7), eq(Some(vec![2, -1, 7])));

    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.flush(), none());
}

#[test]
fn collect_to_vector_with_remainder() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec_with_rem::<i32>(size);

    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(-1), none());
    assert_that!(iteratee.feed(7), eq(Some(vec![2, -1, 7])));

    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.flush(), eq(Some(vec![3])));
}

#[test]
fn collect_all_to_vector() {
    let mut iteratee = collect_all_vec::<i32>();

    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(-1), none());
    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.flush(), eq(Some(vec![2, -1, 7, 3])));
}

#[test]
fn map_iteratee() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec::<i32>(size)
        .map(|vec| { vec.iter().sum() });

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(3), eq(Some(6)));

    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(5), none());
    assert_that!(iteratee.flush(), none());
}

#[test]
fn map_iteratee_with_flush() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec_with_rem::<i32>(size)
        .map(|vec| { vec.iter().sum() });

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(3), eq(Some(6)));

    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(5), none());
    assert_that!(iteratee.flush(), eq(Some(9)));
}

#[test]
fn comap_iteratee() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec(size)
        .comap(|n: i32| { n.to_string() });

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(3), eq(Some(vec!["1".to_owned(), "2".to_owned(), "3".to_owned()])));

    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(5), none());
    assert_that!(iteratee.flush(), none());
}

#[test]
fn comap_iteratee_with_flush() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec_with_rem(size)
        .comap(|n: i32| { n.to_string() });

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(3), eq(Some(vec!["1".to_owned(), "2".to_owned(), "3".to_owned()])));

    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(5), none());
    assert_that!(iteratee.flush(), eq(Some(vec!["4".to_owned(), "5".to_owned()])));
}
