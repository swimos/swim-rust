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
use std::convert::TryInto;
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
    let mut iteratee = collect_vec::<i32>(size).map(|vec| vec.iter().sum());

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
    let mut iteratee = collect_vec_with_rem::<i32>(size).map(|vec| vec.iter().sum());

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
    let mut iteratee = collect_vec(size).comap(|n: i32| n.to_string());

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(
        iteratee.feed(3),
        eq(Some(vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]))
    );

    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(5), none());
    assert_that!(iteratee.flush(), none());
}

#[test]
fn comap_iteratee_with_flush() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec_with_rem(size).comap(|n: i32| n.to_string());

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(
        iteratee.feed(3),
        eq(Some(vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]))
    );

    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(5), none());
    assert_that!(
        iteratee.flush(),
        eq(Some(vec!["4".to_owned(), "5".to_owned()]))
    );
}

#[test]
fn maybe_comap_iteratee() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec(size).maybe_comap(|n: i32| {
        if n % 2 == 0 {
            Some(n.to_string())
        } else {
            None
        }
    });

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(5), none());
    assert_that!(
        iteratee.feed(6),
        eq(Some(vec!["2".to_owned(), "4".to_owned(), "6".to_owned()]))
    );

    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(8), none());
    assert_that!(iteratee.flush(), none());
}

#[test]
fn maybe_comap_iteratee_with_flush() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec_with_rem(size).maybe_comap(|n: i32| {
        if n % 2 == 0 {
            Some(n.to_string())
        } else {
            None
        }
    });

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(5), none());
    assert_that!(
        iteratee.feed(6),
        eq(Some(vec!["2".to_owned(), "4".to_owned(), "6".to_owned()]))
    );

    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(8), none());
    assert_that!(iteratee.flush(), eq(Some(vec!["8".to_owned()])));
}

#[test]
fn scan_iteratee() {
    let mut iteratee = identity::<i32>().scan(0, |max, i| {
        if i > *max {
            *max = i;
            Some(i)
        } else {
            None
        }
    });

    assert_that!(iteratee.feed(2), eq(Some(2)));
    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(5), eq(Some(5)));
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(22), eq(Some(22)));

    assert_that!(iteratee.flush(), none());
}

#[test]
fn scan_iteratee_with_flush() {
    let mut iteratee = identity::<i32>().scan_with_flush(
        None,
        |prev, i| match *prev {
            Some(p) => {
                *prev = Some(i);
                Some(p)
            }
            _ => {
                *prev = Some(i);
                None
            }
        },
        |prev| prev,
    );

    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), eq(Some(1)));
    assert_that!(iteratee.feed(5), eq(Some(2)));
    assert_that!(iteratee.feed(-1), eq(Some(5)));

    assert_that!(iteratee.flush(), eq(Some(-1)));
}

#[test]
fn filter_iteratee() {
    let mut iteratee = identity::<i32>().filter(|i| i % 2 == 0);
    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(4), eq(Some(4)));
    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.feed(0), eq(Some(0)));

    assert_that!(iteratee.flush(), none());
}

#[test]
fn filter_iteratee_with_flush() {
    let size = NonZeroUsize::new(2).unwrap();
    let mut iteratee =
        collect_vec_with_rem::<i32>(size).filter(|v| v.get(0).map(|i| i % 2 == 0).unwrap_or(false));
    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(3), eq(Some(vec![2, 3])));

    assert_that!(iteratee.feed(0), none());

    assert_that!(iteratee.flush(), eq(Some(vec![0])));
}

#[test]
fn maybe_map_iteratee() {
    let mut iteratee = identity::<i32>().maybe_map(|i| {
        if i % 2 == 0 {
            Some(i.to_string())
        } else {
            None
        }
    });
    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(4), eq(Some("4".to_owned())));
    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.feed(0), eq(Some("0".to_owned())));

    assert_that!(iteratee.flush(), none());
}

#[test]
fn maybe_map_with_flush() {
    let size = NonZeroUsize::new(2).unwrap();
    let mut iteratee = collect_vec_with_rem::<i32>(size).maybe_map(|v| match v.get(0) {
        Some(i) if i % 2 == 0 => Some(v.iter().map(|j| j.to_string()).collect()),
        _ => None,
    });
    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(2), none());
    assert_that!(
        iteratee.feed(3),
        eq(Some(vec!["2".to_owned(), "3".to_owned()]))
    );

    assert_that!(iteratee.feed(0), none());

    assert_that!(iteratee.flush(), eq(Some(vec!["0".to_owned()])));
}

#[test]
fn and_then_iteratees() {
    let size = NonZeroUsize::new(2).unwrap();
    let mut iteratee = identity::<i32>()
        .filter(|i| i % 2 == 0)
        .and_then(collect_vec(size));

    assert_that!(iteratee.feed(0), none());
    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), eq(Some(vec![0, 2])));
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(10), eq(Some(vec![4, 10])));
    assert_that!(iteratee.feed(2), none());

    assert_that!(iteratee.flush(), none());
}

#[test]
fn and_then_iteratees_with_flush() {
    let size = NonZeroUsize::new(2).unwrap();
    let mut iteratee = identity::<i32>()
        .filter(|i| i % 2 == 0)
        .and_then(collect_vec_with_rem(size));

    assert_that!(iteratee.feed(0), none());
    assert_that!(iteratee.feed(1), none());
    assert_that!(iteratee.feed(2), eq(Some(vec![0, 2])));
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.feed(4), none());
    assert_that!(iteratee.feed(10), eq(Some(vec![4, 10])));
    assert_that!(iteratee.feed(2), none());

    assert_that!(iteratee.flush(), eq(Some(vec![2])));
}

fn to_non_zero(n: i32) -> Option<NonZeroUsize> {
    match n.try_into() {
        Ok(i) => NonZeroUsize::new(i),
        Err(_) => None,
    }
}

#[test]
fn flat_map_iteratee() {
    let mut iteratee = identity::<i32>()
        .maybe_map(to_non_zero)
        .flat_map(|i| collect_vec(i));

    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(8), eq(Some(vec![7, 8])));
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.feed(9), none());
    assert_that!(iteratee.feed(10), none());
    assert_that!(iteratee.feed(11), eq(Some(vec![9, 10, 11])));

    assert_that!(iteratee.flush(), none());
}

#[test]
fn add_flush() {
    let mut iteratee = identity::<i32>().with_flush(42);
    assert_that!(iteratee.feed(2), eq(Some(2)));
    assert_that!(iteratee.feed(7), eq(Some(7)));

    assert_that!(iteratee.flush(), eq(Some(42)));
}

#[test]
fn remove_flush() {
    let size = NonZeroUsize::new(3).unwrap();
    let mut iteratee = collect_vec_with_rem::<i32>(size).without_flush();

    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(-1), none());
    assert_that!(iteratee.feed(7), eq(Some(vec![2, -1, 7])));

    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.flush(), none());
}

#[test]
fn flatten_iteratee() {
    let mut iteratee = identity::<i32>()
        .maybe_map(to_non_zero)
        .map(|n| collect_vec(n))
        .flatten();

    assert_that!(iteratee.feed(2), none());
    assert_that!(iteratee.feed(7), none());
    assert_that!(iteratee.feed(8), eq(Some(vec![7, 8])));
    assert_that!(iteratee.feed(3), none());
    assert_that!(iteratee.feed(9), none());
    assert_that!(iteratee.feed(10), none());
    assert_that!(iteratee.feed(11), eq(Some(vec![9, 10, 11])));

    assert_that!(iteratee.flush(), none());
}
