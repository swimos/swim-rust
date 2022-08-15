// Copyright 2015-2021 Swim Inc.
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

use std::convert::TryInto;
use std::num::NonZeroUsize;
use swim_algebra::non_zero_usize;

use super::*;

#[test]
fn identity_iteratee() {
    let mut iteratee = identity::<i32>();

    assert_eq!(iteratee.feed(7), Some(7));
    assert_eq!(iteratee.feed(4), Some(4));
    assert_eq!(iteratee.feed(1), Some(1));
    assert_eq!(iteratee.feed(3), Some(3));

    assert!(iteratee.flush().is_none());
}

#[test]
fn never_iteratee() {
    let mut iteratee = never::<i32>();

    assert!(iteratee.feed(7).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(3).is_none());

    assert!(iteratee.flush().is_none());
}

#[test]
fn unfold_iteratee() {
    let mut iteratee = unfold((0, 0), |state, n: i32| {
        let (count, sum) = state;
        *count += 1;
        *sum += n;
        if *count == 4 {
            Some(*sum)
        } else {
            None
        }
    });

    assert!(iteratee.feed(7).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(1).is_none());
    assert_eq!(iteratee.feed(3), Some(15));

    assert!(iteratee.flush().is_none());
}

#[test]
fn unfold_iteratee_with_flush() {
    let mut iteratee = unfold_with_flush(
        (0, 0),
        |state, n: i32| {
            let (count, sum) = state;
            *count += 1;
            *sum += n;
            if *count == 4 {
                Some(*sum)
            } else {
                None
            }
        },
        |state| Some(state.1 + 1),
    );

    assert!(iteratee.feed(7).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(1).is_none());
    assert_eq!(iteratee.feed(3), Some(15));

    assert_eq!(iteratee.flush(), Some(16));
}

#[test]
fn collect_to_vector() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec::<i32>(size);

    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(-1).is_none());
    assert_eq!(iteratee.feed(7), Some(vec![2, -1, 7]));

    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.flush().is_none());
}

#[test]
fn collect_to_vector_with_remainder() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec_with_rem::<i32>(size);

    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(-1).is_none());
    assert_eq!(iteratee.feed(7), Some(vec![2, -1, 7]));

    assert!(iteratee.feed(3).is_none());
    assert_eq!(iteratee.flush(), Some(vec![3]));
}

#[test]
fn collect_all_to_vector() {
    let mut iteratee = collect_all_vec::<i32>();

    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(-1).is_none());
    assert!(iteratee.feed(7).is_none());
    assert!(iteratee.feed(3).is_none());
    assert_eq!(iteratee.flush(), Some(vec![2, -1, 7, 3]));
}

#[test]
fn map_iteratee() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec::<i32>(size).map(|vec| vec.iter().sum());

    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(2).is_none());
    assert_eq!(iteratee.feed(3), Some(6));

    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(5).is_none());
    assert!(iteratee.flush().is_none());
}

#[test]
fn map_iteratee_with_flush() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec_with_rem::<i32>(size).map(|vec| vec.iter().sum());

    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(2).is_none());
    assert_eq!(iteratee.feed(3), Some(6));

    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(5).is_none());
    assert_eq!(iteratee.flush(), Some(9));
}

#[test]
fn comap_iteratee() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec(size).comap(|n: i32| n.to_string());

    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(2).is_none());
    assert_eq!(
        iteratee.feed(3),
        Some(vec!["1".to_owned(), "2".to_owned(), "3".to_owned()])
    );

    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(5).is_none());
    assert!(iteratee.flush().is_none());
}

#[test]
fn comap_iteratee_with_flush() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec_with_rem(size).comap(|n: i32| n.to_string());

    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(2).is_none());
    assert_eq!(
        iteratee.feed(3),
        Some(vec!["1".to_owned(), "2".to_owned(), "3".to_owned()])
    );

    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(5).is_none());
    assert_eq!(iteratee.flush(), Some(vec!["4".to_owned(), "5".to_owned()]));
}

#[test]
fn maybe_comap_iteratee() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec(size).maybe_comap(|n: i32| {
        if n % 2 == 0 {
            Some(n.to_string())
        } else {
            None
        }
    });

    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(5).is_none());
    assert_eq!(
        iteratee.feed(6),
        Some(vec!["2".to_owned(), "4".to_owned(), "6".to_owned()])
    );

    assert!(iteratee.feed(7).is_none());
    assert!(iteratee.feed(8).is_none());
    assert!(iteratee.flush().is_none());
}

#[test]
fn maybe_comap_iteratee_with_flush() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec_with_rem(size).maybe_comap(|n: i32| {
        if n % 2 == 0 {
            Some(n.to_string())
        } else {
            None
        }
    });

    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(5).is_none());
    assert_eq!(
        iteratee.feed(6),
        Some(vec!["2".to_owned(), "4".to_owned(), "6".to_owned()])
    );

    assert!(iteratee.feed(7).is_none());
    assert!(iteratee.feed(8).is_none());
    assert_eq!(iteratee.flush(), Some(vec!["8".to_owned()]));
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

    assert_eq!(iteratee.feed(2), Some(2));
    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(2).is_none());
    assert_eq!(iteratee.feed(5), Some(5));
    assert!(iteratee.feed(4).is_none());
    assert_eq!(iteratee.feed(22), Some(22));

    assert!(iteratee.flush().is_none());
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

    assert!(iteratee.feed(1).is_none());
    assert_eq!(iteratee.feed(2), Some(1));
    assert_eq!(iteratee.feed(5), Some(2));
    assert_eq!(iteratee.feed(-1), Some(5));

    assert_eq!(iteratee.flush(), Some(-1));
}

#[test]
fn filter_iteratee() {
    let mut iteratee = identity::<i32>().filter(|i| i % 2 == 0);
    assert!(iteratee.feed(7).is_none());
    assert_eq!(iteratee.feed(4), Some(4));
    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(3).is_none());
    assert_eq!(iteratee.feed(0), Some(0));

    assert!(iteratee.flush().is_none());
}

#[test]
fn filter_iteratee_with_flush() {
    let size = non_zero_usize!(2);
    let mut iteratee = collect_vec_with_rem::<i32>(size)
        .filter(|v| v.first().map(|i| i % 2 == 0).unwrap_or(false));
    assert!(iteratee.feed(7).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(2).is_none());
    assert_eq!(iteratee.feed(3), Some(vec![2, 3]));

    assert!(iteratee.feed(0).is_none());

    assert_eq!(iteratee.flush(), Some(vec![0]));
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
    assert!(iteratee.feed(7).is_none());
    assert_eq!(iteratee.feed(4), Some("4".to_owned()));
    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(3).is_none());
    assert_eq!(iteratee.feed(0), Some("0".to_owned()));

    assert!(iteratee.flush().is_none());
}

#[test]
fn maybe_map_with_flush() {
    let size = non_zero_usize!(2);
    let mut iteratee = collect_vec_with_rem::<i32>(size).maybe_map(|v| match v.first() {
        Some(i) if i % 2 == 0 => Some(v.iter().map(|j| j.to_string()).collect()),
        _ => None,
    });
    assert!(iteratee.feed(7).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(2).is_none());
    assert_eq!(iteratee.feed(3), Some(vec!["2".to_owned(), "3".to_owned()]));

    assert!(iteratee.feed(0).is_none());

    assert_eq!(iteratee.flush(), Some(vec!["0".to_owned()]));
}

#[test]
fn and_then_iteratees() {
    let size = non_zero_usize!(2);
    let mut iteratee = identity::<i32>()
        .filter(|i| i % 2 == 0)
        .and_then(collect_vec(size));

    assert!(iteratee.feed(0).is_none());
    assert!(iteratee.feed(1).is_none());
    assert_eq!(iteratee.feed(2), Some(vec![0, 2]));
    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.feed(4).is_none());
    assert_eq!(iteratee.feed(10), Some(vec![4, 10]));
    assert!(iteratee.feed(2).is_none());

    assert!(iteratee.flush().is_none());
}

#[test]
fn and_then_iteratees_with_flush() {
    let size = non_zero_usize!(2);
    let mut iteratee = identity::<i32>()
        .filter(|i| i % 2 == 0)
        .and_then(collect_vec_with_rem(size));

    assert!(iteratee.feed(0).is_none());
    assert!(iteratee.feed(1).is_none());
    assert_eq!(iteratee.feed(2), Some(vec![0, 2]));
    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.feed(4).is_none());
    assert_eq!(iteratee.feed(10), Some(vec![4, 10]));
    assert!(iteratee.feed(2).is_none());

    assert_eq!(iteratee.flush(), Some(vec![2]));
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
        .flat_map(collect_vec);

    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(7).is_none());
    assert_eq!(iteratee.feed(8), Some(vec![7, 8]));
    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.feed(9).is_none());
    assert!(iteratee.feed(10).is_none());
    assert_eq!(iteratee.feed(11), Some(vec![9, 10, 11]));

    assert!(iteratee.flush().is_none());
}

#[test]
fn add_flush() {
    let mut iteratee = identity::<i32>().with_flush(42);
    assert_eq!(iteratee.feed(2), Some(2));
    assert_eq!(iteratee.feed(7), Some(7));

    assert_eq!(iteratee.flush(), Some(42));
}

#[test]
fn remove_flush() {
    let size = non_zero_usize!(3);
    let mut iteratee = collect_vec_with_rem::<i32>(size).without_flush();

    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(-1).is_none());
    assert_eq!(iteratee.feed(7), Some(vec![2, -1, 7]));

    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.flush().is_none());
}

#[test]
fn flatten_iteratee() {
    let mut iteratee = identity::<i32>()
        .maybe_map(to_non_zero)
        .map(collect_vec)
        .flatten();

    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(7).is_none());
    assert_eq!(iteratee.feed(8), Some(vec![7, 8]));
    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.feed(9).is_none());
    assert!(iteratee.feed(10).is_none());
    assert_eq!(iteratee.feed(11), Some(vec![9, 10, 11]));

    assert!(iteratee.flush().is_none());
}

#[test]
fn fold_iteratee() {
    let mut iteratee = identity::<i32>()
        .filter(|i| i % 2 == 0)
        .fold(0, |sum, i| *sum += i);

    assert!(iteratee.feed(1).is_none());
    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.feed(4).is_none());

    assert_eq!(iteratee.flush(), Some(6));
}

#[test]
fn fuse_iteratee() {
    let mut iteratee = identity::<i32>().fuse();
    assert_eq!(iteratee.feed(3), Some(3));
    assert!(iteratee.feed(12).is_none());
    assert!(iteratee.feed(-2).is_none());

    assert!(iteratee.flush().is_none());
}

#[test]
fn fuse_iteratee_with_flush() {
    let mut iteratee = identity::<i32>().fold(0, |sum, i| *sum += i).fuse();

    assert!(iteratee.feed(3).is_none());
    assert!(iteratee.feed(12).is_none());
    assert!(iteratee.feed(-2).is_none());

    assert_eq!(iteratee.flush(), Some(13));
}

#[test]
fn transduce_iterator() {
    let size = non_zero_usize!(2);
    let data1 = vec![5, 3, -5, 10, 7];
    let data2 = vec![12, -1];

    let mut iteratee = copy_into_vec_with_rem(size);
    let output1 = iteratee.transduce(data1.iter()).collect::<Vec<_>>();

    assert_eq!(output1, vec![vec![5, 3], vec![-5, 10]]);

    let output2 = iteratee.transduce(data2.iter()).collect::<Vec<_>>();

    assert_eq!(output2, vec![vec![7, 12]]);

    assert_eq!(iteratee.flush(), Some(vec![-1]));
}

#[test]
fn transduce_iterator_consuming_iteratee() {
    let size = non_zero_usize!(2);
    let data = vec![5, 3, -5, 10, 7];
    let iteratee = copy_into_vec_with_rem(size);
    let output = iteratee.transduce_into(data.iter()).collect::<Vec<_>>();

    assert_eq!(output, vec![vec![5, 3], vec![-5, 10], vec![7]]);
}

#[test]
fn fuse_iteratee_on_error() {
    let mut iteratee = identity::<i32>()
        .map(|i| if i > 10 { Err("Too big!") } else { Ok(i) })
        .fuse_on_error();

    assert_eq!(iteratee.feed(3), Some(Ok(3)));
    assert_eq!(iteratee.feed(0), Some(Ok(0)));
    assert_eq!(iteratee.feed(-2), Some(Ok(-2)));
    assert_eq!(iteratee.feed(12), Some(Err("Too big!")));
    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(77).is_none());

    assert!(iteratee.flush().is_none());
}

#[test]
fn fuse_iteratee_on_error_with_flush() {
    let mut iteratee = identity::<i32>()
        .map(|i| if i > 10 { Err("Too big!") } else { Ok(i) })
        .with_flush(Ok(6))
        .fuse_on_error();

    assert_eq!(iteratee.feed(3), Some(Ok(3)));
    assert_eq!(iteratee.feed(0), Some(Ok(0)));
    assert_eq!(iteratee.feed(-2), Some(Ok(-2)));
    assert_eq!(iteratee.feed(12), Some(Err("Too big!")));
    assert!(iteratee.feed(2).is_none());
    assert!(iteratee.feed(4).is_none());
    assert!(iteratee.feed(77).is_none());

    assert!(iteratee.flush().is_none());
}

#[test]
fn iteratee_look_ahead() {
    let mut iteratee = look_ahead::<char>();

    assert!(iteratee.feed('h').is_none());
    assert_eq!(iteratee.feed('e'), Some(('h', Some('e'))));
    assert_eq!(iteratee.feed('l'), Some(('e', Some('l'))));
    assert_eq!(iteratee.feed('l'), Some(('l', Some('l'))));
    assert_eq!(iteratee.feed('o'), Some(('l', Some('o'))));
    assert_eq!(iteratee.flush(), Some(('o', None)));
}

#[test]
fn attach_utf8_offsets() {
    let mut iteratee = utf8_byte_offsets();

    assert_eq!(iteratee.feed('a'), Some((0, 'a')));
    assert_eq!(iteratee.feed('ق'), Some((1, 'ق')));
    assert_eq!(iteratee.feed('b'), Some((3, 'b')));
    assert_eq!(iteratee.feed('🐋'), Some((4, '🐋')));
    assert_eq!(iteratee.feed('c'), Some((8, 'c')));

    assert!(iteratee.flush().is_none());
}

#[test]
fn fallible_composition() {
    let size = non_zero_usize!(2);

    let first = identity::<Result<i32, &str>>();
    let second = collect_vec_with_rem::<i32>(size).map(|v| {
        if v.len() > 1 && v[0] == v[1] {
            Err("Identical".to_owned())
        } else {
            Ok(v)
        }
    });

    let mut iteratee = first.and_then_fallible(second);

    assert!(iteratee.feed(Ok(1)).is_none());
    assert_eq!(iteratee.feed(Err("Boom!")), Some(Err("Boom!".to_owned())));
    assert_eq!(iteratee.feed(Ok(2)), Some(Ok(vec![1, 2])));
    assert!(iteratee.feed(Ok(3)).is_none());
    assert_eq!(iteratee.feed(Ok(3)), Some(Err("Identical".to_owned())));
    assert!(iteratee.feed(Ok(4)).is_none());

    assert_eq!(iteratee.flush(), Some(Ok(vec![4])));
}

#[test]
fn enumerate_input() {
    let inner = identity::<(usize, i32)>()
        .with_flush((0, 0))
        .filter(|(_, n)| *n % 2 == 0);

    let mut iteratee = coenumerate(inner);

    assert_eq!(iteratee.feed(10), Some((0, 10)));
    assert!(iteratee.feed(11).is_none());
    assert_eq!(iteratee.feed(12), Some((2, 12)));
    assert!(iteratee.feed(13).is_none());
    assert_eq!(iteratee.feed(14), Some((4, 14)));
    assert_eq!(iteratee.flush(), Some((0, 0)));
}
