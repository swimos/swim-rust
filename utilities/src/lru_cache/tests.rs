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

use crate::lru_cache::LruCache;

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

#[test]
#[should_panic]
fn panic_on_zero_capacity() {
    let _: LruCache<(), ()> = LruCache::new(0);
}

#[test]
fn is_empty() {
    let mut cache = LruCache::new(3);
    assert!(cache.is_empty());
    cache.insert(0, "0".to_string());
    assert!(!cache.is_empty());
    cache.clear();
    assert!(cache.is_empty());
}

#[test]
fn get_when_empty() {
    let mut cache: LruCache<i32, String> = LruCache::new(3);
    assert_that!(cache.get(&0), none());
}

#[test]
fn replace_existing() {
    let v1 = "hello".to_string();
    let v2 = "world".to_string();

    let mut cache = LruCache::new(3);
    cache.insert(2, v1.clone());
    assert_that!(cache.len(), eq(1));
    let evicted = cache.insert(2, v2.clone());
    assert_that!(evicted, eq(Some((2, v1))));
    assert_that!(cache.len(), eq(1));
    assert_that!(cache.get(&2), eq(Some(&v2)));
}

#[test]
fn insert_single_cap1() {
    insert_single_cap_n(1);
}

#[test]
fn get_after_insert_cap1() {
    get_after_insert_cap_n(1);
}

#[test]
fn exceed_capacity_cap1() {
    get_after_insert_cap_n(1);
}

#[test]
fn exceed_capacity_twice_cap1() {
    let mut cache = LruCache::new(1);
    let evicted = cache.insert(0, "0".to_string());
    assert_that!(evicted, none());
    assert_that!(cache.len(), eq(1));

    let evicted = cache.insert(1, "1".to_string());
    assert_that!(evicted, eq(Some((0, "0".to_string()))));
    assert_that!(cache.len(), eq(1));

    let evicted = cache.insert(2, "2".to_string());
    assert_that!(evicted, eq(Some((1, "1".to_string()))));
    assert_that!(cache.len(), eq(1));
}

#[test]
fn reorder_last_used_cap1() {
    let mut cache = LruCache::new(1);
    let evicted = cache.insert(0, "0".to_string());
    assert_that!(evicted, none());
    assert_that!(cache.get(&0), some());
    let evicted = cache.insert(1, "1".to_string());
    assert_that!(evicted, eq(Some((0, "0".to_string()))));
    assert_that!(cache.len(), eq(1));
}

#[test]
fn remove_cap1() {
    let mut cache = LruCache::new(1);
    let evicted = cache.insert(0, "0".to_string());
    assert_that!(evicted, none());
    assert_that!(cache.len(), eq(1));
    assert_that!(cache.remove(&0), eq(Some("0".to_string())));

    assert!(!cache.contains(&0));
    assert!(cache.is_empty());
}

#[test]
fn clear_cap1() {
    clear_cap_n(1);
}

#[test]
fn insert_single_cap2() {
    insert_single_cap_n(2);
}

#[test]
fn insert_within_capacity_cap2() {
    insert_within_capacity_cap_n(2);
}

#[test]
fn get_after_insert_cap2() {
    get_after_insert_cap_n(2);
}

#[test]
fn exceed_capacity_cap2() {
    exceed_capacity_cap_n(2);
}

#[test]
fn exceed_capacity_twice_cap2() {
    exceed_capacity_twice_cap_n(2);
}

#[test]
fn reorder_last_used_cap2() {
    let mut cache = LruCache::new(2);
    for i in 0..2 {
        cache.insert(i, i.to_string());
    }
    assert_that!(cache.get(&0), some());
    let evicted = cache.insert(2, "2".to_string());
    assert_that!(evicted, eq(Some((1, "1".to_string()))));
    assert_that!(cache.len(), eq(2));

    let evicted = cache.insert(3, "3".to_string());
    assert_that!(evicted, eq(Some((0, "0".to_string()))));
    assert_that!(cache.len(), eq(2));
}

#[test]
fn reorder_last_used_twice_cap2() {
    let mut cache = LruCache::new(2);
    for i in 0..2 {
        cache.insert(i, i.to_string());
    }
    assert_that!(cache.get(&0), some());
    let evicted = cache.insert(2, "2".to_string());
    assert_that!(evicted, eq(Some((1, "1".to_string()))));

    assert_that!(cache.get(&0), some());
    let evicted = cache.insert(3, "3".to_string());
    assert_that!(evicted, eq(Some((2, "2".to_string()))));
    assert_that!(cache.len(), eq(2));
}

#[test]
fn remove_first_cap2() {
    remove_index_cap_n(2, 0);
}

#[test]
fn remove_last_cap2() {
    remove_index_cap_n(2, 1);
}

#[test]
fn clear_cap2() {
    clear_cap_n(2);
}

fn insert_single_cap_n(n: usize) {
    let mut cache = LruCache::new(n);
    let evicted = cache.insert(2, "hello".to_string());
    assert_that!(evicted, none());
    assert_that!(cache.len(), eq(1));
}

#[test]
fn insert_single_cap3() {
    insert_single_cap_n(3);
}

#[test]
fn insert_single_cap4() {
    insert_single_cap_n(4);
}

#[test]
fn insert_single_cap5() {
    insert_single_cap_n(5);
}

fn insert_within_capacity_cap_n(n: usize) {
    let mut cache = LruCache::new(n);
    for i in 0..n {
        let evicted = cache.insert(i, i.to_string());
        assert_that!(evicted, none());
    }
    assert_that!(cache.len(), eq(n));
}

#[test]
fn insert_within_capacity_cap3() {
    insert_within_capacity_cap_n(3);
}

#[test]
fn insert_within_capacity_cap4() {
    insert_within_capacity_cap_n(4);
}

#[test]
fn insert_within_capacity_cap5() {
    insert_within_capacity_cap_n(5);
}

fn get_after_insert_cap_n(n: usize) {
    let s = "hello".to_string();

    let mut cache = LruCache::new(n);
    cache.insert(2, s.clone());

    let value = cache.get(&2);
    assert_that!(value, eq(Some(&s)));
}

#[test]
fn get_after_insert_cap3() {
    get_after_insert_cap_n(3);
}

#[test]
fn get_after_insert_cap4() {
    get_after_insert_cap_n(4);
}

#[test]
fn get_after_insert_cap5() {
    get_after_insert_cap_n(5);
}

fn exceed_capacity_cap_n(n: usize) {
    let mut cache = LruCache::new(n);
    for i in 0..n {
        cache.insert(i, i.to_string());
    }
    assert_that!(cache.len(), eq(n));
    let evicted = cache.insert(n, n.to_string());
    assert_that!(evicted, eq(Some((0, "0".to_string()))));
    assert_that!(cache.len(), eq(n));
}

#[test]
fn exceed_capacity_cap3() {
    exceed_capacity_cap_n(3);
}

#[test]
fn exceed_capacity_cap4() {
    exceed_capacity_cap_n(4);
}

#[test]
fn exceed_capacity_cap5() {
    exceed_capacity_cap_n(5);
}

fn exceed_capacity_twice_cap_n(n: usize) {
    let mut cache = LruCache::new(n);
    for i in 0..n {
        cache.insert(i, i.to_string());
    }
    assert_that!(cache.len(), eq(n));

    let evicted = cache.insert(n, n.to_string());
    assert_that!(evicted, eq(Some((0, "0".to_string()))));
    assert_that!(cache.len(), eq(n));

    let evicted = cache.insert(n + 1, (n + 1).to_string());
    assert_that!(evicted, eq(Some((1, "1".to_string()))));
    assert_that!(cache.len(), eq(n));
}

#[test]
fn exceed_capacity_twice_cap3() {
    exceed_capacity_twice_cap_n(3);
}

#[test]
fn exceed_capacity_twice_cap4() {
    exceed_capacity_twice_cap_n(4);
}

#[test]
fn exceed_capacity_twice_cap5() {
    exceed_capacity_twice_cap_n(5);
}

fn reorder_last_used_cap_n(n: usize) {
    let mut cache = LruCache::new(n);
    for i in 0..n {
        cache.insert(i, i.to_string());
    }
    assert_that!(cache.get(&0), some());
    let evicted = cache.insert(n, n.to_string());
    assert_that!(evicted, eq(Some((1, "1".to_string()))));
    assert_that!(cache.len(), eq(n));

    let evicted = cache.insert(n + 1, (n + 1).to_string());
    assert_that!(evicted, eq(Some((2, "2".to_string()))));
    assert_that!(cache.len(), eq(n));
}

#[test]
fn reorder_last_used_cap3() {
    reorder_last_used_cap_n(3);
}

#[test]
fn reorder_last_used_cap4() {
    reorder_last_used_cap_n(4);
}

#[test]
fn reorder_last_used_cap5() {
    reorder_last_used_cap_n(5);
}

fn reorder_last_used_twice_cap_n(n: usize) {
    let mut cache = LruCache::new(n);
    for i in 0..n {
        cache.insert(i, i.to_string());
    }
    assert_that!(cache.get(&0), some());
    assert_that!(cache.get(&1), some());
    let evicted = cache.insert(n, n.to_string());
    assert_that!(evicted, eq(Some((2, "2".to_string()))));
    assert_that!(cache.len(), eq(n));
}

#[test]
fn reorder_last_used_twice_cap3() {
    reorder_last_used_twice_cap_n(3);
}

#[test]
fn reorder_last_used_twice_cap4() {
    reorder_last_used_twice_cap_n(4);
}

#[test]
fn reorder_last_used_twice_cap5() {
    reorder_last_used_twice_cap_n(5);
}

fn remove_index_cap_n(n: usize, index: usize) {
    let mut cache = LruCache::new(n);
    for i in 0..n {
        let evicted = cache.insert(i, i.to_string());
        assert_that!(evicted, none());
    }
    assert_that!(cache.len(), eq(n));
    assert_that!(cache.remove(&index), eq(Some(index.to_string())));

    assert_that!(cache.len(), eq(n - 1));
    for i in 0..n {
        if i == index {
            assert!(!cache.contains(&i));
        } else {
            assert!(cache.contains(&i));
        }
    }
}

#[test]
fn remove_first_cap3() {
    remove_index_cap_n(3, 0);
}

#[test]
fn remove_middle_cap3() {
    remove_index_cap_n(3, 1);
}

#[test]
fn remove_last_cap3() {
    remove_index_cap_n(3, 2);
}

#[test]
fn remove_cap4() {
    for i in 0..4 {
        remove_index_cap_n(4, i);
    }
}

#[test]
fn remove_cap5() {
    for i in 0..5 {
        remove_index_cap_n(5, i);
    }
}

fn clear_cap_n(n: usize) {
    let mut cache = LruCache::new(n);
    for i in 0..n {
        let evicted = cache.insert(i, i.to_string());
        assert_that!(evicted, none());
    }
    assert_that!(cache.len(), eq(n));
    cache.clear();
    assert_that!(cache.len(), eq(0));
}

#[test]
fn clear_cap3() {
    clear_cap_n(3);
}

#[test]
fn clear_cap4() {
    clear_cap_n(4);
}

#[test]
fn clear_cap5() {
    clear_cap_n(5);
}

#[test]
fn peek_lru() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    assert_that!(cache.peek_lru(), eq(Some((&1, &1))));

    assert_that!(cache.insert(3, 3), eq(Some((1, 1))));
}

#[test]
fn peek_lru_mut() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let maybe_peeked = cache.peek_lru_mut();
    assert_that!(&maybe_peeked, some());
    let (peeked_key, peeked_value) = maybe_peeked.unwrap();
    assert_that!(*peeked_key, eq(1));
    assert_that!(*peeked_value, eq(1));
    *peeked_value = -1;

    assert_that!(cache.insert(3, 3), eq(Some((1, -1))));
}

#[test]
fn peek_mru() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    assert_that!(cache.peek_mru(), eq(Some((&0, &0))));

    assert_that!(cache.insert(3, 3), eq(Some((1, 1))));
    assert_that!(cache.insert(4, 4), eq(Some((2, 2))));
    assert_that!(cache.insert(5, 5), eq(Some((0, 0))));
}

#[test]
fn peek_mru_mut() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let maybe_peeked = cache.peek_mru_mut();
    assert_that!(&maybe_peeked, some());
    let (peeked_key, peeked_value) = maybe_peeked.unwrap();
    assert_that!(*peeked_key, eq(0));
    assert_that!(*peeked_value, eq(0));
    *peeked_value = -1;

    assert_that!(cache.insert(3, 3), eq(Some((1, 1))));
    assert_that!(cache.insert(4, 4), eq(Some((2, 2))));
    assert_that!(cache.insert(5, 5), eq(Some((0, -1))));
}

#[test]
fn forward_iteration() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let entries = cache.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();

    assert_that!(entries, eq(vec![(0, 0), (2, 2), (1, 1)]));
}

#[test]
fn reverse_iteration() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let entries = cache.reverse_iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();

    assert_that!(entries, eq(vec![(1, 1), (2, 2), (0, 0)]));
}

#[test]
fn forward_iteration_mut() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let entries = cache.iter_mut().map(|(k, v)| {
        let result = (*k, *v);
        *v += 1;
        result
    }).collect::<Vec<_>>();

    assert_that!(entries, eq(vec![(0, 0), (2, 2), (1, 1)]));

    let changed_entries = cache.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();

    assert_that!(changed_entries, eq(vec![(0, 1), (2, 3), (1, 2)]));
}

#[test]
fn reverse_iteration_mut() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let entries = cache.reverse_iter_mut().map(|(k, v)| {
        let result = (*k, *v);
        *v += 1;
        result
    }).collect::<Vec<_>>();

    assert_that!(entries, eq(vec![(1, 1), (2, 2), (0, 0)]));

    let changed_entries = cache.reverse_iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();

    assert_that!(changed_entries, eq(vec![(1, 2), (2, 3), (0, 1)]));
}

#[test]
fn pop_lru() {
    let mut cache = LruCache::new(3);
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    assert_that!(cache.pop_lru(), eq(Some((1, 1))));
    assert_that!(cache.len(), eq(2));

    let entries = cache.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();

    assert_that!(entries, eq(vec![(0, 0), (2, 2)]));
}
