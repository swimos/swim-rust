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

use crate::lru_cache::LruCache;

use crate::ZeroUsize;

#[test]
fn error_on_zero_capacity() {
    let maybe_cache: Result<LruCache<(), ()>, ZeroUsize> = LruCache::with_capacity(0);
    if let Err(e) = maybe_cache {
        assert_eq!(e.to_string(), "Zero Usize")
    } else {
        panic!("Expected error!")
    }
}

#[test]
fn len_on_empty() {
    let cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert_eq!(cache.len(), 0);
}

#[test]
fn peek_lru_when_empty() {
    let cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.peek_lru().is_none());
}

#[test]
fn peek_lru_mut_when_empty() {
    let mut cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.peek_lru_mut().is_none());
}

#[test]
fn peek_mru_when_empty() {
    let cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.peek_mru().is_none());
}

#[test]
fn peek_mru_mut_when_empty() {
    let mut cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.peek_mru_mut().is_none());
}

#[test]
fn iterate_forward_when_empty() {
    let cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.iter().next().is_none());
}

#[test]
fn iterate_backwards_when_empty() {
    let cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.reverse_iter().next().is_none());
}

#[test]
fn iterate_mut_forward_when_empty() {
    let mut cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.iter_mut().next().is_none());
}

#[test]
fn iterate_mut_backwards_when_empty() {
    let mut cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.reverse_iter_mut().next().is_none());
}

#[test]
fn pop_lru_when_empty() {
    let mut cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(cache.pop_lru().is_none());
}

#[test]
fn contains_when_empty() {
    let cache: LruCache<(), ()> = LruCache::with_capacity(3).unwrap();
    assert!(!cache.contains(&()));
}

#[test]
fn capacity() {
    for i in 1..6 {
        let cache: LruCache<i32, i32> = LruCache::with_capacity(i).unwrap();
        assert_eq!(cache.capacity(), i);
    }
}

#[test]
fn is_empty() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    assert!(cache.is_empty());
    cache.insert(0, "0".to_string());
    assert!(!cache.is_empty());
    cache.clear();
    assert!(cache.is_empty());
}

#[test]
fn get_when_empty() {
    let mut cache: LruCache<i32, String> = LruCache::with_capacity(3).unwrap();
    assert!(cache.get(&0).is_none());
}

#[test]
fn replace_existing() {
    let v1 = "hello".to_string();
    let v2 = "world".to_string();

    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(2, v1.clone());
    assert_eq!(cache.len(), 1);
    let evicted = cache.insert(2, v2.clone());
    assert_eq!(evicted, Some((2, v1)));
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(&2), Some(&v2));
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
    exceed_capacity_cap_n(1);
}

#[test]
fn exceed_capacity_twice_cap1() {
    let mut cache = LruCache::with_capacity(1).unwrap();
    let evicted = cache.insert(0, "0".to_string());
    assert!(evicted.is_none());
    assert_eq!(cache.len(), 1);

    let evicted = cache.insert(1, "1".to_string());
    assert_eq!(evicted, Some((0, "0".to_string())));
    assert_eq!(cache.len(), 1);

    let evicted = cache.insert(2, "2".to_string());
    assert_eq!(evicted, Some((1, "1".to_string())));
    assert_eq!(cache.len(), 1);
}

#[test]
fn reorder_last_used_cap1() {
    let mut cache = LruCache::with_capacity(1).unwrap();
    let evicted = cache.insert(0, "0".to_string());
    assert!(evicted.is_none());
    assert!(cache.get(&0).is_some());
    let evicted = cache.insert(1, "1".to_string());
    assert_eq!(evicted, Some((0, "0".to_string())));
    assert_eq!(cache.len(), 1);
}

#[test]
fn remove_cap1() {
    let mut cache = LruCache::with_capacity(1).unwrap();
    let evicted = cache.insert(0, "0".to_string());
    assert!(evicted.is_none());
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.remove(&0), Some("0".to_string()));

    assert!(!cache.contains(&0));
    assert!(cache.is_empty());
}

#[test]
fn clear_when_empty() {
    let mut cache: LruCache<i32, i32> = LruCache::with_capacity(3).unwrap();
    assert!(cache.is_empty());
    cache.clear();
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
    let mut cache = LruCache::with_capacity(2).unwrap();
    for i in 0..2 {
        cache.insert(i, i.to_string());
    }
    assert!(cache.get(&0).is_some());
    let evicted = cache.insert(2, "2".to_string());
    assert_eq!(evicted, Some((1, "1".to_string())));
    assert_eq!(cache.len(), 2);

    let evicted = cache.insert(3, "3".to_string());
    assert_eq!(evicted, Some((0, "0".to_string())));
    assert_eq!(cache.len(), 2);
}

#[test]
fn reorder_last_used_twice_cap2() {
    let mut cache = LruCache::with_capacity(2).unwrap();
    for i in 0..2 {
        cache.insert(i, i.to_string());
    }
    assert!(cache.get(&0).is_some());
    let evicted = cache.insert(2, "2".to_string());
    assert_eq!(evicted, Some((1, "1".to_string())));

    assert!(cache.get(&0).is_some());
    let evicted = cache.insert(3, "3".to_string());
    assert_eq!(evicted, Some((2, "2".to_string())));
    assert_eq!(cache.len(), 2);
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
    let mut cache = LruCache::with_capacity(n).unwrap();
    let evicted = cache.insert(2, "hello".to_string());
    assert!(evicted.is_none());
    assert_eq!(cache.len(), 1);
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
    let mut cache = LruCache::with_capacity(n).unwrap();
    for i in 0..n {
        let evicted = cache.insert(i, i.to_string());
        assert!(evicted.is_none());
    }
    assert_eq!(cache.len(), n);
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

    let mut cache = LruCache::with_capacity(n).unwrap();
    cache.insert(2, s.clone());

    let value = cache.get(&2);
    assert_eq!(value, Some(&s));
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

fn get_after_insert_not_contained_cap_n(n: usize) {
    let s = "hello".to_string();

    let mut cache = LruCache::with_capacity(n).unwrap();
    cache.insert(2, s.clone());

    assert!(cache.get(&-1).is_none());
}

#[test]
fn get_after_insert_not_contained_cap1() {
    get_after_insert_not_contained_cap_n(1);
}

#[test]
fn get_after_insert_not_contained_cap2() {
    get_after_insert_not_contained_cap_n(2);
}

#[test]
fn get_after_insert_not_contained_cap3() {
    get_after_insert_not_contained_cap_n(3);
}

#[test]
fn get_after_insert_not_contained_cap4() {
    get_after_insert_not_contained_cap_n(4);
}

#[test]
fn get_after_insert_not_contained_cap5() {
    get_after_insert_not_contained_cap_n(5);
}

fn exceed_capacity_cap_n(n: usize) {
    let mut cache = LruCache::with_capacity(n).unwrap();
    for i in 0..n {
        cache.insert(i, i.to_string());
    }
    assert_eq!(cache.len(), n);
    let evicted = cache.insert(n, n.to_string());
    assert_eq!(evicted, Some((0, "0".to_string())));
    assert_eq!(cache.len(), n);
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
    let mut cache = LruCache::with_capacity(n).unwrap();
    for i in 0..n {
        cache.insert(i, i.to_string());
    }
    assert_eq!(cache.len(), n);

    let evicted = cache.insert(n, n.to_string());
    assert_eq!(evicted, Some((0, "0".to_string())));
    assert_eq!(cache.len(), n);

    let evicted = cache.insert(n + 1, (n + 1).to_string());
    assert_eq!(evicted, Some((1, "1".to_string())));
    assert_eq!(cache.len(), n);
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
    let mut cache = LruCache::with_capacity(n).unwrap();
    for i in 0..n {
        cache.insert(i, i.to_string());
    }
    assert!(cache.get(&0).is_some());
    let evicted = cache.insert(n, n.to_string());
    assert_eq!(evicted, Some((1, "1".to_string())));
    assert_eq!(cache.len(), n);

    let evicted = cache.insert(n + 1, (n + 1).to_string());
    assert_eq!(evicted, Some((2, "2".to_string())));
    assert_eq!(cache.len(), n);
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
    let mut cache = LruCache::with_capacity(n).unwrap();
    for i in 0..n {
        cache.insert(i, i.to_string());
    }
    assert!(cache.get(&0).is_some());
    assert!(cache.get(&1).is_some());
    let evicted = cache.insert(n, n.to_string());
    assert_eq!(evicted, Some((2, "2".to_string())));
    assert_eq!(cache.len(), n);
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
    let mut cache = LruCache::with_capacity(n).unwrap();
    for i in 0..n {
        let evicted = cache.insert(i, i.to_string());
        assert!(evicted.is_none());
    }
    assert_eq!(cache.len(), n);
    assert_eq!(cache.remove(&index), Some(index.to_string()));

    assert_eq!(cache.len(), n - 1);
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

fn remove_non_existent_cap_n(n: usize) {
    let mut cache = LruCache::with_capacity(n).unwrap();
    for i in 0..n {
        let evicted = cache.insert(i, i.to_string());
        assert!(evicted.is_none());
    }
    assert_eq!(cache.len(), n);
    assert!(cache.remove(&1000).is_none());

    assert_eq!(cache.len(), n);
    for i in 0..n {
        assert!(cache.contains(&i));
    }
}

#[test]
fn remove_non_existent_cap1() {
    remove_non_existent_cap_n(1);
}

#[test]
fn remove_non_existent_cap2() {
    remove_non_existent_cap_n(2);
}

#[test]
fn remove_non_existent_cap3() {
    remove_non_existent_cap_n(3);
}

#[test]
fn remove_non_existent_cap4() {
    remove_non_existent_cap_n(4);
}

#[test]
fn remove_non_existent_cap5() {
    remove_non_existent_cap_n(5);
}

fn clear_cap_n(n: usize) {
    let mut cache = LruCache::with_capacity(n).unwrap();
    for i in 0..n {
        let evicted = cache.insert(i, i.to_string());
        assert!(evicted.is_none());
    }
    assert_eq!(cache.len(), n);
    cache.clear();
    assert_eq!(cache.len(), 0);
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
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    assert_eq!(cache.peek_lru(), Some((&1, &1)));

    assert_eq!(cache.insert(3, 3), Some((1, 1)));
}

#[test]
fn peek_lru_mut() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let maybe_peeked = cache.peek_lru_mut();
    assert!(maybe_peeked.is_some());
    let (peeked_key, peeked_value) = maybe_peeked.unwrap();
    assert_eq!(*peeked_key, 1);
    assert_eq!(*peeked_value, 1);
    *peeked_value = -1;

    assert_eq!(cache.insert(3, 3), Some((1, -1)));
}

#[test]
fn peek_mru() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    assert_eq!(cache.peek_mru(), Some((&0, &0)));

    assert_eq!(cache.insert(3, 3), Some((1, 1)));
    assert_eq!(cache.insert(4, 4), Some((2, 2)));
    assert_eq!(cache.insert(5, 5), Some((0, 0)));
}

#[test]
fn peek_mru_mut() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let maybe_peeked = cache.peek_mru_mut();
    assert!(maybe_peeked.is_some());
    let (peeked_key, peeked_value) = maybe_peeked.unwrap();
    assert_eq!(*peeked_key, 0);
    assert_eq!(*peeked_value, 0);
    *peeked_value = -1;

    assert_eq!(cache.insert(3, 3), Some((1, 1)));
    assert_eq!(cache.insert(4, 4), Some((2, 2)));
    assert_eq!(cache.insert(5, 5), Some((0, -1)));
}

#[test]
fn forward_iteration() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let entries = cache.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();

    assert_eq!(entries, vec![(0, 0), (2, 2), (1, 1)]);

    assert_eq!(cache.insert(3, 3), Some((1, 1)));
    assert_eq!(cache.insert(4, 4), Some((2, 2)));
    assert_eq!(cache.insert(5, 5), Some((0, 0)));
}

#[test]
fn reverse_iteration() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let entries = cache
        .reverse_iter()
        .map(|(k, v)| (*k, *v))
        .collect::<Vec<_>>();

    assert_eq!(entries, vec![(1, 1), (2, 2), (0, 0)]);

    assert_eq!(cache.insert(3, 3), Some((1, 1)));
    assert_eq!(cache.insert(4, 4), Some((2, 2)));
    assert_eq!(cache.insert(5, 5), Some((0, 0)));
}

#[test]
fn forward_iteration_mut() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let entries = cache
        .iter_mut()
        .map(|(k, v)| {
            let result = (*k, *v);
            *v += 1;
            result
        })
        .collect::<Vec<_>>();

    assert_eq!(entries, vec![(0, 0), (2, 2), (1, 1)]);

    assert_eq!(cache.insert(3, 3), Some((1, 2)));
    assert_eq!(cache.insert(4, 4), Some((2, 3)));
    assert_eq!(cache.insert(5, 5), Some((0, 1)));
}

#[test]
fn reverse_iteration_mut() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    let entries = cache
        .reverse_iter_mut()
        .map(|(k, v)| {
            let result = (*k, *v);
            *v += 1;
            result
        })
        .collect::<Vec<_>>();

    assert_eq!(entries, vec![(1, 1), (2, 2), (0, 0)]);

    assert_eq!(cache.insert(3, 3), Some((1, 2)));
    assert_eq!(cache.insert(4, 4), Some((2, 3)));
    assert_eq!(cache.insert(5, 5), Some((0, 1)));
}

#[test]
fn pop_lru() {
    let mut cache = LruCache::with_capacity(3).unwrap();
    cache.insert(0, 0);
    cache.insert(1, 1);
    cache.insert(2, 2);
    cache.get(&0);

    assert_eq!(cache.pop_lru(), Some((1, 1)));
    assert_eq!(cache.len(), 2);

    let entries = cache.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();

    assert_eq!(entries, vec![(0, 0), (2, 2)]);
}
