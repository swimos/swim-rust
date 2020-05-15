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

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{BuildHasher, Hash, Hasher};
use std::ptr::NonNull;

#[cfg(test)]
mod tests;

// Key into the internal HashMap, pointing to the actual key in the queue.
struct CacheKey<K>(*const K);

impl<K: PartialEq> PartialEq for CacheKey<K> {
    fn eq(&self, other: &Self) -> bool {
        unsafe { *self.0 == *other.0 }
    }
}

impl<K: Eq> Eq for CacheKey<K> {}

impl<K: Hash> Hash for CacheKey<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.0).hash(state) }
    }
}

impl<K> CacheKey<K> {
    fn new(key: &K) -> Self {
        CacheKey(key)
    }
}

impl<K> Borrow<K> for CacheKey<K> {
    fn borrow(&self) -> &K {
        unsafe { &*self.0 }
    }
}

impl<K: Debug> Debug for CacheKey<K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe { (*self.0).fmt(f) }
    }
}

struct Node<K, V> {
    key: K,
    value: V,
    prev: Option<NonNull<Node<K, V>>>,
    next: Option<NonNull<Node<K, V>>>,
}

impl<K, V: Debug> Debug for Node<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V) -> Self {
        Node {
            key,
            value,
            prev: None,
            next: None,
        }
    }

    fn next(&self) -> Option<&Node<K, V>> {
        self.next.as_ref().map(|ptr| unsafe { ptr.as_ref() })
    }

    fn prev(&self) -> Option<&Node<K, V>> {
        self.prev.as_ref().map(|ptr| unsafe { ptr.as_ref() })
    }

    unsafe fn next_mut<'a>(&self) -> Option<&'a mut Node<K, V>> {
        self.next.as_ref().map(|ptr| &mut *ptr.as_ptr())
    }

    unsafe fn prev_mut<'a>(&self) -> Option<&'a mut Node<K, V>> {
        self.prev.as_ref().map(|ptr| &mut *ptr.as_ptr())
    }
}

type NodePtr<K, V> = NonNull<Node<K, V>>;

impl<K, V> Node<K, V> {
    fn is_head(&self) -> bool {
        self.prev.is_none()
    }

    fn is_tail(&self) -> bool {
        self.next.is_none()
    }

    /// Removes the current node from the linked list. Returns true if the list is now empty.
    fn cut(&mut self) -> bool {
        let Node { prev, next, .. } = self;

        let is_final_node = prev.is_none() && next.is_none();

        unsafe {
            if let Some(prev_ptr) = prev {
                prev_ptr.as_mut().next = *next;
            }
            if let Some(next_ptr) = next {
                next_ptr.as_mut().prev = *prev;
            }
        }
        *next = None;
        *prev = None;
        is_final_node
    }
}

/// A fixed size LRU (least recently used) cache. If a new element is inserted into the cache, when
/// it is full, the least recently accessed element (both insertions and retrievals are accesses)
/// is evicted from the cache. The contents can be inspected and the stored values modified without
/// altering the order of elements in the internal queue.
///
/// # Examples
/// ```
/// use utilities::lru_cache::LruCache;
///
/// let mut cache: LruCache<i32, String> = LruCache::new(3);
///
/// assert!(cache.insert(1, String::from("first")).is_none());
/// assert!(cache.insert(2, String::from("second")).is_none());
/// assert!(cache.insert(3, String::from("third")).is_none());
///
/// assert_eq!(cache.get(&1), Some(&String::from("first")));
///
/// let evicted = cache.insert(4, String::from("fourth"));
///
/// assert_eq!(evicted, Some((2, String::from("second"))));
/// ```
///
pub struct LruCache<K, V, S = RandomState> {
    capacity: usize,
    map: HashMap<CacheKey<K>, Box<Node<K, V>>, S>,
    nodes: Option<Nodes<K, V>>,
}

impl<K: Debug, V: Debug, S> Debug for LruCache<K, V, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LruCache[capacity = {}, contents = {:?}]",
            self.capacity, self.map
        )
    }
}

unsafe impl<K: Send, V: Send, S: Send> Send for LruCache<K, V, S> {}
unsafe impl<K: Sync, V: Sync, S: Sync> Sync for LruCache<K, V, S> {}
impl<K: Unpin, V: Unpin, S: Unpin> Unpin for LruCache<K, V, S> {}

impl<K: Hash + Eq, V> LruCache<K, V, RandomState> {
    /// Create a new cache with the specified capacity and the default hasher.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "LRU cache size must be non-zero.");
        LruCache::with_hasher(capacity, Default::default())
    }
}

struct Nodes<K, V> {
    head: NodePtr<K, V>,
    tail: NodePtr<K, V>,
}

impl<K, V> Nodes<K, V> {
    fn new(node: &mut Node<K, V>) -> Self {
        Nodes {
            head: node.into(),
            tail: node.into(),
        }
    }

    // Moves the specified node the the head of the queue.
    fn move_to_head(&mut self, node: &mut Node<K, V>) {
        if !node.is_head() {
            //The node is not the head and so can't be the last node in the list.
            unsafe {
                self.remove_internal(node);
            }
            self.attach_head(node);
        }
    }

    // Removes a node from the queue and replaces the tail if necessary. This is unsafe as it
    // does not reconnect the head.
    unsafe fn remove_internal(&mut self, node: &mut Node<K, V>) -> bool {
        if node.is_tail() {
            //If the node is the tail and has no previous it is already the head so we don't need
            //to do anything.
            if let Some(prev) = &node.prev {
                self.tail = *prev;
            }
        }
        node.cut()
    }

    // Remove an item from the queue.
    fn remove(&mut self, node: &mut Node<K, V>) -> bool {
        let next = if node.is_head() { node.next } else { None };
        let final_node = unsafe { self.remove_internal(node) };
        if let Some(new_head) = next {
            self.head = new_head;
        }
        final_node
    }

    // Insert the specified node as the head of the queue.
    fn attach_head(&mut self, node: &mut Node<K, V>) {
        assert!(node.prev.is_none());
        node.next = Some(self.head);
        let node_ptr = node.into();
        self.head_mut().prev = Some(node_ptr);
        self.head = node_ptr;
    }

    fn head(&self) -> &Node<K, V> {
        unsafe { self.head.as_ref() }
    }

    fn head_mut(&mut self) -> &mut Node<K, V> {
        unsafe { self.head.as_mut() }
    }

    fn tail(&self) -> &Node<K, V> {
        unsafe { self.tail.as_ref() }
    }

    fn tail_mut(&mut self) -> &mut Node<K, V> {
        unsafe { self.tail.as_mut() }
    }
}

impl<K, V, S> LruCache<K, V, S> {
    /// The current occupancy of the cache.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// The maximum capacity of the cache.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn clear(&mut self) {
        self.nodes = None;
        self.map.clear();
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> LruCache<K, V, S> {
    /// Creates a new cache with a custom hasher.
    pub fn with_hasher(capacity: usize, hasher: S) -> Self {
        LruCache {
            capacity,
            map: HashMap::with_hasher(hasher),
            nodes: None,
        }
    }

    /// Insert an element into the cache. If an element already exists with the specified key, the
    /// entry is replaced, no eviction occurs and the old entry is returned. If there is spare
    /// capacity in the cache, a new entry is created and nothing returned. If the cache is full,
    /// the least recently accessed entry is evicted and returned and a new entry inserted. In any
    /// case, the entry associated with [`key`] is moved to the head of the queue and becomes the
    /// most recently used entry.
    pub fn insert(&mut self, key: K, value: V) -> Option<(K, V)> {
        let LruCache {
            capacity,
            map,
            nodes,
        } = self;

        if let Some(nodes) = nodes {
            if let Some(node) = map.get_mut(&key) {
                let old = std::mem::replace(&mut node.value, value);
                nodes.move_to_head(node);
                Some((key, old))
            } else if map.len() < *capacity {
                let mut new_node = Box::new(Node::new(key, value));
                nodes.attach_head(&mut new_node);
                map.insert(CacheKey::new(&new_node.key), new_node);
                None
            } else {
                let mut evicted = map.remove(unsafe { &nodes.tail.as_ref().key }).unwrap();

                let old_key = std::mem::replace(&mut evicted.key, key);
                let old_value = std::mem::replace(&mut evicted.value, value);

                nodes.move_to_head(&mut evicted);
                map.insert(CacheKey::new(&evicted.key), evicted);
                Some((old_key, old_value))
            }
        } else {
            let mut new_node = Box::new(Node::new(key, value));
            *nodes = Some(Nodes::new(&mut new_node));
            map.insert(CacheKey::new(&new_node.key), new_node);
            None
        }
    }

    /// Get a reference to the value associated with a given key, if it exists. This moves that
    /// entry to the head of the queue (making it the most recently used).
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.get_mut(key).map(|v| &*v)
    }

    /// Get a mutable reference to the value associated with a given key, if it exists. This moves
    /// that entry to the head of the queue (making it the most recently used).
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let LruCache { map, nodes, .. } = self;

        if let Some(nodes) = nodes {
            map.get_mut(key).map(|node| {
                nodes.move_to_head(node);
                &mut node.value
            })
        } else {
            None
        }
    }

    fn remove_inner<Q>(&mut self, key: &Q) -> Option<(K, V)>
    where
        Q: Hash + Eq,
        CacheKey<K>: Borrow<Q>,
    {
        let LruCache { map, nodes, .. } = self;
        let (result, remove_nodes) = if let Some(nodes) = nodes {
            match map.remove(key) {
                Some(mut node) => {
                    let remove_nodes = nodes.remove(&mut node);
                    (Some(((*node).key, (*node).value)), remove_nodes)
                }
                _ => (None, false),
            }
        } else {
            (None, false)
        };
        if remove_nodes {
            *nodes = None;
        }
        result
    }

    /// Remove an entry from the cache.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.remove_inner(key).map(|(_, v)| v)
    }

    /// Determine whether the cache contains an entry for the specified key.
    pub fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    /// Iterate over the entries of the cache, proceeding from the most recently accessed to the
    /// least recently accessed. This does not affect the order of elements in the queue.
    pub fn iter(&self) -> Iter<'_, K, V> {
        let remaining = self.len();
        let inner = self.nodes.as_ref().map(|nodes| nodes.head());
        Iter {
            inner,
            direction: Direction::Forward,
            remaining,
        }
    }

    /// Iterate over the entries of the cache, proceeding from the least recently accessed to the
    /// most recently accessed. This does not affect the order of elements in the queue.
    pub fn reverse_iter(&self) -> Iter<'_, K, V> {
        let remaining = self.len();
        let inner = self.nodes.as_ref().map(|nodes| nodes.tail());
        Iter {
            inner,
            direction: Direction::Back,
            remaining,
        }
    }

    /// Mutably iterate over the entries of the cache, proceeding from the most recently accessed
    /// to the least recently accessed. This does not affect the order of elements in the queue.
    pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
        let remaining = self.len();
        let inner = self.nodes.as_mut().map(|nodes| nodes.head_mut());
        IterMut {
            inner,
            direction: Direction::Forward,
            remaining,
        }
    }

    /// Mutably iterate over the entries of the cache, proceeding from the least recently accessed
    /// to the most recently accessed. This does not affect the order of elements in the queue.
    pub fn reverse_iter_mut(&mut self) -> IterMut<'_, K, V> {
        let remaining = self.len();
        let inner = self.nodes.as_mut().map(|nodes| nodes.tail_mut());
        IterMut {
            inner,
            direction: Direction::Back,
            remaining,
        }
    }

    /// Peek at the least recently accessed element without affecting its position in the queue.
    pub fn peek_lru(&self) -> Option<(&K, &V)> {
        self.nodes.as_ref().map(|nodes| {
            let tail = nodes.tail();
            (&tail.key, &tail.value)
        })
    }

    /// Peek at the least recently accessed element, mutably, without affecting its position in the
    /// queue.
    pub fn peek_lru_mut(&mut self) -> Option<(&K, &mut V)> {
        self.nodes.as_mut().map(|nodes| {
            let tail = nodes.tail_mut();
            (&tail.key, &mut tail.value)
        })
    }

    /// Peek at the most recently accessed element without affecting its position in the queue.
    pub fn peek_mru(&self) -> Option<(&K, &V)> {
        self.nodes.as_ref().map(|nodes| {
            let head = nodes.head();
            (&head.key, &head.value)
        })
    }

    /// Peek at the most recently accessed element, mutably, without affecting its position in the
    /// queue.
    pub fn peek_mru_mut(&mut self) -> Option<(&K, &mut V)> {
        self.nodes.as_mut().map(|nodes| {
            let head = nodes.head_mut();
            (&head.key, &mut head.value)
        })
    }

    /// Remove the least recently accessed entry from the cache.
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        self.peek_lru()
            .map(|(k, _)| CacheKey::new(k))
            .and_then(|key| self.remove_inner(&key))
    }
}

enum Direction {
    Forward,
    Back,
}

pub struct Iter<'a, K, V> {
    inner: Option<&'a Node<K, V>>,
    direction: Direction,
    remaining: usize,
}

pub struct IterMut<'a, K, V> {
    inner: Option<&'a mut Node<K, V>>,
    direction: Direction,
    remaining: usize,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            Some(node) => {
                let result = (&node.key, &node.value);
                match self.direction {
                    Direction::Forward => {
                        self.inner = node.next();
                    }
                    Direction::Back => {
                        self.inner = node.prev();
                    }
                }
                self.remaining -= 1;
                Some(result)
            }
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.take() {
            Some(node) => {
                let next_inner = unsafe {
                    match self.direction {
                        Direction::Forward => node.next_mut(),
                        Direction::Back => node.prev_mut(),
                    }
                };
                let result = (&node.key, &mut node.value);
                self.inner = next_inner;
                self.remaining -= 1;
                Some(result)
            }
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
