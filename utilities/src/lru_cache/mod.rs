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
use std::hash::{BuildHasher, Hash, Hasher};
use std::ptr::NonNull;
use std::fmt::{Debug, Formatter};

#[cfg(test)]
mod tests;

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
        unsafe {
            (*self.0).fmt(f)
        }
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
}

type NodePtr<K, V> = NonNull<Node<K, V>>;

impl<K, V> Node<K, V> {
    fn is_head(&self) -> bool {
        self.prev.is_none()
    }

    fn is_tail(&self) -> bool {
        self.next.is_none()
    }

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

pub struct LruCache<K, V, S = RandomState> {
    capacity: usize,
    map: HashMap<CacheKey<K>, Box<Node<K, V>>, S>,
    nodes: Option<Nodes<K, V>>,
}

impl<K: Debug, V: Debug, S> Debug for LruCache<K, V, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LruCache[capacity = {}, contents = {:?}]", self.capacity, self.map)
    }
}

unsafe impl<K: Send, V: Send, S: Send> Send for LruCache<K, V, S> {}
impl<K: Unpin, V: Unpin, S: Unpin> Unpin for LruCache<K, V, S> {}

impl<K: Hash + Eq, V> LruCache<K, V, RandomState> {
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
    fn new(node: &mut Box<Node<K, V>>) -> Self {
        Nodes {
            head: node.as_mut().into(),
            tail: node.as_mut().into(),
        }
    }

    fn move_to_head(&mut self, node: &mut Box<Node<K, V>>) {
        if !node.is_head() {
            //The node is not the head and so can't be the last node in the list.
            self.remove_internal(node);
            self.attach_head(node);
        }
    }

    fn remove_internal(&mut self, node: &mut Box<Node<K, V>>) -> bool {
        if node.is_tail() {
            //If the node is the tail and has no previous it is already the head so we don't need
            //to do anything.
            if let Some(prev) = &node.prev {
                self.tail = *prev;
            }
        }
        node.cut()
    }

    fn remove(&mut self, node: &mut Box<Node<K, V>>) -> bool {
        let next = if node.is_head() {
            node.next
        } else {
            None
        };
        let final_node = self.remove_internal(node);
        if let Some(new_head) = next {
            self.head = new_head;
        }
        final_node
    }

    fn attach_head(&mut self, node: &mut Box<Node<K, V>>) {
        node.next = Some(self.head);
        let node_ptr = node.as_mut().into();
        unsafe {
            self.head.as_mut().prev = Some(node_ptr);
        }
        self.head = node_ptr;
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> LruCache<K, V, S> {
    pub fn with_hasher(capacity: usize, hasher: S) -> Self {
        LruCache {
            capacity,
            map: HashMap::with_hasher(hasher),
            nodes: None,
        }
    }

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

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.get_mut(key).map(|v| &*v)
    }

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

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let LruCache { map, nodes, .. } = self;
        let (result, remove_nodes) = if let Some(nodes) = nodes {
            match map.remove(key) {
                Some(mut node) => {
                    let remove_nodes = nodes.remove(&mut node);
                    (Some((*node).value), remove_nodes)
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

    pub fn clear(&mut self) {
        self.nodes = None;
        self.map.clear();
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter().map(|(k, v)| (k.borrow(), &v.value))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.map.iter_mut().map(|(k, v)| (k.borrow(), &mut v.value))
    }

    fn print_nodes(&self)
    where
        K: Debug,
        V: Debug,
    {
        if let Some(nodes) = &self.nodes {
            unsafe {
                println!("Forward:");
                let mut p = Some(nodes.head);

                while let Some(n) = p {
                    println!("{:?} -> {:?}", &n.as_ref().key, &n.as_ref().value);
                    p = n.as_ref().next;
                }

                println!("Back:");
                let mut p = Some(nodes.tail);

                while let Some(n) = p {
                    println!("{:?} -> {:?}", &n.as_ref().key, &n.as_ref().value);
                    p = n.as_ref().prev;
                }
            }
        }
    }
}
