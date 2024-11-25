// Copyright 2015-2024 Swim Inc.
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

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap, VecDeque},
    hash::{BuildHasher, Hash},
    ops::Index,
};

use swimos_agent_protocol::MapOperation;
use swimos_form::write::StructuralWritable;

use crate::lanes::map::MapLaneEvent;

/// Common backing store used by both map stores and map lanes.
#[derive(Debug)]
pub struct MapStoreInner<K, V, Q, M = HashMap<K, V>> {
    content: M,
    previous: Option<MapLaneEvent<K, V, M>>,
    queue: Q,
}

/// Both map stores and map lanes maintain a queue of events to be sent to the runtime. This
/// trait captures the operations that are common between both.
pub trait MapEventQueue<K, V>: Default {
    type Output<'a>
    where
        K: 'a,
        V: 'a,
        Self: 'a;

    fn push(&mut self, action: MapOperation<K, ()>);

    fn pop<'a, M>(&mut self, content: &'a M) -> Option<Self::Output<'a>>
    where
        K: 'a,
        M: MapOps<K, V>;
}

impl<K, V, Q: Default, M> MapStoreInner<K, V, Q, M> {
    pub fn new(content: M) -> Self {
        MapStoreInner {
            content,
            previous: Default::default(),
            queue: Default::default(),
        }
    }
}

pub enum TransformEntryResult {
    NoChange,
    Update,
    Remove,
}

impl<K, V, Q, M> MapStoreInner<K, V, Q, M> {
    pub fn init(&mut self, map: M) {
        self.content = map;
    }
}

impl<K, V, Q, M> MapStoreInner<K, V, Q, M>
where
    Q: MapEventQueue<K, V>,
    M: MapOps<K, V>,
{
    pub fn queue(&mut self) -> &mut Q {
        &mut self.queue
    }

    pub fn pop_operation(&mut self) -> Option<Q::Output<'_>> {
        let MapStoreInner { content, queue, .. } = self;
        queue.pop(content)
    }
}

pub trait MapBacking {
    type KeyType;
    type ValueType;
}

/// Operations that a map implementation (e.g. [`HashMap`], [`BTreeMap`]) must support to be
/// used as a backing store for a [`MapStoreInner`] (which underlies all map stores and map lanes).
pub trait MapOps<K, V>: MapBacking<KeyType = K, ValueType = V> {
    /// Attempt to get the value associated with a key.
    ///
    /// # Arguments
    /// * `key` - The key.
    fn get(&self, key: &K) -> Option<&V>;

    /// Insert a new entry into the map. If such an entry already existed, the old value
    /// will be returned.
    ///
    /// # Arguments
    /// * `key` - The key.
    /// * `value` - The new value.
    fn insert(&mut self, key: K, value: V) -> Option<V>;

    /// Remove an entry from the map. If the entry existed, it's previous value will be returned.
    /// # Arguments
    /// * `key` - The key of the entry to remove.
    fn remove(&mut self, key: &K) -> Option<V>;

    /// Clear the map, returning the previous contents.
    fn take(&mut self) -> Self;

    /// Get an iterator over the keys of the map. If [`MapOps::ORDERED_KEYS`] is true, this must
    /// return the keys in their intrinsic order (which must be consistent with the order of their
    /// Recon representation, if the key type implements [`swimos_form::Form`]).
    fn keys<'a>(&'a self) -> impl Iterator<Item = &'a K>
    where
        K: 'a;

    /// If this is true, the keys returned by [`MapOps::keys`] will be in order.
    const ORDERED_KEYS: bool;

    /// Build an instance of the map from an iterator of key-value pairs.
    ///
    /// # Arguments
    /// * `it` - The key value pairs to insert into the map.
    fn from_entries<I>(it: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>;
}

/// Extension to [`MapOps`] that allows entries of the map to be inspected with a borrowed form
/// of the key.
pub trait MapOpsWithEntry<K, V, BK: ?Sized>:
    MapOps<K, V> + for<'a> Index<&'a BK, Output = V>
{
    /// Compute a value based on an entry of the map.
    ///
    /// # Arguments
    /// * `key` - Borrowed form of the key.
    /// * `f` - A function to apply to a borrow of the value associated with the key.
    fn with_item<BV: ?Sized, F, R>(&self, key: &BK, f: F) -> R
    where
        V: Borrow<BV>,
        F: FnOnce(Option<&BV>) -> R;
}

impl<K, V, S> MapBacking for HashMap<K, V, S> {
    type KeyType = K;

    type ValueType = V;
}

impl<K, V, S> MapOps<K, V> for HashMap<K, V, S>
where
    K: Hash + Eq,
    S: BuildHasher + Default,
{
    fn get(&self, key: &K) -> Option<&V> {
        HashMap::get(self, key)
    }

    fn insert(&mut self, key: K, value: V) -> Option<V> {
        HashMap::insert(self, key, value)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        HashMap::remove(self, key)
    }

    fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = &'a K>
    where
        K: 'a,
    {
        HashMap::keys(self)
    }

    fn from_entries<I>(it: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        it.into_iter().collect()
    }

    const ORDERED_KEYS: bool = false;
}

impl<K, V> MapBacking for BTreeMap<K, V> {
    type KeyType = K;

    type ValueType = V;
}

impl<K, V> MapOps<K, V> for BTreeMap<K, V>
where
    K: Ord,
{
    fn get(&self, key: &K) -> Option<&V> {
        BTreeMap::get(self, key)
    }

    fn insert(&mut self, key: K, value: V) -> Option<V> {
        BTreeMap::insert(self, key, value)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        BTreeMap::remove(self, key)
    }

    fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = &'a K>
    where
        K: 'a,
    {
        BTreeMap::keys(self)
    }

    fn from_entries<I>(it: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        it.into_iter().collect()
    }

    const ORDERED_KEYS: bool = true;
}

impl<K, V, BK> MapOpsWithEntry<K, V, BK> for HashMap<K, V>
where
    K: Borrow<BK>,
    BK: ?Sized,
    K: Hash + Eq,
    BK: Hash + Eq,
{
    fn with_item<BV: ?Sized, F, R>(&self, key: &BK, f: F) -> R
    where
        V: Borrow<BV>,
        F: FnOnce(Option<&BV>) -> R,
    {
        f(self.get(key).map(Borrow::borrow))
    }
}

impl<K, V, BK> MapOpsWithEntry<K, V, BK> for BTreeMap<K, V>
where
    K: Borrow<BK>,
    BK: ?Sized,
    K: Ord,
    BK: Ord,
{
    fn with_item<BV: ?Sized, F, R>(&self, key: &BK, f: F) -> R
    where
        V: Borrow<BV>,
        F: FnOnce(Option<&BV>) -> R,
    {
        f(self.get(key).map(Borrow::borrow))
    }
}

impl<K, V, Q, M> MapStoreInner<K, V, Q, M>
where
    K: Clone,
    Q: MapEventQueue<K, V>,
    M: MapOps<K, V>,
{
    pub fn update(&mut self, key: K, value: V) {
        let MapStoreInner {
            content,
            previous,
            queue,
        } = self;
        let prev = content.insert(key.clone(), value);
        *previous = Some(MapLaneEvent::Update(key.clone(), prev));
        queue.push(MapOperation::Update { key, value: () });
    }

    pub fn transform_entry<F>(&mut self, key: K, f: F) -> TransformEntryResult
    where
        F: FnOnce(Option<&V>) -> Option<V>,
    {
        let MapStoreInner {
            content,
            previous,
            queue,
        } = self;
        match content.remove(&key) {
            Some(v) => match f(Some(&v)) {
                Some(v2) => {
                    content.insert(key.clone(), v2);
                    *previous = Some(MapLaneEvent::Update(key.clone(), Some(v)));
                    queue.push(MapOperation::Update { key, value: () });
                    TransformEntryResult::Update
                }
                _ => {
                    *previous = Some(MapLaneEvent::Remove(key.clone(), v));
                    queue.push(MapOperation::Remove { key: key.clone() });
                    TransformEntryResult::Remove
                }
            },
            _ => match f(None) {
                Some(v2) => {
                    content.insert(key.clone(), v2);
                    *previous = Some(MapLaneEvent::Update(key.clone(), None));
                    queue.push(MapOperation::Update { key, value: () });
                    TransformEntryResult::Update
                }
                _ => TransformEntryResult::NoChange,
            },
        }
    }

    pub fn remove(&mut self, key: &K) {
        let MapStoreInner {
            content,
            previous,
            queue,
        } = self;
        let prev = content.remove(key);
        if let Some(prev) = prev {
            *previous = Some(MapLaneEvent::Remove(key.clone(), prev));
            queue.push(MapOperation::Remove { key: key.clone() });
        }
    }
}

impl<K, V, Q, M> MapStoreInner<K, V, Q, M>
where
    Q: MapEventQueue<K, V>,
    M: MapOps<K, V>,
{
    pub fn clear(&mut self) {
        let MapStoreInner {
            content,
            previous,
            queue,
        } = self;
        *previous = Some(MapLaneEvent::Clear(content.take()));
        queue.push(MapOperation::Clear);
    }

    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&M) -> R,
    {
        let MapStoreInner { content, .. } = self;
        f(content)
    }

    pub fn read_with_prev<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V, M>>, &M) -> R,
    {
        let MapStoreInner {
            content, previous, ..
        } = self;
        f(previous.take(), content)
    }
}

impl<K, V, Q, M> MapStoreInner<K, V, Q, M> {
    pub fn with_entry<BK, BV, F, R>(&self, key: &BK, f: F) -> R
    where
        BK: ?Sized,
        BV: ?Sized,
        K: Borrow<BK>,
        V: Borrow<BV>,
        F: FnOnce(Option<&BV>) -> R,
        M: MapOpsWithEntry<K, V, BK>,
    {
        let MapStoreInner { content, .. } = self;
        MapOpsWithEntry::with_item(content, key, f)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DropOrTake {
    Drop,
    Take,
}

pub fn drop_or_take<K, V, M>(map: &M, kind: DropOrTake, number: usize) -> VecDeque<K>
where
    K: StructuralWritable + Clone + Eq + Hash,
    M: MapOps<K, V>,
{
    if M::ORDERED_KEYS {
        to_deque(kind, number, map.keys())
    } else {
        let mut keys_with_recon = map.keys().map(|k| (k.structure(), k)).collect::<Vec<_>>();
        keys_with_recon.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        let it = keys_with_recon.into_iter().map(|(_, k)| k);
        to_deque(kind, number, it)
    }
}

fn to_deque<'a, I, K>(kind: DropOrTake, number: usize, it: I) -> VecDeque<K>
where
    K: Clone + 'a,
    I: Iterator<Item = &'a K>,
{
    match kind {
        DropOrTake::Drop => it.take(number).cloned().collect(),
        DropOrTake::Take => it.skip(number).cloned().collect(),
    }
}
