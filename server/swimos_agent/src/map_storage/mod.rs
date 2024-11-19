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
    collections::{BTreeMap, HashMap},
    hash::{BuildHasher, Hash},
};

use swimos_agent_protocol::MapOperation;

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

pub trait MapOps<K, V> {
    fn get(&self, key: &K) -> Option<&V>;
    fn insert(&mut self, key: K, value: V) -> Option<V>;
    fn remove(&mut self, key: &K) -> Option<V>;
    fn take(&mut self) -> Self;
}

pub trait MapOpsWithEntry<K, V, BK: ?Sized, BV: ?Sized> {
    fn with_item<F, R>(&self, key: &BK, f: F) -> R
    where
        K: Borrow<BK>,
        V: Borrow<BV>,
        F: FnOnce(Option<&BV>) -> R;
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
}

impl<K, V, BK, BV> MapOpsWithEntry<K, V, BK, BV> for HashMap<K, V>
where
    BK: ?Sized,
    BV: ?Sized,
    K: Hash + Eq,
    BK: Hash + Eq,
{
    fn with_item<F, R>(&self, key: &BK, f: F) -> R
    where
        K: Borrow<BK>,
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
        M: MapOpsWithEntry<K, V, BK, BV>,
    {
        let MapStoreInner { content, .. } = self;
        MapOpsWithEntry::with_item(content, key, f)
    }
}
