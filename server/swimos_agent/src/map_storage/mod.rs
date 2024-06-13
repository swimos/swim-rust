// Copyright 2015-2023 Swim Inc.
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

use std::{borrow::Borrow, collections::HashMap, hash::Hash};

use swimos_agent_protocol::MapOperation;

use crate::lanes::map::MapLaneEvent;

/// Common backing store used by both map stores and map lanes.
#[derive(Debug)]
pub struct MapStoreInner<K, V, Q> {
    content: HashMap<K, V>,
    previous: Option<MapLaneEvent<K, V>>,
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

    fn pop<'a>(&mut self, content: &'a HashMap<K, V>) -> Option<Self::Output<'a>>;
}

impl<K, V, Q: Default> MapStoreInner<K, V, Q> {
    pub fn new(content: HashMap<K, V>) -> Self {
        MapStoreInner {
            content,
            previous: Default::default(),
            queue: Default::default(),
        }
    }
}

pub enum WithEntryResult {
    NoChange,
    Update,
    Remove,
}

impl<K, V, Q> MapStoreInner<K, V, Q>
where
    K: Eq + Hash + Clone,
    Q: MapEventQueue<K, V>,
{
    pub fn init(&mut self, map: HashMap<K, V>) {
        self.content = map;
    }

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

    pub fn with_entry<F>(&mut self, key: K, f: F) -> WithEntryResult
    where
        V: Clone,
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let MapStoreInner {
            content,
            previous,
            queue,
        } = self;
        match content.remove(&key) {
            Some(v) => match f(Some(v.clone())) {
                Some(v2) => {
                    content.insert(key.clone(), v2);
                    *previous = Some(MapLaneEvent::Update(key.clone(), Some(v)));
                    queue.push(MapOperation::Update { key, value: () });
                    WithEntryResult::Update
                }
                _ => {
                    *previous = Some(MapLaneEvent::Remove(key.clone(), v));
                    queue.push(MapOperation::Remove { key: key.clone() });
                    WithEntryResult::Remove
                }
            },
            _ => match f(None) {
                Some(v2) => {
                    content.insert(key.clone(), v2);
                    *previous = Some(MapLaneEvent::Update(key.clone(), None));
                    queue.push(MapOperation::Update { key, value: () });
                    WithEntryResult::Update
                }
                _ => WithEntryResult::NoChange,
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

    pub fn remove_local(&mut self, key: &K) {
        let MapStoreInner { content, .. } = self;
        content.remove(key);
    }

    pub fn clear(&mut self) {
        let MapStoreInner {
            content,
            previous,
            queue,
        } = self;
        *previous = Some(MapLaneEvent::Clear(std::mem::take(content)));
        queue.push(MapOperation::Clear);
    }

    pub fn get<B, F, R>(&self, key: &B, f: F) -> R
    where
        K: Borrow<B>,
        B: Hash + Eq,
        F: FnOnce(Option<&V>) -> R,
    {
        let MapStoreInner { content, .. } = self;
        f(content.get(key))
    }

    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<K, V>) -> R,
    {
        let MapStoreInner { content, .. } = self;
        f(content)
    }

    pub fn read_with_prev<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V>>, &HashMap<K, V>) -> R,
    {
        let MapStoreInner {
            content, previous, ..
        } = self;
        f(previous.take(), content)
    }

    pub fn queue(&mut self) -> &mut Q {
        &mut self.queue
    }

    pub fn pop_operation(&mut self) -> Option<Q::Output<'_>> {
        let MapStoreInner { content, queue, .. } = self;
        queue.pop(content)
    }
}
