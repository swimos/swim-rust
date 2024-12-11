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

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use swimos_agent_protocol::MapOperation;
use swimos_agent_protocol::StoreResponse;

use crate::map_storage::{MapEventQueue, MapOps};

/// Keeps track of what changes to the state of the map need to be reported as events.
#[derive(Debug)]
pub struct EventQueue<K, V> {
    events: VecDeque<MapOperation<K, V>>,
    head_epoch: usize,
    epoch_map: HashMap<K, usize>,
}

impl<K, V> Default for EventQueue<K, V> {
    fn default() -> Self {
        Self {
            events: Default::default(),
            head_epoch: Default::default(),
            epoch_map: Default::default(),
        }
    }
}

impl<K, V> EventQueue<K, V>
where
    K: Clone + Eq + Hash,
{
    pub fn push(&mut self, action: MapOperation<K, V>) {
        let EventQueue {
            events,
            head_epoch,
            epoch_map,
        } = self;
        match action {
            MapOperation::Clear => {
                *head_epoch = 0;
                events.clear();
                epoch_map.clear();
                events.push_back(MapOperation::Clear);
            }
            MapOperation::Update { key: k, value: v } => {
                if let Some(entry) = epoch_map.get(&k).and_then(|epoch| {
                    let index = epoch.wrapping_sub(*head_epoch);
                    debug_assert!(index < events.len());
                    events.get_mut(index)
                }) {
                    *entry = MapOperation::Update { key: k, value: v };
                } else {
                    let epoch = head_epoch.wrapping_add(events.len());
                    events.push_back(MapOperation::Update {
                        key: k.clone(),
                        value: v,
                    });
                    epoch_map.insert(k, epoch);
                }
            }
            MapOperation::Remove { key: k } => {
                if let Some(entry) = epoch_map.get(&k).and_then(|epoch| {
                    let index = epoch.wrapping_sub(*head_epoch);
                    debug_assert!(index < events.len());
                    events.get_mut(index)
                }) {
                    *entry = MapOperation::Remove { key: k };
                } else {
                    let epoch = head_epoch.wrapping_add(events.len());
                    events.push_back(MapOperation::Remove { key: k.clone() });
                    epoch_map.insert(k, epoch);
                }
            }
        }
    }

    pub fn pop(&mut self) -> Option<MapOperation<K, V>> {
        let EventQueue {
            events,
            head_epoch,
            epoch_map,
        } = self;
        if let Some(entry) = events.pop_front() {
            *head_epoch = head_epoch.wrapping_add(1);
            if let MapOperation::Update { key: k, .. } | MapOperation::Remove { key: k } = &entry {
                epoch_map.remove(k);
            }
            Some(entry)
        } else {
            None
        }
    }
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl<K, V> MapEventQueue<K, V> for EventQueue<K, ()>
where
    K: Eq + Hash + Clone,
{
    fn push(&mut self, action: MapOperation<K, ()>) {
        EventQueue::push(self, action)
    }

    type Output<'a> = StoreResponse<MapOperation<K, &'a V>>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn pop<'a, M>(&mut self, content: &'a M) -> Option<Self::Output<'a>>
    where
        K: 'a,
        M: MapOps<K, V>,
    {
        loop {
            let action = EventQueue::pop(self)?;
            if let Some(op) = to_operation(content, action) {
                break Some(StoreResponse::new(op));
            }
        }
    }
}

pub type Action<K> = MapOperation<K, ()>;

pub fn to_operation<K, V, M>(content: &M, action: Action<K>) -> Option<MapOperation<K, &V>>
where
    M: MapOps<K, V>,
{
    match action {
        MapOperation::Update { key, .. } => content
            .get(&key)
            .map(|v| MapOperation::Update { key, value: v }),
        MapOperation::Remove { key } => Some(MapOperation::Remove { key }),
        MapOperation::Clear => Some(MapOperation::Clear),
    }
}
