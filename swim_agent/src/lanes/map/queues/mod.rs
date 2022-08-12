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

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use uuid::Uuid;

/// For a sync operation on a map lane, keeps track of which keys are synced for a given remote.
#[derive(Debug)]
pub struct SyncQueue<K> {
    id: Uuid,
    queue: VecDeque<K>,
}

impl<K> SyncQueue<K>
where
    K: Clone + Eq + Hash,
{
    pub fn new<I>(id: Uuid, it: I) -> Self
    where
        I: Iterator<Item = K>,
    {
        SyncQueue {
            id,
            queue: it.collect(),
        }
    }

    pub fn remove(&mut self, key: &K) {
        let SyncQueue { queue, .. } = self;
        if let Some(i) = queue.iter().position(|k| k == key) {
            queue.remove(i);
        }
    }

    pub fn clear(&mut self) {
        self.queue.clear();
    }

    pub fn pop(&mut self) -> Option<K> {
        self.queue.pop_front()
    }
}

#[derive(Debug)]
pub enum Action<K> {
    Clear,
    Update(K),
    Remove(K),
}

/// Keeps track of what changes to the state of the map need to be reported as events.
#[derive(Debug)]
pub struct EventQueue<K> {
    events: VecDeque<Action<K>>,
    head_epoch: usize,
    epoch_map: HashMap<K, usize>,
}

impl<K> Default for EventQueue<K> {
    fn default() -> Self {
        Self {
            events: Default::default(),
            head_epoch: Default::default(),
            epoch_map: Default::default(),
        }
    }
}

impl<K> EventQueue<K>
where
    K: Clone + Eq + Hash,
{
    pub fn push(&mut self, action: Action<K>) {
        let EventQueue {
            events,
            head_epoch,
            epoch_map,
        } = self;
        match action {
            Action::Clear => {
                *head_epoch = 0;
                events.clear();
                epoch_map.clear();
                events.push_back(Action::Clear);
            }
            Action::Update(k) => {
                if let Some(entry) = epoch_map.get(&k).and_then(|epoch| {
                    let index = epoch.wrapping_sub(*head_epoch);
                    debug_assert!(index < events.len());
                    events.get_mut(index)
                }) {
                    *entry = Action::Update(k);
                } else {
                    let epoch = head_epoch.wrapping_add(events.len());
                    events.push_back(Action::Update(k.clone()));
                    epoch_map.insert(k, epoch);
                }
            }
            Action::Remove(k) => {
                if let Some(entry) = epoch_map.get(&k).and_then(|epoch| {
                    let index = epoch.wrapping_sub(*head_epoch);
                    debug_assert!(index < events.len());
                    events.get_mut(index)
                }) {
                    *entry = Action::Remove(k);
                } else {
                    let epoch = head_epoch.wrapping_add(events.len());
                    events.push_back(Action::Remove(k.clone()));
                    epoch_map.insert(k, epoch);
                }
            }
        }
    }

    pub fn pop(&mut self) -> Option<Action<K>> {
        let EventQueue {
            events,
            head_epoch,
            epoch_map,
        } = self;
        if let Some(entry) = events.pop_front() {
            *head_epoch = head_epoch.wrapping_add(1);
            if let Action::Update(k) | Action::Remove(k) = &entry {
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

#[derive(Debug, Clone, Copy)]
enum EventOrSync {
    Event,
    Sync,
}

impl EventOrSync {
    pub fn is_event(&self) -> bool {
        matches!(self, EventOrSync::Event)
    }

    pub fn flip(&mut self) -> Self {
        match *self {
            EventOrSync::Event => {
                *self = EventOrSync::Sync;
                EventOrSync::Event
            }
            EventOrSync::Sync => {
                *self = EventOrSync::Event;
                EventOrSync::Sync
            }
        }
    }
}

/// Determines whether to produce a standard event or sync event next (if one is available).
#[derive(Debug, Clone, Copy)]
struct NextWrite {
    sync_index: usize,
    next: EventOrSync,
}

impl Default for NextWrite {
    fn default() -> Self {
        Self {
            sync_index: 0,
            next: EventOrSync::Event,
        }
    }
}

/// Queues to keep track of which events a map lane needs to produce. This includes both standard
/// events generated by changes to the state of the map and events to allow remotes to sync with the
/// state of the map.
#[derive(Debug)]
pub struct WriteQueues<K> {
    event_queue: EventQueue<K>,
    sync_queues: Vec<SyncQueue<K>>,
    next: NextWrite,
}

impl<K> Default for WriteQueues<K> {
    fn default() -> Self {
        Self {
            event_queue: Default::default(),
            sync_queues: Default::default(),
            next: Default::default(),
        }
    }
}

pub enum ToWrite<K> {
    Event(Action<K>),
    SyncEvent(Uuid, K),
    Synced(Uuid),
}

impl<K> WriteQueues<K>
where
    K: Clone + Eq + Hash,
{
    pub fn push_update(&mut self, key: K) {
        let WriteQueues { event_queue, .. } = self;
        event_queue.push(Action::Update(key));
    }

    pub fn push_remove(&mut self, key: K) {
        let WriteQueues { event_queue, .. } = self;
        event_queue.push(Action::Remove(key));
    }

    pub fn push_clear(&mut self) {
        let WriteQueues { event_queue, .. } = self;
        event_queue.push(Action::Clear);
    }

    pub fn sync<I>(&mut self, id: Uuid, keys: I)
    where
        I: Iterator<Item = K>,
    {
        self.sync_queues.push(SyncQueue::new(id, keys));
    }

    pub fn pop(&mut self) -> Option<ToWrite<K>> {
        let WriteQueues {
            event_queue,
            sync_queues,
            next: NextWrite { sync_index, next },
        } = self;
        let selection = next.flip();
        if (selection.is_event() && !event_queue.is_empty()) || sync_queues.is_empty() {
            if let Some(action) = event_queue.pop() {
                update_sync_queues(sync_queues, &action);
                Some(ToWrite::Event(action))
            } else {
                None
            }
        } else if let Some(queue) = sync_queues.get_mut(*sync_index) {
            Some(if let Some(k) = queue.pop() {
                let id = queue.id;
                *sync_index = (*sync_index + 1) % sync_queues.len();
                ToWrite::SyncEvent(id, k)
            } else {
                let id = sync_queues.remove(*sync_index).id;
                if *sync_index >= sync_queues.len() {
                    *sync_index = 0;
                }
                ToWrite::Synced(id)
            })
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.event_queue.is_empty() && self.sync_queues.is_empty()
    }
}

fn update_sync_queues<K>(queues: &mut Vec<SyncQueue<K>>, action: &Action<K>)
where
    K: Clone + Eq + Hash,
{
    match action {
        Action::Update(k) | Action::Remove(k) => {
            for queue in queues {
                queue.remove(k);
            }
        }
        _ => {
            for queue in queues {
                queue.clear();
            }
        }
    }
}
