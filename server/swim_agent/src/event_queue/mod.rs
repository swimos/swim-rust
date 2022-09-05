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
