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

use std::collections::{hash_map::Entry, HashMap, HashSet};

use uuid::Uuid;

#[derive(Debug, Default)]
pub struct Links {
    forward: HashMap<u64, HashSet<Uuid>>,
    backwards: HashMap<Uuid, HashSet<u64>>,
}

impl Links {
    pub fn insert(&mut self, lane_id: u64, remote_id: Uuid) {
        let Links { forward, backwards } = self;
        forward.entry(lane_id).or_default().insert(remote_id);
        backwards.entry(remote_id).or_default().insert(lane_id);
    }

    pub fn remove(&mut self, lane_id: u64, remote_id: Uuid) {
        let Links { forward, backwards } = self;

        if let Entry::Occupied(mut entry) = forward.entry(lane_id) {
            entry.get_mut().remove(&remote_id);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
        if let Entry::Occupied(mut entry) = backwards.entry(remote_id) {
            entry.get_mut().remove(&lane_id);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    pub fn linked_from(&self, id: u64) -> Option<&HashSet<Uuid>> {
        self.forward.get(&id)
    }

    pub fn remove_lane(&mut self, id: u64) -> HashSet<Uuid> {
        let Links { forward, backwards } = self;
        let remote_ids = forward.remove(&id).unwrap_or_default();
        for remote_id in remote_ids.iter() {
            if let Some(set) = backwards.get_mut(remote_id) {
                set.remove(&id);
            }
        }
        remote_ids
    }

    pub fn remove_remote(&mut self, id: Uuid) {
        let Links { forward, backwards } = self;
        let lane_ids = backwards.remove(&id).unwrap_or_default();
        for lane_id in lane_ids.iter() {
            if let Some(set) = forward.get_mut(lane_id) {
                set.remove(&id);
            }
        }
    }
}
