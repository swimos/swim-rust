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
        for remote_id in &remote_ids {
            if let Entry::Occupied(mut entry) = backwards.entry(*remote_id) {
                entry.get_mut().remove(&id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
        remote_ids
    }

    pub fn remove_remote(&mut self, id: Uuid) {
        let Links { forward, backwards } = self;
        let lane_ids = backwards.remove(&id).unwrap_or_default();
        for lane_id in lane_ids {
            if let Entry::Occupied(mut entry) = forward.entry(lane_id) {
                entry.get_mut().remove(&id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use uuid::Uuid;

    use super::Links;

    const RID1: Uuid = Uuid::from_u128(567);
    const RID2: Uuid = Uuid::from_u128(97263);
    const RID3: Uuid = Uuid::from_u128(111);

    const LID1: u64 = 7;
    const LID2: u64 = 38;
    const LID3: u64 = 999;
    const LID4: u64 = 58349209;

    #[test]
    fn insert_link() {
        let mut links = Links::default();
        links.insert(LID1, RID1);

        assert_eq!(links.linked_from(LID1), Some(&[RID1].into()));
    }

    fn make_links() -> Links {
        let mut links = Links::default();
        links.insert(LID1, RID1);
        links.insert(LID2, RID1);
        links.insert(LID3, RID1);
        links.insert(LID1, RID2);
        links.insert(LID1, RID3);
        links
    }

    #[test]
    fn link_reporting() {
        let links = make_links();

        assert_eq!(links.linked_from(LID1), Some(&[RID1, RID2, RID3,].into()));

        assert_eq!(links.linked_from(LID2), Some(&[RID1,].into()));

        assert_eq!(links.linked_from(LID3), Some(&[RID1,].into()));

        assert_eq!(links.linked_from(LID4), None);
    }

    #[test]
    fn remove_link() {
        let mut links = make_links();

        links.remove(LID1, RID2);

        assert_eq!(links.linked_from(LID1), Some(&[RID1, RID3,].into()));

        links.remove(LID2, RID1);
        assert_eq!(links.linked_from(LID2), None);
    }

    #[test]
    fn remove_remote() {
        let mut links = make_links();

        links.remove_remote(RID1);

        assert_eq!(links.linked_from(LID1), Some(&[RID2, RID3,].into()));

        assert_eq!(links.linked_from(LID2), None);

        assert_eq!(links.linked_from(LID3), None);
    }

    #[test]
    fn remove_lane() {
        let mut links = make_links();

        let remotes = links.remove_lane(LID1);
        assert_eq!(remotes, [RID1, RID2, RID3].into());

        assert_eq!(links.linked_from(LID1), None);

        assert_eq!(links.linked_from(LID2), Some(&[RID1,].into()));

        assert_eq!(links.linked_from(LID3), Some(&[RID1,].into()));
    }
}
