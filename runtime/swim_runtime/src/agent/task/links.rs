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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct TriggerUnlink {
    pub remote_id: Uuid,
    pub schedule_prune: bool,
}

impl TriggerUnlink {
    fn new(remote_id: Uuid, schedule_prune: bool) -> Self {
        TriggerUnlink {
            remote_id,
            schedule_prune,
        }
    }

    pub fn into_option(self) -> Option<Uuid> {
        if self.schedule_prune {
            Some(self.remote_id)
        } else {
            None
        }
    }
}

impl Links {
    pub fn insert(&mut self, lane_id: u64, remote_id: Uuid) {
        let Links { forward, backwards } = self;
        forward.entry(lane_id).or_default().insert(remote_id);
        backwards.entry(remote_id).or_default().insert(lane_id);
    }

    #[must_use]
    pub fn remove(&mut self, lane_id: u64, remote_id: Uuid) -> TriggerUnlink {
        let Links { forward, backwards } = self;

        if let Entry::Occupied(mut entry) = forward.entry(lane_id) {
            entry.get_mut().remove(&remote_id);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
        if let Entry::Occupied(mut entry) = backwards.entry(remote_id) {
            entry.get_mut().remove(&lane_id);
            let no_links = entry.get().is_empty();
            if no_links {
                entry.remove();
            }
            TriggerUnlink::new(remote_id, no_links)
        } else {
            TriggerUnlink::new(remote_id, false)
        }
    }

    pub fn all_links(&self) -> impl Iterator<Item = (u64, Uuid)> + '_ {
        self.forward
            .iter()
            .flat_map(|(lane_id, remote_ids)| remote_ids.iter().map(move |rid| (*lane_id, *rid)))
    }

    pub fn linked_from(&self, id: u64) -> Option<&HashSet<Uuid>> {
        self.forward.get(&id)
    }

    pub fn linked_to(&self, id: Uuid) -> Option<&HashSet<u64>> {
        self.backwards.get(&id)
    }

    pub fn is_linked(&self, remote_id: Uuid, lane_id: u64) -> bool {
        self.forward
            .get(&lane_id)
            .map(|remotes| remotes.contains(&remote_id))
            .unwrap_or(false)
    }

    pub fn remove_lane(&mut self, id: u64) -> impl Iterator<Item = TriggerUnlink> + '_ {
        let Links { forward, backwards } = self;
        let remote_ids = forward.remove(&id).unwrap_or_default();
        remote_ids.into_iter().map(move |remote_id| {
            if let Entry::Occupied(mut entry) = backwards.entry(remote_id) {
                entry.get_mut().remove(&id);
                let no_links = entry.get().is_empty();
                if no_links {
                    entry.remove();
                }
                TriggerUnlink::new(remote_id, no_links)
            } else {
                TriggerUnlink::new(remote_id, false)
            }
        })
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

    use std::collections::HashMap;

    use uuid::Uuid;

    use crate::agent::task::links::TriggerUnlink;

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

        let TriggerUnlink {
            remote_id,
            schedule_prune,
        } = links.remove(LID1, RID2);
        assert_eq!(remote_id, RID2);
        assert!(schedule_prune);

        assert_eq!(links.linked_from(LID1), Some(&[RID1, RID3,].into()));

        let TriggerUnlink {
            remote_id,
            schedule_prune,
        } = links.remove(LID2, RID1);
        assert_eq!(remote_id, RID1);
        assert!(!schedule_prune);
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

        let remotes = links
            .remove_lane(LID1)
            .map(
                |TriggerUnlink {
                     remote_id,
                     schedule_prune,
                 }| (remote_id, schedule_prune),
            )
            .collect::<HashMap<_, _>>();
        assert_eq!(remotes, [(RID1, false), (RID2, true), (RID3, true)].into());

        assert_eq!(links.linked_from(LID1), None);

        assert_eq!(links.linked_from(LID2), Some(&[RID1,].into()));

        assert_eq!(links.linked_from(LID3), Some(&[RID1,].into()));
    }
}
