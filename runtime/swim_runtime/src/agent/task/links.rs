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

use crate::agent::reporting::UplinkReporter;

#[derive(Default, Debug)]
struct LaneLinks {
    remotes: HashSet<Uuid>,
    reporter: Option<UplinkReporter>,
}

enum Delta {
    Zero,
    Incr,
    Decr(u64),
}

impl Delta {
    fn apply(self, n: &mut u64) {
        match self {
            Delta::Zero => {}
            Delta::Incr => *n = n.saturating_add(1),
            Delta::Decr(d) => *n = n.saturating_sub(d),
        }
    }
}

impl LaneLinks {
    fn insert(&mut self, remote_id: Uuid) -> Delta {
        if self.remotes.insert(remote_id) {
            Delta::Incr
        } else {
            Delta::Zero
        }
    }

    fn remove(&mut self, remote_id: &Uuid) -> Delta {
        if self.remotes.remove(remote_id) {
            Delta::Decr(1)
        } else {
            Delta::Zero
        }
    }

    fn is_empty(&self) -> bool {
        self.remotes.is_empty()
    }

    fn take_remotes(&mut self) -> (Delta, impl Iterator<Item = Uuid> + 'static) {
        let delta =
            Delta::Decr(u64::try_from(self.remotes.len()).expect("Lenth did not fit into a u64."));
        (delta, std::mem::take(&mut self.remotes).into_iter())
    }

    fn contains(&self, id: &Uuid) -> bool {
        self.remotes.contains(id)
    }

    fn count_single(&self) {
        if let Some(reporter) = &self.reporter {
            reporter.count_events(1);
        }
    }

    fn count_broadcast(&self) -> u64 {
        let LaneLinks { remotes, reporter } = self;
        let n = u64::try_from(remotes.len()).expect("Length did not fit into a u64.");
        if let Some(reporter) = reporter {
            reporter.count_events(n);
        }
        n
    }
}
/// A registry of links between lanes and remotes in the agent runtime.
#[derive(Debug)]
pub struct Links {
    forward: HashMap<u64, LaneLinks>,
    backwards: HashMap<Uuid, HashSet<u64>>,
    aggregate_reporter: Option<(u64, UplinkReporter)>,
}

/// An instruction to send an unlinked message to a remote.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct TriggerUnlink {
    pub remote_id: Uuid,      // The ID of the remote.
    pub schedule_prune: bool, // The remote no longer has any links and so is elligable to be pruned.
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
    pub fn new(aggregate_reporter: Option<UplinkReporter>) -> Self {
        Links {
            forward: Default::default(),
            backwards: Default::default(),
            aggregate_reporter: aggregate_reporter.map(|r| (0, r)),
        }
    }

    pub fn register_reporter(&mut self, lane_id: u64, reporter: UplinkReporter) {
        self.forward
            .entry(lane_id)
            .or_insert_with(Default::default)
            .reporter = Some(reporter);
    }

    /// Create a new link from a lane to a remote.
    pub fn insert(&mut self, lane_id: u64, remote_id: Uuid) {
        let Links {
            forward,
            backwards,
            aggregate_reporter,
        } = self;
        let delta = forward.entry(lane_id).or_default().insert(remote_id);
        if let Some((n, reporter)) = aggregate_reporter {
            delta.apply(n);
            reporter.set_uplinks(*n);
        }
        backwards.entry(remote_id).or_default().insert(lane_id);
    }

    /// Remove a single link between a lane and remote.
    #[must_use]
    pub fn remove(&mut self, lane_id: u64, remote_id: Uuid) -> TriggerUnlink {
        let Links {
            forward,
            backwards,
            aggregate_reporter,
        } = self;

        if let Entry::Occupied(mut entry) = forward.entry(lane_id) {
            let delta = entry.get_mut().remove(&remote_id);
            if let Some((n, reporter)) = aggregate_reporter {
                delta.apply(n);
                reporter.set_uplinks(*n);
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

    /// Iterate over all links in the registry.
    pub fn remove_all_links(&mut self) -> impl Iterator<Item = (u64, Uuid)> + '_ {
        let Links {
            forward,
            backwards,
            aggregate_reporter,
        } = self;
        backwards.clear();
        if let Some((n, reporter)) = aggregate_reporter {
            *n = 0;
            reporter.set_uplinks(0);
        }
        forward.iter_mut().flat_map(|(lane_id, remote_ids)| {
            let (_, it) = remote_ids.take_remotes();
            it.map(move |rid| (*lane_id, rid))
        })
    }

    /// Get the remotes linked from a specific lane.
    pub fn linked_from(&self, id: u64) -> Option<&HashSet<Uuid>> {
        self.forward.get(&id).map(|links| &links.remotes)
    }

    /// Report a single event to the reporter for a lane (if present).
    pub fn count_single(&self, id: u64) {
        let Links {
            forward,
            aggregate_reporter,
            ..
        } = self;
        if let (Some((_, agg_reporter)), Some(links)) = (aggregate_reporter, forward.get(&id)) {
            links.count_single();
            agg_reporter.count_events(1);
        }
    }

    /// Report an event, for each link, to the reporter for a lane (if present).
    pub fn count_broadcast(&self, id: u64) {
        let Links {
            forward,
            aggregate_reporter,
            ..
        } = self;
        if let (Some((_, agg_reporter)), Some(links)) = (aggregate_reporter, forward.get(&id)) {
            let n = links.count_broadcast();
            agg_reporter.count_events(n);
        }
    }

    /// Get the lanes linked to a specific remote.
    pub fn linked_to(&self, id: Uuid) -> Option<&HashSet<u64>> {
        self.backwards.get(&id)
    }

    /// Determine if a specific link exists.
    pub fn is_linked(&self, remote_id: Uuid, lane_id: u64) -> bool {
        self.forward
            .get(&lane_id)
            .map(|remotes| remotes.contains(&remote_id))
            .unwrap_or(false)
    }

    /// Remove a lane (and all associated links). This can necessitate any number of unlinked messages.
    pub fn remove_lane(&mut self, id: u64) -> impl Iterator<Item = TriggerUnlink> + '_ {
        let Links {
            forward,
            backwards,
            aggregate_reporter,
        } = self;
        if let Some(links) = forward.get_mut(&id) {
            let (delta, remote_ids) = links.take_remotes();
            if let Some((n, reporter)) = aggregate_reporter {
                delta.apply(n);
                reporter.set_uplinks(*n);
            }
            Some(remote_ids.map(move |remote_id| {
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
            }))
        } else {
            None
        }
        .into_iter()
        .flatten()
    }

    /// Remove a remote and all associated links.
    pub fn remove_remote(&mut self, id: Uuid) {
        let Links {
            forward,
            backwards,
            aggregate_reporter,
        } = self;
        let lane_ids = backwards.remove(&id).unwrap_or_default();
        if let Some((n, reporter)) = aggregate_reporter {
            for lane_id in lane_ids {
                if let Entry::Occupied(mut entry) = forward.entry(lane_id) {
                    let delta = entry.get_mut().remove(&id);
                    delta.apply(n);
                    if entry.get().is_empty() {
                        entry.remove();
                    }
                }
            }
            reporter.set_uplinks(*n);
        } else {
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
        let mut links = Links::new(None);
        links.insert(LID1, RID1);

        assert_eq!(links.linked_from(LID1), Some(&[RID1].into()));
    }

    fn make_links() -> Links {
        let mut links = Links::new(None);
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
