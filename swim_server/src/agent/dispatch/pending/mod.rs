// Copyright 2015-2020 SWIM.AI inc.
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
use std::collections::hash_map::Entry;
use crate::routing::TaggedClientEnvelope;
use std::borrow::Borrow;
use std::hash::Hash;

#[cfg(test)]
mod tests;

/// Queue of pending [`Envelope`]s, by lane, that will reject new envelopes when a maximum number
/// is reached. Queues are recycled once they become empty and used for other lanes.
#[derive(Debug)]
pub struct PendingEnvelopes {
    max_pending: usize,
    num_pending: usize,
    pending: HashMap<String, VecDeque<TaggedClientEnvelope>>,
    queue_store: Vec<VecDeque<TaggedClientEnvelope>>,
}

impl PendingEnvelopes {

    /// The maximum number of envelopes that the queue will hold (for all lanes).
    pub fn new(max_pending: usize) -> Self {
        PendingEnvelopes {
            max_pending,
            num_pending: 0,
            pending: Default::default(),
            queue_store: vec![],
        }
    }

    /// Attempt to enqueue an envelope for a lane.
    pub fn enqueue(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
    ) -> Result<bool, (String, TaggedClientEnvelope)> {
        self.push(lane, envelope, false)
    }

    /// Replace a previously popped envelope (inserting it at the front of the appropriate queue).
    pub fn replace(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
    ) -> Result<bool, (String, TaggedClientEnvelope)> {
        self.push(lane, envelope, true)
    }

    /// Clear all pending envelopes for a given label.
    pub fn clear<Q>(&mut self, lane: &Q) -> bool
        where
            String: Borrow<Q>,
            Q: ?Sized + Hash + Eq,
    {
        if let Some(mut queue) = self.pending.remove(&lane) {
            self.num_pending -= queue.len();
            queue.clear();
            self.queue_store.push(queue);
        }
        self.num_pending < self.max_pending
    }

    fn push(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
        front: bool,
    ) -> Result<bool, (String, TaggedClientEnvelope)> {
        let PendingEnvelopes {
            max_pending,
            num_pending,
            pending,
            queue_store,
        } = self;
        if *num_pending < *max_pending {
            let queue = match pending.entry(lane) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(queue_store.pop().unwrap_or_else(VecDeque::new))
                }
            };
            if front {
                queue.push_front(envelope);
            } else {
                queue.push_back(envelope);
            }
            *num_pending += 1;
            Ok(*num_pending < *max_pending)
        } else {
            Err((lane, envelope))
        }
    }

    /// Pop an envelope from the front of the queue for a lane.
    pub fn pop<Q>(&mut self, lane: &Q) -> Option<TaggedClientEnvelope>
        where
            String: Borrow<Q>,
            Q: ?Sized + Hash + Eq,
    {
        let PendingEnvelopes {
            num_pending,
            pending,
            queue_store,
            ..
        } = self;
        let (envelope, cleared_queue) = if let Some(queue) = pending.get_mut(lane) {
            let envelope = queue.pop_front();
            (envelope, queue.is_empty())
        } else {
            (None, false)
        };
        if cleared_queue {
            if let Some(queue) = pending.remove(lane) {
                queue_store.push(queue);
            }
        }
        if envelope.is_some() {
            *num_pending -= 1;
        }
        envelope
    }
}