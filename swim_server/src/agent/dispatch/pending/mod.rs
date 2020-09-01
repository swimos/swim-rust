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

#[derive(Debug)]
pub struct PendingEnvelopes {
    max_pending: usize,
    num_pending: usize,
    pending: HashMap<String, VecDeque<TaggedClientEnvelope>>,
    queue_store: Vec<VecDeque<TaggedClientEnvelope>>,
}

impl PendingEnvelopes {

    pub fn new(max_pending: usize) -> Self {
        PendingEnvelopes {
            max_pending,
            num_pending: 0,
            pending: Default::default(),
            queue_store: vec![],
        }
    }

    pub fn enqueue(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
    ) -> Result<(), (String, TaggedClientEnvelope)> {
        self.push(lane, envelope, false)
    }

    pub fn replace(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
    ) -> Result<(), (String, TaggedClientEnvelope)> {
        self.push(lane, envelope, true)
    }

    pub fn push(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
        front: bool,
    ) -> Result<(), (String, TaggedClientEnvelope)> {
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
            Ok(())
        } else {
            Err((lane, envelope))
        }
    }

    pub fn pop<Q>(&mut self, lane: &Q) -> Option<TaggedClientEnvelope>
        where
            String: Borrow<Q>,
            Q: Hash + Eq,
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