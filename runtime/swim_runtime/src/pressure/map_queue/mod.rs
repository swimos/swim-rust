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

use std::{
    collections::{hash_map::RandomState, HashMap, VecDeque},
    hash::BuildHasher,
};

use bytes::{BufMut, BytesMut};
use swim_api::protocol::map::{MapOperation, RawMapOperation, RawMapOperationMut};

use crate::error::InvalidKey;

use super::key::ReconKey;

#[cfg(test)]
mod tests;

type QueueEntry = MapOperation<ReconKey, BytesMut>;

/// A queue of map operations to be sent by a transmitter of state updates for a map lane. If
/// an update/removal is sent for a key that is already in the queue this will replace the
/// existing entry (without altering its position in the queue) rather than being pushed at
/// the tail. If a clear operation is pushed into the queue, the queue will be emptied and the
/// clear pushed at the head.
///
/// # Notes
///
/// The queue contains an internal hash map, the hasing of which can be controlled via the type
/// parameter.
#[derive(Debug)]
pub struct MapOperationQueue<S = RandomState> {
    buffer: BytesMut,
    head_epoch: usize,
    queue: VecDeque<QueueEntry>,
    epoch_map: HashMap<ReconKey, usize, S>,
}

impl<S> MapOperationQueue<S> {
    pub fn with_hasher(hash_builder: S) -> Self {
        MapOperationQueue {
            buffer: BytesMut::new(),
            head_epoch: 0,
            queue: VecDeque::new(),
            epoch_map: HashMap::with_hasher(hash_builder),
        }
    }
}

impl MapOperationQueue<RandomState> {
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }
}

impl Default for MapOperationQueue<RandomState> {
    fn default() -> Self {
        Self::new()
    }
}

fn make_copy(target: &mut BytesMut, bytes: impl AsRef<[u8]>) -> BytesMut {
    let mut content = bytes.as_ref();
    target.put(&mut content);
    target.split()
}

impl<S> MapOperationQueue<S> {
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl<S: BuildHasher> MapOperationQueue<S> {
    /// Push an operation into the queue. It is necessary for the key to contain valid UTF-8 for
    /// it to be possible to compare keys and, if this is not the case, this operation will fail.
    ///
    /// # Arguments
    /// * `operation` - The operation push into the queue (any key must be valid UTF-8).
    pub fn push(&mut self, operation: RawMapOperationMut) -> Result<(), InvalidKey> {
        let MapOperationQueue {
            buffer,
            head_epoch,
            queue,
            epoch_map,
        } = self;
        match operation {
            RawMapOperationMut::Update { key, value } => {
                let recon_key = ReconKey::try_from(make_copy(buffer, &key).freeze())
                    .map_err(|e| InvalidKey::new(key.freeze(), e))?;
                let slot = epoch_map.get(&recon_key).and_then(|epoch| {
                    let index = epoch.wrapping_sub(*head_epoch);
                    debug_assert!(index < queue.len());
                    queue.get_mut(index)
                });
                if let Some(entry) = slot {
                    match entry {
                        QueueEntry::Update { value: old, .. } if old.capacity() >= value.len() => {
                            old.clear();
                            old.put(value);
                        }
                        _ => {
                            *entry = QueueEntry::Update {
                                key: recon_key,
                                value: make_copy(buffer, &value),
                            };
                        }
                    }
                } else {
                    let epoch = head_epoch.wrapping_add(queue.len());
                    let key = recon_key.clone();
                    epoch_map.insert(recon_key, epoch);
                    queue.push_back(QueueEntry::Update {
                        key,
                        value: make_copy(buffer, &value),
                    });
                }
            }
            RawMapOperationMut::Remove { key } => {
                let frozen_key = key.freeze();
                let recon_key = ReconKey::try_from(frozen_key.clone())
                    .map_err(|e| InvalidKey::new(frozen_key, e))?;
                let slot = epoch_map.get(&recon_key).and_then(|epoch| {
                    let index = epoch.wrapping_sub(*head_epoch);
                    debug_assert!(index < queue.len());
                    queue.get_mut(index)
                });
                if let Some(entry) = slot {
                    *entry = QueueEntry::Remove { key: recon_key };
                } else {
                    let epoch = head_epoch.wrapping_add(queue.len());
                    let key = recon_key.clone();
                    epoch_map.insert(recon_key, epoch);
                    queue.push_back(QueueEntry::Remove { key });
                }
            }
            RawMapOperationMut::Clear => {
                *head_epoch = 0;
                queue.clear();
                epoch_map.clear();
                queue.push_back(QueueEntry::Clear);
            }
        }
        Ok(())
    }

    /// Attempt to remove the head of the queue.
    pub fn pop(&mut self) -> Option<RawMapOperation> {
        let MapOperationQueue {
            head_epoch,
            queue,
            epoch_map,
            ..
        } = self;
        if let Some(entry) = queue.pop_front() {
            *head_epoch = head_epoch.wrapping_add(1);
            Some(match entry {
                MapOperation::Update { key, value } => {
                    epoch_map.remove(&key);
                    MapOperation::Update {
                        key: key.into_bytes(),
                        value,
                    }
                }
                MapOperation::Remove { key } => {
                    epoch_map.remove(&key);
                    MapOperation::Remove {
                        key: key.into_bytes(),
                    }
                }
                MapOperation::Clear => MapOperation::Clear,
            })
        } else {
            None
        }
    }
}
