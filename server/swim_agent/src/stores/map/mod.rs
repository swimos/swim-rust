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

use std::borrow::Borrow;
use std::hash::Hash;
use std::{cell::RefCell, collections::HashMap};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use swim_api::protocol::agent::{MapStoreResponseEncoder, StoreResponse};
use swim_form::structural::write::StructuralWritable;
use tokio_util::codec::Encoder;

use crate::agent_model::WriteResult;
use crate::event_queue::EventQueue;
use crate::lanes::map::MapLaneEvent;
use crate::AgentItem;
use crate::map_storage::MapStoreInner;

use super::Store;

type Inner<K, V> = MapStoreInner<K, V, EventQueue<K, ()>>;

#[derive(Debug)]
pub struct MapStore<K, V> {
    id: u64,
    inner: RefCell<Inner<K, V>>,
}

assert_impl_all!(MapStore<(), ()>: Send);

impl<K, V> MapStore<K, V> {
    /// #Arguments
    /// * `id` - The ID of the lane. This should be unique within an agent.
    /// * `init` - The initial contents of the map.
    pub fn new(id: u64, init: HashMap<K, V>) -> Self {
        MapStore {
            id,
            inner: RefCell::new(Inner::new(init)),
        }
    }
}

impl<K, V> AgentItem for MapStore<K, V> {
    fn id(&self) -> u64 {
        self.id
    }
}

impl<K, V> MapStore<K, V>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn init(&self, map: HashMap<K, V>) {
        self.inner.borrow_mut().init(map)
    }

    /// Update the value associated with a key.
    pub fn update(&self, key: K, value: V) {
        self.inner.borrow_mut().update(key, value)
    }

    /// Remove and entry from the map.
    pub fn remove(&self, key: &K) {
        self.inner.borrow_mut().remove(key)
    }

    /// Clear the map.
    pub fn clear(&self) {
        self.inner.borrow_mut().clear()
    }

    /// Read a value from the map, if it exists.
    pub fn get<Q, F, R>(&self, key: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
        F: FnOnce(Option<&V>) -> R,
    {
        self.inner.borrow().get(key, f)
    }

    /// Read the complete state of the map.
    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<K, V>) -> R,
    {
        self.inner.borrow().get_map(f)
    }

    /// Read the state of the map, consuming the previous event (used when triggering
    /// the event handlers for the lane).
    pub(crate) fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V>>, &HashMap<K, V>) -> R,
    {
        self.inner.borrow_mut().read_with_prev(f)
    }
}

const INFALLIBLE_SER: &str = "Serializing store responses to recon should be infallible.";

impl<K, V> Store for MapStore<K, V>
where
    K: Clone + Eq + Hash + StructuralWritable + 'static,
    V: StructuralWritable + 'static,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let mut encoder = MapStoreResponseEncoder::default();
        let mut guard = self.inner.borrow_mut();
        if let Some(op) = guard.pop_operation() {
            encoder.encode(StoreResponse::new(op), buffer).expect(INFALLIBLE_SER);
            if guard.queue().is_empty() {
                WriteResult::Done
            } else {
                WriteResult::DataStillAvailable
            }
        } else {
            WriteResult::NoData
        }
    }
}
 