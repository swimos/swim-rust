// Copyright 2015-2023 Swim Inc.
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
use swim_api::protocol::agent::MapStoreResponseEncoder;
use swim_form::structural::write::StructuralWritable;
use tokio_util::codec::Encoder;

use crate::agent_model::WriteResult;
use crate::event_handler::{ActionContext, HandlerAction, Modification, StepResult};
use crate::event_queue::EventQueue;
use crate::item::{AgentItem, MapItem};
use crate::map_storage::{MapStoreInner, WithEntryResult};
use crate::meta::AgentMetadata;

use super::StoreItem;

#[cfg(test)]
mod tests;

type Inner<K, V> = MapStoreInner<K, V, EventQueue<K, ()>>;

/// Adding a [`MapStore`] to an agent provides additional state that is not exposed as a lane.
/// If persistence is enabled (and the store is not marked as transient) the state of the store
/// will be persisted in the same way as the state of a lane.
#[derive(Debug)]
pub struct MapStore<K, V> {
    id: u64,
    inner: RefCell<Inner<K, V>>,
}

assert_impl_all!(MapStore<(), ()>: Send);

impl<K, V> MapStore<K, V> {
    /// #Arguments
    /// * `id` - The ID of the store. This should be unique within an agent.
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

impl<K, V> MapItem<K, V> for MapStore<K, V>
where
    K: Eq + Hash + Clone,
{
    fn init(&self, map: HashMap<K, V>) {
        self.inner.borrow_mut().init(map)
    }

    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<crate::lanes::map::MapLaneEvent<K, V>>, &HashMap<K, V>) -> R,
    {
        self.inner.borrow_mut().read_with_prev(f)
    }
}

impl<K, V> MapStore<K, V>
where
    K: Clone + Eq + Hash,
{
    /// Update the value associated with a key.
    pub fn update(&self, key: K, value: V) {
        self.inner.borrow_mut().update(key, value)
    }

    /// Transform the value associated with a key.
    pub fn with_entry<F>(&self, key: K, f: F) -> WithEntryResult
    where
        V: Clone,
        F: FnOnce(Option<V>) -> Option<V>,
    {
        self.inner.borrow_mut().with_entry(key, f)
    }

    /// Remove an entry from the map.
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
}

const INFALLIBLE_SER: &str = "Serializing store responses to recon should be infallible.";

impl<K, V> StoreItem for MapStore<K, V>
where
    K: Clone + Eq + Hash + StructuralWritable + 'static,
    V: StructuralWritable + 'static,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let mut encoder = MapStoreResponseEncoder::default();
        let mut guard = self.inner.borrow_mut();
        if let Some(op) = guard.pop_operation() {
            encoder.encode(op, buffer).expect(INFALLIBLE_SER);
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

/// An [`EventHandler`] that will update the value of an entry in the map.
pub struct MapStoreUpdate<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>,
    key_value: Option<(K, V)>,
}

impl<C, K, V> MapStoreUpdate<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>, key: K, value: V) -> Self {
        MapStoreUpdate {
            projection,
            key_value: Some((key, value)),
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapStoreUpdate<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapStoreUpdate {
            projection,
            key_value,
        } = self;
        if let Some((key, value)) = key_value.take() {
            let store = projection(context);
            store.update(key, value);
            StepResult::Complete {
                modified_item: Some(Modification::of(store.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will remove an entry from the map.
pub struct MapStoreRemove<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>,
    key: Option<K>,
}

impl<C, K, V> MapStoreRemove<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>, key: K) -> Self {
        MapStoreRemove {
            projection,
            key: Some(key),
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapStoreRemove<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapStoreRemove { projection, key } = self;
        if let Some(key) = key.take() {
            let store = projection(context);
            store.remove(&key);
            StepResult::Complete {
                modified_item: Some(Modification::of(store.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will clear the map.
pub struct MapStoreClear<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>,
    done: bool,
}

impl<C, K, V> MapStoreClear<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>) -> Self {
        MapStoreClear {
            projection,
            done: false,
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapStoreClear<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapStoreClear { projection, done } = self;
        if !*done {
            *done = true;
            let store = projection(context);
            store.clear();
            StepResult::Complete {
                modified_item: Some(Modification::of(store.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will get an entry from the map.
pub struct MapStoreGet<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>,
    key: K,
    done: bool,
}

impl<C, K, V> MapStoreGet<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>, key: K) -> Self {
        MapStoreGet {
            projection,
            key,
            done: false,
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapStoreGet<C, K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Completion = Option<V>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapStoreGet {
            projection,
            key,
            done,
        } = self;
        if !*done {
            *done = true;
            let store = projection(context);
            StepResult::done(store.get(key, |v| v.cloned()))
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will read the entire state of a map store.
pub struct MapStoreGetMap<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>,
    done: bool,
}

impl<C, K, V> MapStoreGetMap<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>) -> Self {
        MapStoreGetMap {
            projection,
            done: false,
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapStoreGetMap<C, K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Completion = HashMap<K, V>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapStoreGetMap { projection, done } = self;
        if !*done {
            *done = true;
            let store = projection(context);
            StepResult::done(store.get_map(Clone::clone))
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will alter an entry in the map.
pub struct MapStoreWithEntry<C, K, V, F> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>,
    key_and_f: Option<(K, F)>,
}

impl<C, K, V, F> MapStoreWithEntry<C, K, V, F> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V>, key: K, f: F) -> Self {
        MapStoreWithEntry {
            projection,
            key_and_f: Some((key, f)),
        }
    }
}

impl<C, K, V, F> HandlerAction<C> for MapStoreWithEntry<C, K, V, F>
where
    K: Clone + Eq + Hash,
    V: Clone,
    F: FnOnce(Option<V>) -> Option<V>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapStoreWithEntry {
            projection,
            key_and_f,
        } = self;
        if let Some((key, f)) = key_and_f.take() {
            let store = projection(context);
            if matches!(store.with_entry(key, f), WithEntryResult::NoChange) {
                StepResult::done(())
            } else {
                StepResult::Complete {
                    modified_item: Some(Modification::of(store.id())),
                    result: (),
                }
            }
        } else {
            StepResult::after_done()
        }
    }
}
