// Copyright 2015-2024 Swim Inc.
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

use std::any::type_name;
use std::borrow::Borrow;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::{cell::RefCell, collections::HashMap};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use swimos_agent_protocol::encoding::store::MapStoreResponseEncoder;
use swimos_form::write::StructuralWritable;
use tokio_util::codec::Encoder;

use crate::agent_model::{AgentDescription, WriteResult};
use crate::event_handler::{ActionContext, HandlerAction, Modification, StepResult};
use crate::event_queue::EventQueue;
use crate::item::{AgentItem, InspectableMapLikeItem, MapItem, MapLikeItem, MutableMapLikeItem};
use crate::lanes::map::MapLaneEvent;
use crate::map_storage::{MapOps, MapOpsWithEntry, MapStoreInner, TransformEntryResult};
use crate::meta::AgentMetadata;

use super::StoreItem;

#[cfg(test)]
mod tests;

type Inner<K, V, M> = MapStoreInner<K, V, EventQueue<K, ()>, M>;

/// Adding a [`MapStore`] to an agent provides additional state that is not exposed as a lane.
/// If persistence is enabled (and the store is not marked as transient) the state of the store
/// will be persisted in the same way as the state of a lane.
#[derive(Debug)]
pub struct MapStore<K, V, M = HashMap<K, V>> {
    id: u64,
    inner: RefCell<Inner<K, V, M>>,
}

assert_impl_all!(MapStore<(), ()>: Send);

impl<K, V, M> MapStore<K, V, M> {
    /// # Arguments
    /// * `id` - The ID of the store. This should be unique within an agent.
    /// * `init` - The initial contents of the map.
    pub fn new(id: u64, init: M) -> Self {
        MapStore {
            id,
            inner: RefCell::new(Inner::new(init)),
        }
    }
}

impl<K, V, M> AgentItem for MapStore<K, V, M> {
    fn id(&self) -> u64 {
        self.id
    }
}

impl<K, V, M> MapItem<K, V, M> for MapStore<K, V, M>
where
    K: Hash + Eq + Clone,
    M: MapOps<K, V>,
{
    fn init(&self, map: M) {
        self.inner.borrow_mut().init(map)
    }

    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V, M>>, &M) -> R,
    {
        self.inner.borrow_mut().read_with_prev(f)
    }
}

impl<K, V, M> MapStore<K, V, M>
where
    K: Clone + Eq + Hash,
    M: MapOps<K, V>,
{
    /// Update the value associated with a key.
    pub fn update(&self, key: K, value: V) {
        self.inner.borrow_mut().update(key, value)
    }

    /// Transform the value associated with a key.
    pub fn transform_entry<F>(&self, key: K, f: F) -> TransformEntryResult
    where
        F: FnOnce(Option<&V>) -> Option<V>,
    {
        self.inner.borrow_mut().transform_entry(key, f)
    }

    /// Remove an entry from the map.
    pub fn remove(&self, key: &K) {
        self.inner.borrow_mut().remove(key)
    }

    /// Clear the map.
    pub fn clear(&self) {
        self.inner.borrow_mut().clear()
    }

    /// Read the complete state of the map.
    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&M) -> R,
    {
        self.inner.borrow().get_map(f)
    }
}

impl<K, V, M> MapStore<K, V, M> {
    /// Read a value from the map, if it exists.
    pub fn get<Q, F, R>(&self, key: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        F: FnOnce(Option<&V>) -> R,
        M: MapOpsWithEntry<K, V, Q>,
    {
        self.inner.borrow().with_entry(key, f)
    }
}

impl<K, V, M> MapStore<K, V, M> {
    pub fn with_entry<F, B1, B2, U>(&self, key: &B1, f: F) -> U
    where
        B1: ?Sized,
        B2: ?Sized,
        K: Borrow<B1>,
        V: Borrow<B2>,
        F: FnOnce(Option<&B2>) -> U,
        M: MapOpsWithEntry<K, V, B1>,
    {
        self.inner.borrow().with_entry(key, f)
    }
}

const INFALLIBLE_SER: &str = "Serializing store responses to recon should be infallible.";

impl<K, V, M> StoreItem for MapStore<K, V, M>
where
    K: Clone + Eq + Hash + StructuralWritable + 'static,
    V: StructuralWritable + 'static,
    M: MapOps<K, V>,
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

///  An [event handler](crate::event_handler::EventHandler)`] that will update the value of an entry in the map.
pub struct MapStoreUpdate<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>,
    key_value: Option<(K, V)>,
}

impl<C, K, V, M> MapStoreUpdate<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>, key: K, value: V) -> Self {
        MapStoreUpdate {
            projection,
            key_value: Some((key, value)),
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapStoreUpdate<C, K, V, M>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    M: MapOps<K, V>,
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapStoreUpdate {
            projection,
            key_value,
        } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapStoreUpdate")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &key_value.is_none())
            .finish()
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        true
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &C, mut hasher: &mut dyn std::hash::Hasher) {
        use std::any::TypeId;

        let lane = (self.projection)(context);
        TypeId::of::<MapStoreUpdate<(), (), (), ()>>().hash(&mut hasher);
        hasher.write_u64(lane.id());
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will remove an entry from the map.
pub struct MapStoreRemove<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>,
    key: Option<K>,
}

impl<C, K, V, M> MapStoreRemove<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>, key: K) -> Self {
        MapStoreRemove {
            projection,
            key: Some(key),
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapStoreRemove<C, K, V, M>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    M: MapOps<K, V>,
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapStoreRemove { projection, key } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapStoreRemove")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &key.is_none())
            .finish()
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        true
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &C, mut hasher: &mut dyn std::hash::Hasher) {
        use std::any::TypeId;

        let lane = (self.projection)(context);
        TypeId::of::<MapStoreRemove<(), (), (), ()>>().hash(&mut hasher);
        hasher.write_u64(lane.id());
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will clear the map.
pub struct MapStoreClear<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>,
    done: bool,
}

impl<C, K, V, M> MapStoreClear<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>) -> Self {
        MapStoreClear {
            projection,
            done: false,
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapStoreClear<C, K, V, M>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    M: MapOps<K, V>,
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapStoreClear { projection, done } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapStoreClear")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", done)
            .finish()
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        true
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &C, mut hasher: &mut dyn std::hash::Hasher) {
        use std::any::TypeId;

        let lane = (self.projection)(context);
        TypeId::of::<MapStoreClear<(), (), (), ()>>().hash(&mut hasher);
        hasher.write_u64(lane.id());
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will get an entry from the map.
pub struct MapStoreGet<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>,
    key: K,
    done: bool,
}

impl<C, K, V, M> MapStoreGet<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>, key: K) -> Self {
        MapStoreGet {
            projection,
            key,
            done: false,
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapStoreGet<C, K, V, M>
where
    C: AgentDescription,
    V: Clone,
    M: MapOpsWithEntry<K, V, K>,
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
            StepResult::done(MapStore::get(store, key, |v| v.cloned()))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapStoreGet")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &self.done)
            .finish()
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        true
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &C, mut hasher: &mut dyn std::hash::Hasher) {
        use std::any::TypeId;

        let lane = (self.projection)(context);
        TypeId::of::<MapStoreGet<(), (), (), ()>>().hash(&mut hasher);
        hasher.write_u64(lane.id());
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will read the entire state of a map store.
pub struct MapStoreGetMap<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>,
    done: bool,
}

impl<C, K, V, M> MapStoreGetMap<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>) -> Self {
        MapStoreGetMap {
            projection,
            done: false,
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapStoreGetMap<C, K, V, M>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    V: Clone,
    M: MapOps<K, V> + Clone,
{
    type Completion = M;

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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapStoreGetMap")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &self.done)
            .finish()
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        true
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &C, mut hasher: &mut dyn std::hash::Hasher) {
        use std::any::TypeId;

        let lane = (self.projection)(context);
        TypeId::of::<MapStoreGetMap<(), (), (), ()>>().hash(&mut hasher);
        hasher.write_u64(lane.id());
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that may alter an entry in the map.
pub struct MapStoreTransformEntry<C, K, V, F, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>,
    key_and_f: Option<(K, F)>,
}

impl<C, K, V, F, M> MapStoreTransformEntry<C, K, V, F, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>, key: K, f: F) -> Self {
        MapStoreTransformEntry {
            projection,
            key_and_f: Some((key, f)),
        }
    }
}

impl<C, K, V, F, M> HandlerAction<C> for MapStoreTransformEntry<C, K, V, F, M>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    F: FnOnce(Option<&V>) -> Option<V>,
    M: MapOps<K, V>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapStoreTransformEntry {
            projection,
            key_and_f,
        } = self;
        if let Some((key, f)) = key_and_f.take() {
            let store = projection(context);
            if matches!(
                store.transform_entry(key, f),
                TransformEntryResult::NoChange
            ) {
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapStoreTransformEntry {
            projection,
            key_and_f,
        } = self;
        let lane = projection(context);
        let name = context.item_name(lane.id());

        f.debug_struct("MapStoreTransformEntry")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &key_and_f.is_none())
            .finish()
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        true
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &C, mut hasher: &mut dyn std::hash::Hasher) {
        use std::any::TypeId;

        let lane = (self.projection)(context);
        TypeId::of::<MapStoreTransformEntry<(), (), (), (), ()>>().hash(&mut hasher);
        hasher.write_u64(lane.id());
    }
}

/// A [handler action][`HandlerAction`] that will produce a value by applying a closure to a reference to
/// and entry in the store.
pub struct MapStoreWithEntry<C, K, V, F, B: ?Sized, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>,
    key_and_f: Option<(K, F)>,
    _type: PhantomData<fn(&B)>,
}

impl<C, K, V, F, B: ?Sized, M> MapStoreWithEntry<C, K, V, F, B, M> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the store.
    /// * `key` - Key of the entry.
    /// * `f` - The closure to apply to the entry.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapStore<K, V, M>, key: K, f: F) -> Self {
        MapStoreWithEntry {
            projection,
            key_and_f: Some((key, f)),
            _type: PhantomData,
        }
    }
}

impl<C, K, V, F, B, U, M> HandlerAction<C> for MapStoreWithEntry<C, K, V, F, B, M>
where
    C: AgentDescription,
    B: ?Sized,
    V: Borrow<B>,
    F: FnOnce(Option<&B>) -> U,
    M: MapOpsWithEntry<K, V, K>,
{
    type Completion = U;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapStoreWithEntry {
            projection,
            key_and_f,
            ..
        } = self;
        if let Some((key, f)) = key_and_f.take() {
            let store = projection(context);
            StepResult::done(store.with_entry(&key, f))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapStoreWithEntry")
            .field("id", &lane.id())
            .field("store_name", &name.as_ref().map(|s| s.as_ref()))
            .field("result_type", &type_name::<U>())
            .field("consumed", &self.key_and_f.is_none())
            .finish()
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        true
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &C, mut hasher: &mut dyn std::hash::Hasher) {
        use std::any::TypeId;

        let lane = (self.projection)(context);
        TypeId::of::<MapStoreWithEntry<(), (), (), (), (), ()>>().hash(&mut hasher);
        hasher.write_u64(lane.id());
    }
}
impl<K, V, M> MapLikeItem<K, V, M> for MapStore<K, V, M>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + 'static,
    M: MapOpsWithEntry<K, V, K> + Clone + 'static,
{
    type GetHandler<C> = MapStoreGet<C, K, V, M>
    where
        C: AgentDescription + 'static;

    fn get_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
    ) -> Self::GetHandler<C> {
        MapStoreGet::new(projection, key)
    }

    type GetMapHandler<C> = MapStoreGetMap<C, K, V, M>
    where
        C: AgentDescription + 'static;

    fn get_map_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::GetMapHandler<C> {
        MapStoreGetMap::new(projection)
    }
}

impl<K, V, M, B> InspectableMapLikeItem<K, V, B> for MapStore<K, V, M>
where
    K: Send + 'static,
    V: Borrow<B> + 'static,
    B: ?Sized + 'static,
    M: MapOpsWithEntry<K, V, K>,
{
    type WithEntryHandler<'a, C, F, U> = MapStoreWithEntry<C, K, V, F, B, M>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&B>) -> U + Send + 'a;

    fn with_entry_handler<'a, C, F, U>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::WithEntryHandler<'a, C, F, U>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&B>) -> U + Send + 'a,
    {
        MapStoreWithEntry::new(projection, key, f)
    }
}

impl<K, V, M> MutableMapLikeItem<K, V> for MapStore<K, V, M>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Send + 'static,
    M: MapOps<K, V> + 'static,
{
    type UpdateHandler<C> = MapStoreUpdate<C, K, V, M>
    where
        C: AgentDescription + 'static;

    type RemoveHandler<C> = MapStoreRemove<C, K, V, M>
    where
        C: AgentDescription + 'static;

    type ClearHandler<C> = MapStoreClear<C, K, V, M>
    where
        C: AgentDescription + 'static;

    fn update_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
        value: V,
    ) -> Self::UpdateHandler<C> {
        MapStoreUpdate::new(projection, key, value)
    }

    fn remove_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
    ) -> Self::RemoveHandler<C> {
        MapStoreRemove::new(projection, key)
    }

    fn clear_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::ClearHandler<C> {
        MapStoreClear::new(projection)
    }

    type TransformEntryHandler<'a, C, F> = MapStoreTransformEntry<C, K, V, F, M>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&V>) -> Option<V> + Send + 'a;

    fn transform_entry_handler<'a, C, F>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::TransformEntryHandler<'a, C, F>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&V>) -> Option<V> + Send + 'a,
    {
        MapStoreTransformEntry::new(projection, key, f)
    }
}
