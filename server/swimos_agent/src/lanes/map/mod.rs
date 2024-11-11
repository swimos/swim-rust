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

use bytes::BytesMut;
use frunk::{Coprod, Coproduct};
use static_assertions::assert_impl_all;
use std::{
    borrow::Borrow, cell::RefCell, collections::HashMap, fmt::Debug, hash::Hash,
    marker::PhantomData,
};
use swimos_agent_protocol::{encoding::lane::MapLaneResponseEncoder, MapMessage};
use swimos_form::{read::RecognizerReadable, write::StructuralWritable};
use swimos_recon::parser::RecognizerDecoder;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

mod event;
pub mod lifecycle;

#[cfg(test)]
mod tests;

use crate::{
    agent_model::{AgentDescription, WriteResult},
    event_handler::{
        ActionContext, AndThen, EventHandlerError, HandlerAction, HandlerActionExt, HandlerTrans,
        Modification, StepResult,
    },
    item::{AgentItem, InspectableMapLikeItem, MapItem, MapLikeItem, MutableMapLikeItem},
    map_storage::{MapStoreInner, TransformEntryResult},
    meta::AgentMetadata,
};

use super::{queues::WriteQueues, Selector, SelectorFn};

pub use event::MapLaneEvent;

use super::{LaneItem, ProjTransform};

type Inner<K, V> = MapStoreInner<K, V, WriteQueues<K>>;

/// Model of a value lane. This maintains a sate consisting of a hash-map from keys to values. It generates an
/// event whenever the map is updated (updating the value for a key, removing a key or clearing the map).
///
/// TODO: This could be parameterized over the type of the hash (and potentially over the kind of the map,
/// potentially allowing a choice between hash and ordered maps).
#[derive(Debug)]
pub struct MapLane<K, V> {
    id: u64,
    inner: RefCell<Inner<K, V>>,
}

assert_impl_all!(MapLane<(), ()>: Send);

impl<K, V> MapLane<K, V> {
    /// # Arguments
    /// * `id` - The ID of the lane. This should be unique within an agent.
    /// * `init` - The initial contents of the map.
    pub fn new(id: u64, init: HashMap<K, V>) -> Self {
        MapLane {
            id,
            inner: RefCell::new(Inner::new(init)),
        }
    }
}

impl<K, V> AgentItem for MapLane<K, V> {
    fn id(&self) -> u64 {
        self.id
    }
}

impl<K, V> MapItem<K, V> for MapLane<K, V>
where
    K: Eq + Hash + Clone,
{
    fn init(&self, map: HashMap<K, V>) {
        self.inner.borrow_mut().init(map)
    }

    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V>>, &HashMap<K, V>) -> R,
    {
        self.inner.borrow_mut().read_with_prev(f)
    }
}

impl<K, V> MapLane<K, V>
where
    K: Clone + Eq + Hash,
{
    /// Update the value associated with a key.
    pub(crate) fn update(&self, key: K, value: V) {
        self.inner.borrow_mut().update(key, value)
    }

    /// Transform the value associated with a key.
    pub fn transform_entry<F>(&self, key: K, f: F) -> TransformEntryResult
    where
        F: FnOnce(Option<&V>) -> Option<V>,
    {
        self.inner.borrow_mut().transform_entry(key, f)
    }

    /// Remove and entry from the map.
    pub(crate) fn remove(&self, key: &K) {
        self.inner.borrow_mut().remove(key)
    }

    /// Clear the map.
    pub(crate) fn clear(&self) {
        self.inner.borrow_mut().clear()
    }

    /// Read a value from the map, if it exists.
    pub fn get<Q, F, R>(&self, key: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
        F: FnOnce(Option<&V>) -> R,
    {
        self.inner.borrow().with_entry(key, f)
    }

    /// Read the complete state of the map.
    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<K, V>) -> R,
    {
        self.inner.borrow().get_map(f)
    }

    /// Start a sync operation from the lane to the specified remote.
    pub(crate) fn sync(&self, id: Uuid) {
        let keys = self.get_map(|content| content.keys().cloned().collect());
        self.inner.borrow_mut().queue().sync(id, keys);
    }
}

impl<K, V> MapLane<K, V>
where
    K: Eq + Hash,
{
    pub fn with_entry<F, B, U>(&self, key: &K, f: F) -> U
    where
        B: ?Sized,
        V: Borrow<B>,
        F: FnOnce(Option<&B>) -> U,
    {
        self.inner.borrow().with_entry(key, f)
    }
}

const INFALLIBLE_SER: &str = "Serializing lane responses to recon should be infallible.";

impl<K, V> LaneItem for MapLane<K, V>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let mut encoder = MapLaneResponseEncoder::default();
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
pub struct MapLaneUpdate<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>,
    key_value: Option<(K, V)>,
}

impl<C, K, V> MapLaneUpdate<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>, key: K, value: V) -> Self {
        MapLaneUpdate {
            projection,
            key_value: Some((key, value)),
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapLaneUpdate<C, K, V>
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
        let MapLaneUpdate {
            projection,
            key_value,
        } = self;
        if let Some((key, value)) = key_value.take() {
            let lane = projection(context);
            lane.update(key, value);
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will remove an entry from the map.
pub struct MapLaneRemove<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>,
    key: Option<K>,
}

impl<C, K, V> MapLaneRemove<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>, key: K) -> Self {
        MapLaneRemove {
            projection,
            key: Some(key),
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapLaneRemove<C, K, V>
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
        let MapLaneRemove { projection, key } = self;
        if let Some(key) = key.take() {
            let lane = projection(context);
            lane.remove(&key);
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will clear the map.
pub struct MapLaneClear<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>,
    done: bool,
}

impl<C, K, V> MapLaneClear<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>) -> Self {
        MapLaneClear {
            projection,
            done: false,
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapLaneClear<C, K, V>
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
        let MapLaneClear { projection, done } = self;
        if !*done {
            *done = true;
            let lane = projection(context);
            lane.clear();
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will get an entry from the map.
pub struct MapLaneGet<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>,
    key: K,
    done: bool,
}

impl<C, K, V> MapLaneGet<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>, key: K) -> Self {
        MapLaneGet {
            projection,
            key,
            done: false,
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapLaneGet<C, K, V>
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
        let MapLaneGet {
            projection,
            key,
            done,
        } = self;
        if !*done {
            *done = true;
            let lane = projection(context);
            StepResult::done(lane.get(key, |v| v.cloned()))
        } else {
            StepResult::after_done()
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will read the entire state of a map lane.
pub struct MapLaneGetMap<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>,
    done: bool,
}

impl<C, K, V> MapLaneGetMap<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>) -> Self {
        MapLaneGetMap {
            projection,
            done: false,
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapLaneGetMap<C, K, V>
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
        let MapLaneGetMap { projection, done } = self;
        if !*done {
            *done = true;
            let lane = projection(context);
            StepResult::done(lane.get_map(Clone::clone))
        } else {
            StepResult::after_done()
        }
    }
}

impl<C, K, V, F, B, U> HandlerAction<C> for MapLaneWithEntry<C, K, V, F, B>
where
    K: Eq + Hash,
    B: ?Sized,
    V: Borrow<B>,
    F: FnOnce(Option<&B>) -> U,
{
    type Completion = U;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneWithEntry {
            projection,
            key_and_f,
            ..
        } = self;
        if let Some((key, f)) = key_and_f.take() {
            let lane = projection(context);
            StepResult::done(lane.with_entry(&key, f))
        } else {
            StepResult::after_done()
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will alter an entry in the map.
pub struct MapLaneWithEntry<C, K, V, F, B: ?Sized> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>,
    key_and_f: Option<(K, F)>,
    _type: PhantomData<fn(&B)>,
}

impl<C, K, V, F, B: ?Sized> MapLaneWithEntry<C, K, V, F, B> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `key` - Key of the entry.
    /// * `f` - The closure to apply to the entry.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>, key: K, f: F) -> Self {
        MapLaneWithEntry {
            projection,
            key_and_f: Some((key, f)),
            _type: PhantomData,
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will request a sync from the lane.
pub struct MapLaneSync<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>,
    id: Option<Uuid>,
}

impl<C, K, V> MapLaneSync<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>, id: Uuid) -> Self {
        MapLaneSync {
            projection,
            id: Some(id),
        }
    }
}

impl<C, K, V> HandlerAction<C> for MapLaneSync<C, K, V>
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
        let MapLaneSync { projection, id } = self;
        if let Some(id) = id.take() {
            let lane = projection(context);
            lane.sync(id);
            StepResult::Complete {
                modified_item: Some(Modification::no_trigger(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

type MapLaneHandler<C, K, V> = Coprod!(
    MapLaneUpdate<C, K, V>,
    MapLaneRemove<C, K, V>,
    MapLaneClear<C, K, V>,
);

impl<C, K, V> HandlerTrans<MapMessage<K, V>> for ProjTransform<C, MapLane<K, V>> {
    type Out = MapLaneHandler<C, K, V>;

    fn transform(self, input: MapMessage<K, V>) -> Self::Out {
        let ProjTransform { projection } = self;
        match input {
            MapMessage::Update { key, value } => {
                Coproduct::Inl(MapLaneUpdate::new(projection, key, value))
            }
            MapMessage::Remove { key } => {
                Coproduct::Inr(Coproduct::Inl(MapLaneRemove::new(projection, key)))
            }
            MapMessage::Clear => Coproduct::Inr(Coproduct::Inr(Coproduct::Inl(MapLaneClear::new(
                projection,
            )))),
            _ => {
                todo!("Drop and take not yet implemented.")
            }
        }
    }
}

pub struct DecodeMapMessage<K, V> {
    _target_type: PhantomData<fn() -> MapMessage<K, V>>,
    message: Option<MapMessage<BytesMut, BytesMut>>,
}

impl<K, V> DecodeMapMessage<K, V> {
    pub fn new(message: MapMessage<BytesMut, BytesMut>) -> Self {
        DecodeMapMessage {
            _target_type: Default::default(),
            message: Some(message),
        }
    }
}

//TODO: The decoders should be shifted elsewhere so they don't need constantly recreating.
fn try_decode<T: RecognizerReadable>(mut buffer: BytesMut) -> Result<T, EventHandlerError> {
    let mut decoder = RecognizerDecoder::new(T::make_recognizer());
    match decoder.decode_eof(&mut buffer) {
        Ok(Some(value)) => Ok(value),
        Ok(_) => Err(EventHandlerError::IncompleteCommand),
        Err(e) => Err(EventHandlerError::BadCommand(e)),
    }
}

impl<K, V, Context> HandlerAction<Context> for DecodeMapMessage<K, V>
where
    K: RecognizerReadable,
    V: RecognizerReadable,
{
    type Completion = MapMessage<K, V>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let DecodeMapMessage { message, .. } = self;
        if let Some(message) = message.take() {
            match message {
                MapMessage::Update { key, value } => {
                    match try_decode::<K>(key).and_then(|k| {
                        try_decode::<V>(value).map(|v| (MapMessage::Update { key: k, value: v }))
                    }) {
                        Ok(msg) => StepResult::done(msg),
                        Err(e) => StepResult::Fail(e),
                    }
                }
                MapMessage::Remove { key } => match try_decode::<K>(key) {
                    Ok(k) => StepResult::done(MapMessage::Remove { key: k }),
                    Err(e) => StepResult::Fail(e),
                },
                MapMessage::Clear => StepResult::done(MapMessage::Clear),
                MapMessage::Take(n) => StepResult::done(MapMessage::Take(n)),
                MapMessage::Drop(n) => StepResult::done(MapMessage::Drop(n)),
            }
        } else {
            StepResult::after_done()
        }
    }
}

pub type DecodeAndApply<C, K, V> =
    AndThen<DecodeMapMessage<K, V>, MapLaneHandler<C, K, V>, ProjTransform<C, MapLane<K, V>>>;

/// Create an event handler that will decode an incoming map message and apply the value into a map lane.
pub fn decode_and_apply<C, K, V>(
    message: MapMessage<BytesMut, BytesMut>,
    projection: fn(&C) -> &MapLane<K, V>,
) -> DecodeAndApply<C, K, V>
where
    K: Clone + Eq + Hash + RecognizerReadable,
    V: RecognizerReadable,
{
    let decode: DecodeMapMessage<K, V> = DecodeMapMessage::new(message);
    decode.and_then(ProjTransform::new(projection))
}

/// An (event handler)[`crate::event_handler::EventHandler`] that will alter an entry in the map.
pub struct MapLaneTransformEntry<C, K, V, F> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>,
    key_and_f: Option<(K, F)>,
}

impl<C, K, V, F> MapLaneTransformEntry<C, K, V, F> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V>, key: K, f: F) -> Self {
        MapLaneTransformEntry {
            projection,
            key_and_f: Some((key, f)),
        }
    }
}

impl<C, K, V, F> HandlerAction<C> for MapLaneTransformEntry<C, K, V, F>
where
    K: Clone + Eq + Hash,
    F: FnOnce(Option<&V>) -> Option<V>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneTransformEntry {
            projection,
            key_and_f,
        } = self;
        if let Some((key, f)) = key_and_f.take() {
            let lane = projection(context);
            if matches!(lane.transform_entry(key, f), TransformEntryResult::NoChange) {
                StepResult::done(())
            } else {
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id())),
                    result: (),
                }
            }
        } else {
            StepResult::after_done()
        }
    }
}

impl<K, V> MapLikeItem<K, V> for MapLane<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + 'static,
{
    type GetHandler<C> = MapLaneGet<C, K, V>
    where
        C: AgentDescription + 'static;

    fn get_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
    ) -> Self::GetHandler<C> {
        MapLaneGet::new(projection, key)
    }

    type GetMapHandler<C> = MapLaneGetMap<C, K, V>
    where
        C: AgentDescription + 'static;

    fn get_map_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::GetMapHandler<C> {
        MapLaneGetMap::new(projection)
    }
}

impl<K, V> InspectableMapLikeItem<K, V> for MapLane<K, V>
where
    K: Eq + Hash + Send + 'static,
    V: 'static,
{
    type WithEntryHandler<'a, C, F, B, U> = MapLaneWithEntry<C, K, V, F, B>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        B: ?Sized +'static,
        V: Borrow<B>,
        F: FnOnce(Option<&B>) -> U + Send + 'a;

    fn with_entry_handler<'a, C, F, B, U>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::WithEntryHandler<'a, C, F, B, U>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        B: ?Sized + 'static,
        V: Borrow<B>,
        F: FnOnce(Option<&B>) -> U + Send + 'a,
    {
        MapLaneWithEntry::new(projection, key, f)
    }
}

impl<K, V> MutableMapLikeItem<K, V> for MapLane<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Send + 'static,
{
    type UpdateHandler<C> = MapLaneUpdate<C, K, V>
    where
        C: AgentDescription + 'static;

    type RemoveHandler<C> = MapLaneRemove<C, K, V>
    where
        C: AgentDescription + 'static;

    type ClearHandler<C> = MapLaneClear<C, K, V>
    where
        C: AgentDescription + 'static;

    fn update_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
        value: V,
    ) -> Self::UpdateHandler<C> {
        MapLaneUpdate::new(projection, key, value)
    }

    fn remove_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
    ) -> Self::RemoveHandler<C> {
        MapLaneRemove::new(projection, key)
    }

    fn clear_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::ClearHandler<C> {
        MapLaneClear::new(projection)
    }

    type TransformEntryHandler<'a, C, F> = MapLaneTransformEntry<C, K, V, F>
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
        MapLaneTransformEntry::new(projection, key, f)
    }
}

/// An [event handler](crate::event_handler::EventHandler) that attempts to update an entry in a map lane, if
/// that lane exists.
pub struct MapLaneSelectUpdate<C, K, V, F> {
    _type: PhantomData<fn(&C)>,
    projection_key_value: Option<(F, K, V)>,
}

impl<C, K: Debug, V: Debug, F: Debug> Debug for MapLaneSelectUpdate<C, K, V, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapLaneSelectUpdate")
            .field("projection_key_value", &self.projection_key_value)
            .finish()
    }
}

impl<C, K, V, F> MapLaneSelectUpdate<C, K, V, F> {
    /// # Arguments
    /// * `projection` - A projection from the agent type onto an (optional) map lane.
    /// * `key` - The key of the entry to update.
    /// * `value` - The new value for the entry.
    pub fn new(projection: F, key: K, value: V) -> Self {
        MapLaneSelectUpdate {
            _type: PhantomData,
            projection_key_value: Some((projection, key, value)),
        }
    }
}

impl<C, K, V, F> HandlerAction<C> for MapLaneSelectUpdate<C, K, V, F>
where
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V>>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneSelectUpdate {
            projection_key_value,
            ..
        } = self;
        if let Some((projection, key, value)) = projection_key_value.take() {
            let selector = projection.selector(context);
            if let Some(lane) = selector.select() {
                lane.update(key, value);
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id)),
                    result: (),
                }
            } else {
                StepResult::Fail(EventHandlerError::LaneNotFound(selector.name().to_string()))
            }
        } else {
            StepResult::after_done()
        }
    }
}

/// An [event handler](crate::event_handler::EventHandler) that attempts to remove an entry from a map lane, if
/// that lane exists.
pub struct MapLaneSelectRemove<C, K, V, F> {
    _type: PhantomData<fn(&C, &V)>,
    projection_key: Option<(F, K)>,
}

impl<C, K: Debug, V: Debug, F: Debug> Debug for MapLaneSelectRemove<C, K, V, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapLaneSelectRemove")
            .field("projection_key", &self.projection_key)
            .finish()
    }
}

impl<C, K, V, F> MapLaneSelectRemove<C, K, V, F> {
    /// # Arguments
    /// * `projection` - A projection from the agent type onto an (optional) map lane.
    /// * `key` - The key of the entry to remove.
    pub fn new(projection: F, key: K) -> Self {
        MapLaneSelectRemove {
            _type: PhantomData,
            projection_key: Some((projection, key)),
        }
    }
}

impl<C, K, V, F> HandlerAction<C> for MapLaneSelectRemove<C, K, V, F>
where
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V>>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneSelectRemove { projection_key, .. } = self;
        if let Some((projection, key)) = projection_key.take() {
            let selector = projection.selector(context);
            if let Some(lane) = selector.select() {
                lane.remove(&key);
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id)),
                    result: (),
                }
            } else {
                StepResult::Fail(EventHandlerError::LaneNotFound(selector.name().to_string()))
            }
        } else {
            StepResult::after_done()
        }
    }
}

/// An [event handler](crate::event_handler::EventHandler) that attempts to clear a map lane, if
/// that lane exists.
pub struct MapLaneSelectClear<C, K, V, F> {
    _type: PhantomData<fn(&C, &K, &V)>,
    projection: Option<F>,
}

impl<C, K, V, F: Debug> Debug for MapLaneSelectClear<C, K, V, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapLaneSelectClear")
            .field("projection", &self.projection)
            .finish()
    }
}

impl<C, K, V, F> MapLaneSelectClear<C, K, V, F> {
    /// #Arguments
    /// * `projection` - A projection from the agent type onto an (optional) map lane.
    pub fn new(projection: F) -> Self {
        MapLaneSelectClear {
            _type: PhantomData,
            projection: Some(projection),
        }
    }
}

impl<C, K, V, F> HandlerAction<C> for MapLaneSelectClear<C, K, V, F>
where
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V>>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneSelectClear { projection, .. } = self;
        if let Some(projection) = projection.take() {
            let selector = projection.selector(context);
            if let Some(lane) = selector.select() {
                lane.clear();
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id)),
                    result: (),
                }
            } else {
                StepResult::Fail(EventHandlerError::LaneNotFound(selector.name().to_string()))
            }
        } else {
            StepResult::after_done()
        }
    }
}

#[derive(Default)]
#[doc(hidden)]
pub enum DecodeAndSelectApply<C, K, V, F> {
    Decoding(DecodeMapMessage<K, V>, F),
    Updating(MapLaneSelectUpdate<C, K, V, F>),
    Removing(MapLaneSelectRemove<C, K, V, F>),
    Clearing(MapLaneSelectClear<C, K, V, F>),
    #[default]
    Done,
}

impl<C, K, V, F> HandlerAction<C> for DecodeAndSelectApply<C, K, V, F>
where
    K: RecognizerReadable + Clone + Eq + Hash,
    V: RecognizerReadable,
    F: SelectorFn<C, Target = MapLane<K, V>>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<C>,
        meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            DecodeAndSelectApply::Decoding(mut decoding, selector) => {
                match decoding.step(action_context, meta, context) {
                    StepResult::Continue { modified_item } => {
                        *self = DecodeAndSelectApply::Decoding(decoding, selector);
                        StepResult::Continue { modified_item }
                    }
                    StepResult::Fail(err) => StepResult::Fail(err),
                    StepResult::Complete {
                        modified_item,
                        result,
                    } => {
                        match result {
                            MapMessage::Update { key, value } => {
                                *self = DecodeAndSelectApply::Updating(MapLaneSelectUpdate::new(
                                    selector, key, value,
                                ));
                            }
                            MapMessage::Remove { key } => {
                                *self = DecodeAndSelectApply::Removing(MapLaneSelectRemove::new(
                                    selector, key,
                                ));
                            }
                            MapMessage::Clear => {
                                *self = DecodeAndSelectApply::Clearing(MapLaneSelectClear::new(
                                    selector,
                                ));
                            }
                            _ => {
                                todo!("Drop and take not yet implemented.")
                            }
                        }

                        StepResult::Continue { modified_item }
                    }
                }
            }
            DecodeAndSelectApply::Updating(mut selector) => {
                let result = selector.step(action_context, meta, context);
                if !result.is_cont() {
                    *self = DecodeAndSelectApply::Done;
                }
                result
            }
            DecodeAndSelectApply::Removing(mut selector) => {
                let result = selector.step(action_context, meta, context);
                if !result.is_cont() {
                    *self = DecodeAndSelectApply::Done;
                }
                result
            }
            DecodeAndSelectApply::Clearing(mut selector) => {
                let result = selector.step(action_context, meta, context);
                if !result.is_cont() {
                    *self = DecodeAndSelectApply::Done;
                }
                result
            }
            DecodeAndSelectApply::Done => StepResult::after_done(),
        }
    }
}

/// Create an event handler that will decode an incoming map message and apply the value into a map lane.
pub fn decode_and_select_apply<C, K, V, F>(
    message: MapMessage<BytesMut, BytesMut>,
    projection: F,
) -> DecodeAndSelectApply<C, K, V, F>
where
    K: Clone + Eq + Hash + RecognizerReadable,
    V: RecognizerReadable,
    F: SelectorFn<C, Target = MapLane<K, V>>,
{
    let decode: DecodeMapMessage<K, V> = DecodeMapMessage::new(message);
    DecodeAndSelectApply::Decoding(decode, projection)
}

type SelectType<C, K, V> = fn(&C) -> (&K, &V);

/// An [event handler](crate::event_handler::EventHandler) that will request a sync from a map lane
/// that might not exist.
pub struct MapLaneSelectSync<C, K, V, F> {
    _type: PhantomData<SelectType<C, K, V>>,
    projection_id: Option<(F, Uuid)>,
}

impl<C, K, V, F> MapLaneSelectSync<C, K, V, F> {
    /// # Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `id` - The ID of the remote that requested the sync.
    pub fn new(projection: F, id: Uuid) -> Self {
        MapLaneSelectSync {
            _type: PhantomData,
            projection_id: Some((projection, id)),
        }
    }
}

impl<C, K, V, F> HandlerAction<C> for MapLaneSelectSync<C, K, V, F>
where
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V>>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneSelectSync { projection_id, .. } = self;
        if let Some((projection, id)) = projection_id.take() {
            let selector = projection.selector(context);
            if let Some(lane) = selector.select() {
                lane.sync(id);
                StepResult::Complete {
                    modified_item: Some(Modification::no_trigger(lane.id())),
                    result: (),
                }
            } else {
                StepResult::Fail(EventHandlerError::LaneNotFound(selector.name().to_string()))
            }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }
}
