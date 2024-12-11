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
use frunk::Coprod;
use static_assertions::assert_impl_all;
use std::{
    any::type_name,
    borrow::Borrow,
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt::{Debug, Formatter},
    hash::Hash,
    marker::PhantomData,
};
use swimos_agent_protocol::{encoding::lane::MapLaneResponseEncoder, MapMessage};
use swimos_form::{read::RecognizerReadable, write::StructuralWritable, Form};
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
        ActionContext, AndThen, Described, EventHandlerError, HandlerAction, HandlerActionExt,
        HandlerTrans, Modification, StepResult,
    },
    item::{AgentItem, InspectableMapLikeItem, MapItem, MapLikeItem, MutableMapLikeItem},
    map_storage::{
        drop_or_take, DropOrTake, MapOps, MapOpsWithEntry, MapStoreInner, TransformEntryResult,
    },
    meta::AgentMetadata,
};

use super::{queues::WriteQueues, Selector, SelectorFn};

pub use event::MapLaneEvent;

use super::{LaneItem, ProjTransform};

type Inner<K, V, M> = MapStoreInner<K, V, WriteQueues<K>, M>;

/// Model of a value lane. This maintains a sate consisting of a hash-map from keys to values. It generates an
/// event whenever the map is updated (updating the value for a key, removing a key or clearing the map).
#[derive(Debug)]
pub struct MapLane<K, V, M = HashMap<K, V>> {
    id: u64,
    inner: RefCell<Inner<K, V, M>>,
}

assert_impl_all!(MapLane<(), ()>: Send);

impl<K, V, M> MapLane<K, V, M> {
    /// # Arguments
    /// * `id` - The ID of the lane. This should be unique within an agent.
    /// * `init` - The initial contents of the map.
    pub fn new(id: u64, init: M) -> Self {
        MapLane {
            id,
            inner: RefCell::new(Inner::new(init)),
        }
    }
}

impl<K, V, M> AgentItem for MapLane<K, V, M> {
    fn id(&self) -> u64 {
        self.id
    }
}

impl<K, V, M> MapItem<K, V, M> for MapLane<K, V, M>
where
    K: Eq + Hash + Clone,
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

impl<K, V, M> MapLane<K, V, M>
where
    K: Clone + Eq + Hash,
    M: MapOps<K, V>,
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
        M: MapOpsWithEntry<K, V, Q>,
    {
        self.inner.borrow().with_entry(key, f)
    }

    /// Read the complete state of the map.
    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&M) -> R,
    {
        self.inner.borrow().get_map(f)
    }

    /// Start a sync operation from the lane to the specified remote.
    pub(crate) fn sync(&self, id: Uuid) {
        let keys = self.get_map(|content| content.keys().cloned().collect());
        self.inner.borrow_mut().queue().sync(id, keys);
    }
}

impl<K, V, M> MapLane<K, V, M> {
    pub fn with_entry<F, B, U>(&self, key: &K, f: F) -> U
    where
        B: ?Sized,
        V: Borrow<B>,
        F: FnOnce(Option<&B>) -> U,
        M: MapOpsWithEntry<K, V, K>,
    {
        self.inner.borrow().with_entry(key, f)
    }
}

const INFALLIBLE_SER: &str = "Serializing lane responses to recon should be infallible.";

impl<K, V, M> LaneItem for MapLane<K, V, M>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
    M: MapOps<K, V>,
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
pub struct MapLaneUpdate<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    key_value: Option<(K, V)>,
}

impl<C, K, V, M> MapLaneUpdate<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>, key: K, value: V) -> Self {
        MapLaneUpdate {
            projection,
            key_value: Some((key, value)),
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapLaneUpdate<C, K, V, M>
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneUpdate {
            projection,
            key_value,
        } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapLaneUpdate")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &key_value.is_none())
            .finish()
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will remove an entry from the map.
pub struct MapLaneRemove<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    key: Option<K>,
}

impl<C, K, V, M> MapLaneRemove<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>, key: K) -> Self {
        MapLaneRemove {
            projection,
            key: Some(key),
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapLaneRemove<C, K, V, M>
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneRemove { projection, key } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapLaneRemove")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &key.is_none())
            .finish()
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will clear the map.
pub struct MapLaneClear<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    done: bool,
}

impl<C, K, V, M> MapLaneClear<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>) -> Self {
        MapLaneClear {
            projection,
            done: false,
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapLaneClear<C, K, V, M>
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneClear { projection, done } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapLaneClear")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", done)
            .finish()
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will get an entry from the map.
pub struct MapLaneGet<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    key: K,
    done: bool,
}

impl<C, K, V, M> MapLaneGet<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>, key: K) -> Self {
        MapLaneGet {
            projection,
            key,
            done: false,
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapLaneGet<C, K, V, M>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapLaneGet")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &self.done)
            .finish()
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will read the entire state of a map lane.
pub struct MapLaneGetMap<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    done: bool,
}

impl<C, K, V, M> MapLaneGetMap<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>) -> Self {
        MapLaneGetMap {
            projection,
            done: false,
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapLaneGetMap<C, K, V, M>
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
        let MapLaneGetMap { projection, done } = self;
        if !*done {
            *done = true;
            let lane = projection(context);
            StepResult::done(lane.get_map(Clone::clone))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapLaneGetMap")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &self.done)
            .finish()
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will alter an entry in the map.
pub struct MapLaneWithEntry<C, K, V, F, B: ?Sized, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    key_and_f: Option<(K, F)>,
    _type: PhantomData<fn(&B)>,
}

impl<C, K, V, F, B: ?Sized, M> MapLaneWithEntry<C, K, V, F, B, M> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `key` - Key of the entry.
    /// * `f` - The closure to apply to the entry.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>, key: K, f: F) -> Self {
        MapLaneWithEntry {
            projection,
            key_and_f: Some((key, f)),
            _type: PhantomData,
        }
    }
}

impl<C, K, V, F, B, U, M> HandlerAction<C> for MapLaneWithEntry<C, K, V, F, B, M>
where
    C: AgentDescription,
    K: Eq + Hash,
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapLaneWithEntry")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("result_type", &type_name::<U>())
            .field("consumed", &self.key_and_f.is_none())
            .finish()
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will request a sync from the lane.
pub struct MapLaneSync<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    id: Option<Uuid>,
}

impl<C, K, V, M> MapLaneSync<C, K, V, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>, id: Uuid) -> Self {
        MapLaneSync {
            projection,
            id: Some(id),
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapLaneSync<C, K, V, M>
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneSync { projection, id } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapLaneSync")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("sync_id", &id)
            .finish()
    }
}

type MapLaneHandler<C, K, V, M = HashMap<K, V>> = Coprod!(
    MapLaneUpdate<C, K, V, M>,
    MapLaneRemove<C, K, V, M>,
    MapLaneClear<C, K, V, M>,
    MapLaneDropOrTake<C, K, V, M>,
);

impl<C, K, V, M> HandlerTrans<MapMessage<K, V>> for ProjTransform<C, MapLane<K, V, M>> {
    type Out = MapLaneHandler<C, K, V, M>;

    fn transform(self, input: MapMessage<K, V>) -> Self::Out {
        let ProjTransform { projection } = self;
        match input {
            MapMessage::Update { key, value } => {
                MapLaneHandler::inject(MapLaneUpdate::new(projection, key, value))
            }
            MapMessage::Remove { key } => {
                MapLaneHandler::inject(MapLaneRemove::new(projection, key))
            }
            MapMessage::Clear => MapLaneHandler::inject(MapLaneClear::new(projection)),
            MapMessage::Drop(n) => {
                MapLaneHandler::inject(MapLaneDropOrTake::new(projection, DropOrTake::Drop, n))
            }
            MapMessage::Take(n) => {
                MapLaneHandler::inject(MapLaneDropOrTake::new(projection, DropOrTake::Take, n))
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
    Context: AgentDescription,
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

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let DecodeMapMessage { message, .. } = self;
        let content = message.as_ref().map(|msg| match msg {
            MapMessage::Update { key, value } => {
                let key_str = std::str::from_utf8(key.as_ref()).unwrap_or("<<BAD UTF8>>");
                let value_str = std::str::from_utf8(value.as_ref()).unwrap_or("<<BAD UTF8>>");
                MapMessage::Update {
                    key: key_str,
                    value: value_str,
                }
            }
            MapMessage::Remove { key } => {
                let key_str = std::str::from_utf8(key.as_ref()).unwrap_or("<<BAD UTF8>>");
                MapMessage::Remove { key: key_str }
            }
            MapMessage::Clear => MapMessage::Clear,
            MapMessage::Take(n) => MapMessage::Take(*n),
            MapMessage::Drop(n) => MapMessage::Drop(*n),
        });
        f.debug_struct("Decode")
            .field("key_type", &type_name::<K>())
            .field("value_type", &type_name::<V>())
            .field("content", &content)
            .finish()
    }
}

pub type DecodeAndApply<C, K, V, M = HashMap<K, V>> =
    AndThen<DecodeMapMessage<K, V>, MapLaneHandler<C, K, V, M>, ProjTransform<C, MapLane<K, V, M>>>;

/// Create an event handler that will decode an incoming map message and apply the value into a map lane.
pub fn decode_and_apply<C, K, V, M>(
    message: MapMessage<BytesMut, BytesMut>,
    projection: fn(&C) -> &MapLane<K, V, M>,
) -> DecodeAndApply<C, K, V, M>
where
    C: AgentDescription,
    K: Form + Clone + Eq + Hash,
    V: RecognizerReadable,
    M: MapOps<K, V>,
{
    let decode: DecodeMapMessage<K, V> = DecodeMapMessage::new(message);
    decode.and_then(ProjTransform::new(projection))
}

/// An (event handler)[`crate::event_handler::EventHandler`] that will alter an entry in the map.
pub struct MapLaneTransformEntry<C, K, V, F, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    key_and_f: Option<(K, F)>,
}

impl<C, K, V, F, M> MapLaneTransformEntry<C, K, V, F, M> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>, key: K, f: F) -> Self {
        MapLaneTransformEntry {
            projection,
            key_and_f: Some((key, f)),
        }
    }
}

impl<C, K, V, F, M> HandlerAction<C> for MapLaneTransformEntry<C, K, V, F, M>
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

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneTransformEntry {
            projection,
            key_and_f,
        } = self;
        let lane = projection(context);
        let name = context.item_name(lane.id());

        f.debug_struct("MapLaneTransformEntry")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &key_and_f.is_none())
            .finish()
    }
}

impl<K, V, M> MapLikeItem<K, V, M> for MapLane<K, V, M>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + 'static,
    M: MapOpsWithEntry<K, V, K> + Clone + 'static,
{
    type GetHandler<C> = MapLaneGet<C, K, V, M>
    where
        C: AgentDescription + 'static;

    fn get_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
    ) -> Self::GetHandler<C> {
        MapLaneGet::new(projection, key)
    }

    type GetMapHandler<C> = MapLaneGetMap<C, K, V, M>
    where
        C: AgentDescription + 'static;

    fn get_map_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::GetMapHandler<C> {
        MapLaneGetMap::new(projection)
    }
}

impl<K, V, B, M> InspectableMapLikeItem<K, V, B> for MapLane<K, V, M>
where
    K: Eq + Hash + Send + 'static,
    V: Borrow<B> + 'static,
    B: ?Sized + 'static,
    M: MapOpsWithEntry<K, V, K>,
{
    type WithEntryHandler<'a, C, F, U> = MapLaneWithEntry<C, K, V, F, B, M>
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
        MapLaneWithEntry::new(projection, key, f)
    }
}

impl<K, V, M> MutableMapLikeItem<K, V> for MapLane<K, V, M>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Send + 'static,
    M: MapOps<K, V> + 'static,
{
    type UpdateHandler<C> = MapLaneUpdate<C, K, V, M>
    where
        C: AgentDescription + 'static;

    type RemoveHandler<C> = MapLaneRemove<C, K, V, M>
    where
        C: AgentDescription + 'static;

    type ClearHandler<C> = MapLaneClear<C, K, V, M>
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

    type TransformEntryHandler<'a, C, F> = MapLaneTransformEntry<C, K, V, F, M>
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl<C, K, V, F, M> HandlerAction<C> for MapLaneSelectUpdate<C, K, V, F>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V, M>>,
    M: MapOps<K, V>,
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

    fn describe(&self, _context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneSelectUpdate {
            projection_key_value,
            ..
        } = self;
        if let Some((projection, _, _)) = projection_key_value {
            f.debug_struct("MapLaneSelectUpdate")
                .field("lane_name", &projection.name())
                .field("key_type", &type_name::<K>())
                .field("value_type", &type_name::<V>())
                .field("consumed", &false)
                .finish()
        } else {
            f.debug_struct("MapLaneSelectUpdate")
                .field("key_type", &type_name::<K>())
                .field("value_type", &type_name::<V>())
                .field("consumed", &true)
                .finish()
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl<C, K, V, F, M> HandlerAction<C> for MapLaneSelectRemove<C, K, V, F>
where
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V, M>>,
    M: MapOps<K, V>,
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

    fn describe(&self, _context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneSelectRemove { projection_key, .. } = self;
        if let Some((projection, _)) = projection_key {
            f.debug_struct("MapLaneSelectRemove")
                .field("lane_name", &projection.name())
                .field("key_type", &type_name::<K>())
                .field("value_type", &type_name::<V>())
                .field("consumed", &false)
                .finish()
        } else {
            f.debug_struct("MapLaneSelectRemove")
                .field("key_type", &type_name::<K>())
                .field("value_type", &type_name::<V>())
                .field("consumed", &true)
                .finish()
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl<C, K, V, F, M> HandlerAction<C> for MapLaneSelectClear<C, K, V, F>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V, M>>,
    M: MapOps<K, V>,
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

    fn describe(&self, _context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneSelectClear { projection, .. } = self;
        if let Some(projection) = projection {
            f.debug_struct("MapLaneSelectClear")
                .field("lane_name", &projection.name())
                .field("key_type", &type_name::<K>())
                .field("value_type", &type_name::<V>())
                .field("consumed", &false)
                .finish()
        } else {
            f.debug_struct("MapLaneSelectClear")
                .field("key_type", &type_name::<K>())
                .field("value_type", &type_name::<V>())
                .field("consumed", &true)
                .finish()
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
    DroppingOrTaking(MapLaneSelectDropOrTake<C, K, V, F>),
    #[default]
    Done,
}

impl<C, K, V, F, M> HandlerAction<C> for DecodeAndSelectApply<C, K, V, F>
where
    C: AgentDescription,
    K: Form + Clone + Eq + Hash,
    V: RecognizerReadable,
    F: SelectorFn<C, Target = MapLane<K, V, M>>,
    M: MapOps<K, V>,
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
                            MapMessage::Drop(n) => {
                                *self = DecodeAndSelectApply::DroppingOrTaking(
                                    MapLaneSelectDropOrTake::new(selector, DropOrTake::Drop, n),
                                );
                            }
                            MapMessage::Take(n) => {
                                *self = DecodeAndSelectApply::DroppingOrTaking(
                                    MapLaneSelectDropOrTake::new(selector, DropOrTake::Take, n),
                                );
                            }
                        }
                        StepResult::Continue { modified_item }
                    }
                }
            }
            DecodeAndSelectApply::Updating(mut selector) => {
                let result = selector.step(action_context, meta, context);
                if result.is_cont() {
                    *self = DecodeAndSelectApply::Updating(selector);
                }
                result
            }
            DecodeAndSelectApply::Removing(mut selector) => {
                let result = selector.step(action_context, meta, context);
                if result.is_cont() {
                    *self = DecodeAndSelectApply::Removing(selector);
                }
                result
            }
            DecodeAndSelectApply::Clearing(mut selector) => {
                let result = selector.step(action_context, meta, context);
                if result.is_cont() {
                    *self = DecodeAndSelectApply::Clearing(selector);
                }
                result
            }
            DecodeAndSelectApply::DroppingOrTaking(mut selector) => {
                let result = selector.step(action_context, meta, context);
                if result.is_cont() {
                    *self = DecodeAndSelectApply::DroppingOrTaking(selector);
                }
                result
            }
            DecodeAndSelectApply::Done => StepResult::after_done(),
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            DecodeAndSelectApply::Decoding(decode_map_message, proj) => f
                .debug_struct("DecodeAndSelectApply")
                .field("state", &"Decoding")
                .field("decoder", &Described::new(context, decode_map_message))
                .field("lane_name", &proj.name())
                .finish(),
            DecodeAndSelectApply::Updating(selector) => f
                .debug_struct("DecodeAndSelectApply")
                .field("state", &"Updating")
                .field("selector", &Described::new(context, selector))
                .finish(),
            DecodeAndSelectApply::Removing(selector) => f
                .debug_struct("DecodeAndSelectApply")
                .field("state", &"Removing")
                .field("selector", &Described::new(context, selector))
                .finish(),
            DecodeAndSelectApply::Clearing(selector) => f
                .debug_struct("DecodeAndSelectApply")
                .field("state", &"Clearing")
                .field("selector", &Described::new(context, selector))
                .finish(),
            DecodeAndSelectApply::DroppingOrTaking(selector) => f
                .debug_struct("DecodeAndSelectApply")
                .field("state", &"DroppingOrTaking")
                .field("selector", &Described::new(context, selector))
                .finish(),
            DecodeAndSelectApply::Done => f
                .debug_tuple("DecodeAndSelectSet")
                .field(&"<<CONSUMED>>")
                .finish(),
        }
    }
}

/// Create an event handler that will decode an incoming map message and apply the value into a map lane.
pub fn decode_and_select_apply<C, K, V, M, F>(
    message: MapMessage<BytesMut, BytesMut>,
    projection: F,
) -> DecodeAndSelectApply<C, K, V, F>
where
    K: Clone + Eq + Hash + RecognizerReadable,
    V: RecognizerReadable,
    F: SelectorFn<C, Target = MapLane<K, V, M>>,
    M: MapOps<K, V>,
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

impl<C, K, V, F, M> HandlerAction<C> for MapLaneSelectSync<C, K, V, F>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V, M>>,
    M: MapOps<K, V>,
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

    fn describe(&self, _context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneSelectSync { projection_id, .. } = self;
        if let Some((proj, id)) = projection_id {
            f.debug_struct("MapLaneSelectSync")
                .field("lane_name", &proj.name())
                .field("sync_id", id)
                .finish()
        } else {
            f.debug_tuple("MapLaneSelectSync")
                .field(&"<<CONSUMED>>")
                .finish()
        }
    }
}

struct MapLaneRemoveMultiple<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    keys: Option<VecDeque<K>>,
    current: Option<MapLaneRemove<C, K, V, M>>,
}

impl<C, K, V, M> MapLaneRemoveMultiple<C, K, V, M> {
    fn new(projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>, keys: VecDeque<K>) -> Self {
        MapLaneRemoveMultiple {
            projection,
            keys: Some(keys),
            current: None,
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapLaneRemoveMultiple<C, K, V, M>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
    M: MapOps<K, V>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<C>,
        meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneRemoveMultiple {
            projection,
            keys,
            current,
        } = self;
        if let Some(key_queue) = keys {
            let h = if let Some(h) = current {
                h
            } else if let Some(next) = key_queue.pop_front() {
                current.insert(MapLaneRemove::new(*projection, next))
            } else {
                *keys = None;
                return StepResult::done(());
            };
            match h.step(action_context, meta, context) {
                StepResult::Continue { modified_item } => StepResult::Continue { modified_item },
                StepResult::Fail(err) => StepResult::Fail(err),
                StepResult::Complete { modified_item, .. } => {
                    *current = None;
                    StepResult::Continue { modified_item }
                }
            }
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneRemoveMultiple {
            projection, keys, ..
        } = self;
        let lane = projection(context);
        let name = context.item_name(lane.id());
        f.debug_struct("MapLaneRemoveMultiple")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("keys", &keys.as_ref().map(|v| v.len()))
            .field("consumed", &keys.is_none())
            .finish()
    }
}

enum DropOrTakeState<C, K, V, M> {
    Init,
    Removing(MapLaneRemoveMultiple<C, K, V, M>),
}

/// An [event handler](crate::event_handler::EventHandler)`] that will either retain or drop the first `n` elements
/// from a map (ordering the keys by the ordering of their Recon model representations).
pub struct MapLaneDropOrTake<C, K, V, M = HashMap<K, V>> {
    projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
    kind: DropOrTake,
    number: u64,
    state: DropOrTakeState<C, K, V, M>,
}

impl<C, K, V, M> MapLaneDropOrTake<C, K, V, M> {
    fn new(
        projection: for<'a> fn(&'a C) -> &'a MapLane<K, V, M>,
        kind: DropOrTake,
        number: u64,
    ) -> Self {
        MapLaneDropOrTake {
            projection,
            kind,
            number,
            state: DropOrTakeState::Init,
        }
    }
}

impl<C, K, V, M> HandlerAction<C> for MapLaneDropOrTake<C, K, V, M>
where
    C: AgentDescription,
    K: StructuralWritable + Clone + Eq + Hash,
    M: MapOps<K, V>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<C>,
        meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneDropOrTake {
            projection,
            kind,
            number,
            state,
        } = self;
        match state {
            DropOrTakeState::Init => {
                let n = match usize::try_from(*number) {
                    Ok(n) => n,
                    Err(err) => {
                        return StepResult::Fail(EventHandlerError::EffectError(Box::new(err)))
                    }
                };
                let lane = projection(context);
                let to_remove = lane.get_map(|map| drop_or_take(map, *kind, n));
                let mut handler = MapLaneRemoveMultiple::new(*projection, to_remove);
                let result = handler.step(action_context, meta, context);
                *state = DropOrTakeState::Removing(handler);
                result
            }
            DropOrTakeState::Removing(h) => h.step(action_context, meta, context),
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneDropOrTake {
            projection,
            kind,
            number,
            state,
        } = self;
        let mut dbg = f.debug_struct("MapLaneRemoveMultiple");
        dbg.field("kind", kind).field("number", number);
        match state {
            DropOrTakeState::Init => {
                let lane = projection(context);
                let name = context.item_name(lane.id());
                dbg.field("id", &lane.id())
                    .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
                    .field("state", &"Init")
                    .finish()
            }
            DropOrTakeState::Removing(h) => dbg
                .field("state", &"Removing")
                .field("inner", &Described::new(context, h))
                .finish(),
        }
    }
}

struct MapLaneSelectRemoveMultiple<C, K, V, F> {
    _type: PhantomData<fn(&C, &V)>,
    projection: F,
    keys: Option<VecDeque<K>>,
}

impl<C, K, V, F> MapLaneSelectRemoveMultiple<C, K, V, F> {
    fn new(projection: F, keys: VecDeque<K>) -> Self {
        MapLaneSelectRemoveMultiple {
            _type: PhantomData,
            projection,
            keys: Some(keys),
        }
    }
}

impl<C, K, V, F, M> HandlerAction<C> for MapLaneSelectRemoveMultiple<C, K, V, F>
where
    K: Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V, M>>,
    M: MapOps<K, V>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneSelectRemoveMultiple {
            projection, keys, ..
        } = self;
        if let Some(key_queue) = keys {
            if let Some(next) = key_queue.pop_front() {
                let selector = projection.selector(context);
                if let Some(lane) = selector.select() {
                    lane.remove(&next);
                    StepResult::Continue {
                        modified_item: Some(Modification::of(lane.id)),
                    }
                } else {
                    StepResult::Fail(EventHandlerError::LaneNotFound(selector.name().to_owned()))
                }
            } else {
                *keys = None;
                StepResult::done(())
            }
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneSelectRemoveMultiple {
            projection, keys, ..
        } = self;
        f.debug_struct("MapLaneSelectRemoveMultiple")
            .field("lane_name", &projection.name())
            .field("keys", &keys.as_ref().map(|v| v.len()))
            .field("consumed", &keys.is_none())
            .finish()
    }
}

#[derive(Default)]
enum SelectDropOrTakeState<C, K, V, F> {
    Init(F),
    Removing(MapLaneSelectRemoveMultiple<C, K, V, F>),
    #[default]
    Done,
}

/// An [event handler](crate::event_handler::EventHandler) that attempts to drop or retain the first or last `n` elements of
/// a map lane, if that lane exists.
pub struct MapLaneSelectDropOrTake<C, K, V, F> {
    kind: DropOrTake,
    number: u64,
    state: SelectDropOrTakeState<C, K, V, F>,
}

impl<C, K, V, F> MapLaneSelectDropOrTake<C, K, V, F> {
    fn new(projection: F, kind: DropOrTake, number: u64) -> Self {
        MapLaneSelectDropOrTake {
            kind,
            number,
            state: SelectDropOrTakeState::Init(projection),
        }
    }
}

impl<C, K, V, F, M> HandlerAction<C> for MapLaneSelectDropOrTake<C, K, V, F>
where
    C: AgentDescription,
    K: StructuralWritable + Clone + Eq + Hash,
    F: SelectorFn<C, Target = MapLane<K, V, M>>,
    M: MapOps<K, V>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<C>,
        meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneSelectDropOrTake {
            kind,
            number,
            state,
        } = self;
        match std::mem::take(state) {
            SelectDropOrTakeState::Init(projection) => {
                let n = match usize::try_from(*number) {
                    Ok(n) => n,
                    Err(err) => {
                        return StepResult::Fail(EventHandlerError::EffectError(Box::new(err)))
                    }
                };
                let selector = projection.selector(context);
                let to_remove = if let Some(lane) = selector.select() {
                    lane.get_map(|map| drop_or_take(map, *kind, n))
                } else {
                    return StepResult::Fail(EventHandlerError::LaneNotFound(
                        selector.name().to_owned(),
                    ));
                };
                drop(selector);
                let mut handler = MapLaneSelectRemoveMultiple::new(projection, to_remove);
                let result = handler.step(action_context, meta, context);
                if result.is_cont() {
                    *state = SelectDropOrTakeState::Removing(handler);
                }
                result
            }
            SelectDropOrTakeState::Removing(mut h) => {
                let result = h.step(action_context, meta, context);
                if result.is_cont() {
                    *state = SelectDropOrTakeState::Removing(h);
                }
                result
            }
            SelectDropOrTakeState::Done => StepResult::after_done(),
        }
    }

    fn describe(&self, context: &C, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let MapLaneSelectDropOrTake {
            kind,
            number,
            state,
        } = self;
        let mut dbg = f.debug_struct("MapLaneRemoveMultiple");
        dbg.field("kind", kind).field("number", number);
        match state {
            SelectDropOrTakeState::Init(_) => dbg.field("state", &"Init").finish(),
            SelectDropOrTakeState::Removing(h) => dbg
                .field("state", &"Removing")
                .field("inner", &Described::new(context, h))
                .finish(),
            SelectDropOrTakeState::Done => dbg.field("state", &"Done").finish(),
        }
    }
}
