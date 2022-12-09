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

use bytes::BytesMut;
use frunk::{Coprod, Coproduct};
use static_assertions::assert_impl_all;
use std::{borrow::Borrow, cell::RefCell, collections::HashMap, hash::Hash, marker::PhantomData};
use swim_api::protocol::{
    agent::{LaneResponseKind, MapLaneResponse, MapLaneResponseEncoder},
    map::{MapMessage, MapOperation},
};
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swim_recon::parser::RecognizerDecoder;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

mod event;
pub mod lifecycle;
mod queues;

#[cfg(test)]
mod tests;

use crate::{
    agent_model::WriteResult,
    event_handler::{
        AndThen, EventHandlerError, HandlerAction, HandlerActionExt, HandlerTrans, Modification,
        Spawner, StepResult,
    },
    meta::AgentMetadata,
};

use self::queues::{Action, ToWrite, WriteQueues};

pub use event::MapLaneEvent;

use super::{Lane, ProjTransform};

/// Model of a value lane. This maintain a sate consisting of a hash-map from keys to values. It generates an
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
    /// #Arguments
    /// * `id` - The ID of the lane. This should be unique within an agent.
    /// * `init` - The initial contents of the map.
    pub fn new(id: u64, init: HashMap<K, V>) -> Self {
        MapLane {
            id,
            inner: RefCell::new(Inner::new(init)),
        }
    }
}

impl<K, V> MapLane<K, V>
where
    K: Clone + Eq + Hash,
{
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

    /// Start a sync operation from the lane to the specified remote.
    pub fn sync(&self, id: Uuid) {
        self.inner.borrow_mut().sync(id);
    }
}

impl<K, V> Lane for MapLane<K, V>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        self.inner.borrow_mut().write_to_buffer(buffer)
    }
}

#[derive(Debug)]
struct Inner<K, V> {
    content: HashMap<K, V>,
    previous: Option<MapLaneEvent<K, V>>,
    write_queues: WriteQueues<K>,
}

impl<K, V> Inner<K, V> {
    fn new(init: HashMap<K, V>) -> Self {
        Inner {
            content: init,
            previous: None,
            write_queues: Default::default(),
        }
    }
}

impl<K, V> Inner<K, V>
where
    K: Clone + Eq + Hash,
{
    pub fn update(&mut self, key: K, value: V) {
        let Inner {
            content,
            previous,
            write_queues,
        } = self;
        let prev = content.insert(key.clone(), value);
        *previous = Some(MapLaneEvent::Update(key.clone(), prev));
        write_queues.push_update(key);
    }

    pub fn remove(&mut self, key: &K) {
        let Inner {
            content,
            previous,
            write_queues,
        } = self;
        let prev = content.remove(key);
        if let Some(prev) = prev {
            *previous = Some(MapLaneEvent::Remove(key.clone(), prev));
            write_queues.push_remove(key.clone());
        }
    }

    pub fn clear(&mut self) {
        let Inner {
            content,
            previous,
            write_queues,
        } = self;
        *previous = Some(MapLaneEvent::Clear(std::mem::take(content)));
        write_queues.push_clear();
    }

    pub fn get<Q, F, R>(&self, key: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
        F: FnOnce(Option<&V>) -> R,
    {
        let Inner { content, .. } = self;
        f(content.get(key))
    }

    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<K, V>) -> R,
    {
        let Inner { content, .. } = self;
        f(content)
    }

    pub(crate) fn read_with_prev<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V>>, &HashMap<K, V>) -> R,
    {
        let Inner {
            content, previous, ..
        } = self;
        f(previous.take(), content)
    }

    pub fn sync(&mut self, id: Uuid) {
        let Inner {
            content,
            write_queues,
            ..
        } = self;
        write_queues.sync(id, content.keys().cloned());
    }
}

const INFALLIBLE_SER: &str = "Serializing map operations to recon should be infallible.";

impl<K, V> Inner<K, V>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
{
    pub fn write_to_buffer(&mut self, buffer: &mut BytesMut) -> WriteResult {
        let Inner {
            content,
            write_queues,
            ..
        } = self;
        loop {
            let maybe_response = match write_queues.pop() {
                Some(ToWrite::Event(event)) => {
                    to_operation(content, event).map(|operation| MapLaneResponse::Event {
                        kind: LaneResponseKind::StandardEvent,
                        operation,
                    })
                }
                Some(ToWrite::SyncEvent(id, key)) => to_operation(content, Action::Update(key))
                    .map(|operation| MapLaneResponse::Event {
                        kind: LaneResponseKind::SyncEvent(id),
                        operation,
                    }),
                Some(ToWrite::Synced(id)) => Some(MapLaneResponse::SyncComplete(id)),
                _ => break WriteResult::NoData,
            };
            if let Some(response) = maybe_response {
                let mut encoder = MapLaneResponseEncoder::default();
                encoder.encode(response, buffer).expect(INFALLIBLE_SER);
                break if write_queues.is_empty() {
                    WriteResult::Done
                } else {
                    WriteResult::DataStillAvailable
                };
            } else {
                continue;
            }
        }
    }
}

fn to_operation<K: Eq + Hash, V>(
    content: &HashMap<K, V>,
    action: Action<K>,
) -> Option<MapOperation<K, &V>> {
    match action {
        Action::Update(k) => content
            .get(&k)
            .map(|v| MapOperation::Update { key: k, value: v }),
        Action::Remove(k) => Some(MapOperation::Remove { key: k }),
        Action::Clear => Some(MapOperation::Clear),
    }
}

/// An [`EventHandler`] that will update the value of an entry in the map.
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
        _suspend: &dyn Spawner<C>,
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
                modified_lane: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will remove an entry from the map.
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
        _suspend: &dyn Spawner<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneRemove { projection, key } = self;
        if let Some(key) = key.take() {
            let lane = projection(context);
            lane.remove(&key);
            StepResult::Complete {
                modified_lane: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will clear the map.
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
        _suspend: &dyn Spawner<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneClear { projection, done } = self;
        if !*done {
            *done = true;
            let lane = projection(context);
            lane.clear();
            StepResult::Complete {
                modified_lane: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will get an entry from the map.
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
        _suspend: &dyn Spawner<C>,
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

/// An [`EventHandler`] that will read the entire state of a map lane.
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
        _suspend: &dyn Spawner<C>,
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

/// An [`EventHandler`] that will request a sync from the lane.
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
        _suspend: &dyn Spawner<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let MapLaneSync { projection, id } = self;
        if let Some(id) = id.take() {
            let lane = projection(context);
            lane.sync(id);
            StepResult::Complete {
                modified_lane: Some(Modification::no_trigger(lane.id)),
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
        _suspend: &dyn Spawner<Context>,
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
