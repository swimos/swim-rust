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
use static_assertions::assert_impl_all;
use std::{borrow::Borrow, cell::RefCell, collections::HashMap, hash::Hash};
use swim_api::protocol::{
    agent::{LaneResponseKind, MapLaneResponse, MapLaneResponseEncoder},
    map::MapOperation,
};
use swim_form::structural::write::StructuralWritable;
use tokio_util::codec::Encoder;
use uuid::Uuid;

mod event;
pub mod lifecycle;
mod queues;

#[cfg(test)]
mod tests;

use crate::{
    agent_model::WriteResult,
    event_handler::{EventHandler, Modification, StepResult},
    meta::AgentMetadata,
};

use self::queues::{Action, ToWrite, WriteQueues};

pub use event::MapLaneEvent;

#[derive(Debug)]
pub struct MapLane<K, V> {
    id: u64,
    inner: RefCell<Inner<K, V>>,
}

assert_impl_all!(MapLane<(), ()>: Send);

impl<K, V> MapLane<K, V> {
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
    pub fn update(&self, key: K, value: V) {
        self.inner.borrow_mut().update(key, value)
    }

    pub fn remove(&self, key: &K) {
        self.inner.borrow_mut().remove(key)
    }

    pub fn clear(&self) {
        self.inner.borrow_mut().clear()
    }

    pub fn get<Q, F, R>(&self, key: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
        F: FnOnce(Option<&V>) -> R,
    {
        self.inner.borrow().get(key, f)
    }

    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<K, V>) -> R,
    {
        self.inner.borrow().get_map(f)
    }

    pub(crate) fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V>>, &HashMap<K, V>) -> R,
    {
        self.inner.borrow_mut().read_with_prev(f)
    }

    pub fn sync(&self, id: Uuid) {
        self.inner.borrow_mut().sync(id);
    }
}

impl<K, V> MapLane<K, V>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
{
    pub fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
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

impl<C, K, V> EventHandler<C> for MapLaneUpdate<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
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

impl<C, K, V> EventHandler<C> for MapLaneRemove<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
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

impl<C, K, V> EventHandler<C> for MapLaneClear<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
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

impl<C, K, V> EventHandler<C> for MapLaneGet<C, K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Completion = Option<V>;

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
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

impl<C, K, V> EventHandler<C> for MapLaneGetMap<C, K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Completion = HashMap<K, V>;

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
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

impl<C, K, V> EventHandler<C> for MapLaneSync<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
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
