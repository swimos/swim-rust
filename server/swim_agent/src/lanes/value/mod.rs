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

pub mod lifecycle;

#[cfg(test)]
mod tests;

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use swim_api::protocol::agent::{ValueLaneResponse, ValueLaneResponseEncoder};
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::{
    agent_model::WriteResult,
    event_handler::{
        AndThen, Decode, EventHandler, EventHandlerError, EventHandlerExt, HandlerTrans,
        Modification, StepResult,
    },
    meta::AgentMetadata,
};

use super::{Lane, ProjTransform};

/// Model of a value lane. This maintains a state and triggers an event each time this state is updated.
/// Updates may come from external commands or from an action performed by an event handler on the agent.
#[derive(Debug)]
pub struct ValueLane<T> {
    id: u64,
    content: RefCell<T>,
    previous: RefCell<Option<T>>,
    dirty: Cell<bool>,
    sync_queue: RefCell<VecDeque<Uuid>>,
}

assert_impl_all!(ValueLane<()>: Send);

impl<T> ValueLane<T> {
    /// #Arguments
    /// * `id` - The ID of the lane. This should be unique in an agent.
    /// * `init` - The initial value of the lane.
    pub fn new(id: u64, init: T) -> Self {
        ValueLane {
            id,
            content: RefCell::new(init),
            previous: Default::default(),
            dirty: Cell::new(false),
            sync_queue: Default::default(),
        }
    }

    /// Read the state of the lane.
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let ValueLane { content, .. } = self;
        let value = content.borrow();
        f(&*value)
    }

    /// Read the state of the lane, consuming the previous value (used when triggering the `on_set` event
    /// handler for the lane).
    pub(crate) fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<T>, &T) -> R,
    {
        let ValueLane {
            content, previous, ..
        } = self;
        let prev = previous.borrow_mut().take();
        let value = content.borrow();
        f(prev, &*value)
    }

    /// Update the state of the lane.
    pub fn set(&self, value: T) {
        let ValueLane {
            content,
            previous,
            dirty,
            ..
        } = self;
        let prev = content.replace(value);
        previous.replace(Some(prev));
        dirty.replace(true);
    }

    pub fn sync(&self, id: Uuid) {
        let ValueLane { sync_queue, .. } = self;
        sync_queue.borrow_mut().push_back(id);
    }
}

const INFALLIBLE_SER: &str = "Serializing to recon should be infallible.";

impl<T: StructuralWritable> Lane for ValueLane<T> {
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let ValueLane {
            content,
            dirty,
            sync_queue,
            ..
        } = self;
        let mut encoder = ValueLaneResponseEncoder;
        let mut sync = sync_queue.borrow_mut();
        if let Some(id) = sync.pop_front() {
            let value_guard = content.borrow();
            let response = ValueLaneResponse::synced(id, &*value_guard);
            encoder.encode(response, buffer).expect(INFALLIBLE_SER);
            if dirty.get() || !sync.is_empty() {
                WriteResult::DataStillAvailable
            } else {
                WriteResult::Done
            }
        } else if dirty.get() {
            let value_guard = content.borrow();
            let response = ValueLaneResponse::event(&*value_guard);
            encoder.encode(response, buffer).expect(INFALLIBLE_SER);
            dirty.set(false);
            WriteResult::Done
        } else {
            WriteResult::NoData
        }
    }
}

/// An [`EventHandler`] that will get the value of a value lane.
pub struct ValueLaneGet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueLane<T>,
    done: bool,
}

impl<C, T> ValueLaneGet<C, T> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the lane.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>) -> Self {
        ValueLaneGet {
            projection,
            done: false,
        }
    }
}

/// An [`EventHandler`] that will set the value of a value lane.
pub struct ValueLaneSet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueLane<T>,
    value: Option<T>,
}

/// An [`EventHandler`] that will request a sync from the lane.
pub struct ValueLaneSync<C, T> {
    projection: for<'b> fn(&'b C) -> &'b ValueLane<T>,
    id: Option<Uuid>,
}

impl<C, T> ValueLaneSet<C, T> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `value` - The new value for the lane.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>, value: T) -> Self {
        ValueLaneSet {
            projection,
            value: Some(value),
        }
    }
}

impl<C, T> ValueLaneSync<C, T> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `id` - The ID of the remote that requested the sync.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>, id: Uuid) -> Self {
        ValueLaneSync {
            projection,
            id: Some(id),
        }
    }
}

impl<C, T: Clone> EventHandler<C> for ValueLaneGet<C, T> {
    type Completion = T;

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
        let ValueLaneGet { projection, done } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            let lane = projection(context);
            let value = lane.read(T::clone);
            StepResult::done(value)
        }
    }
}

impl<C, T> EventHandler<C> for ValueLaneSet<C, T> {
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
        let ValueLaneSet { projection, value } = self;
        if let Some(value) = value.take() {
            let lane = projection(context);
            lane.set(value);
            StepResult::Complete {
                modified_lane: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }
}

impl<C, T> EventHandler<C> for ValueLaneSync<C, T> {
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
        let ValueLaneSync { projection, id } = self;
        if let Some(id) = id.take() {
            let lane = projection(context);
            lane.sync(id);
            StepResult::Complete {
                modified_lane: Some(Modification::no_trigger(lane.id)),
                result: (),
            }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }
}

impl<C, T> HandlerTrans<T> for ProjTransform<C, ValueLane<T>> {
    type Out = ValueLaneSet<C, T>;

    fn transform(self, input: T) -> Self::Out {
        let ProjTransform { projection } = self;
        ValueLaneSet::new(projection, input)
    }
}

pub type DecodeAndSet<C, T> =
    AndThen<Decode<T>, ValueLaneSet<C, T>, ProjTransform<C, ValueLane<T>>>;

/// Create an event handler that will decode an incoming command and set the value into a value lane.
pub fn decode_and_set<C, T: RecognizerReadable>(
    buffer: BytesMut,
    projection: fn(&C) -> &ValueLane<T>,
) -> DecodeAndSet<C, T> {
    let decode: Decode<T> = Decode::new(buffer);
    decode.and_then(ProjTransform::new(projection))
}
