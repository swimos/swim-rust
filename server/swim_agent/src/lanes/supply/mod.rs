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

use std::{cell::RefCell, collections::VecDeque};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use swim_api::protocol::agent::{LaneResponse, ValueLaneResponseEncoder};
use swim_form::structural::write::StructuralWritable;

use crate::event_handler::EventHandlerError;
use crate::{
    agent_model::WriteResult,
    event_handler::{ActionContext, HandlerAction, Modification, StepResult},
    item::AgentItem,
    meta::AgentMetadata,
};

use super::LaneItem;

#[cfg(test)]
mod tests;

#[derive(Debug)]
struct SupplyLaneInner<T> {
    sync_queue: VecDeque<Uuid>,
    event_queue: VecDeque<T>,
    next: EventOrSync,
}

impl<T> Default for SupplyLaneInner<T> {
    fn default() -> SupplyLaneInner<T> {
        SupplyLaneInner {
            sync_queue: Default::default(),
            event_queue: Default::default(),
            next: Default::default(),
        }
    }
}

/// A stateless lane that pushes events received directly to all uplinks attached to it.
///
/// A Supply lane can be push a a value by executing an instance of [`Supply`] (which can be
/// constructed using the [`crate::agent_lifecycle::utility::HandlerContext`]).
#[derive(Debug)]
pub struct SupplyLane<T> {
    id: u64,
    inner: RefCell<SupplyLaneInner<T>>,
}

impl<T> SupplyLane<T> {
    /// Create a Supply lane with the specified ID (this needs to be unique within an agent).
    pub fn new(id: u64) -> Self {
        SupplyLane {
            id,
            inner: Default::default(),
        }
    }

    fn push(&self, event: T) {
        let mut guard = self.inner.borrow_mut();
        guard.event_queue.push_back(event)
    }

    fn sync(&self, id: Uuid) {
        let mut guard = self.inner.borrow_mut();
        guard.sync_queue.push_back(id);
    }
}

assert_impl_all!(SupplyLane<()>: Send);

impl<T> AgentItem for SupplyLane<T> {
    fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug, Default)]
enum EventOrSync {
    #[default]
    Event,
    Sync,
}

impl EventOrSync {
    fn negate(&mut self) {
        match self {
            EventOrSync::Event => *self = EventOrSync::Sync,
            EventOrSync::Sync => *self = EventOrSync::Event,
        }
    }
}

const INFALLIBLE_SER: &str = "Serializing a value to recon should be infallible.";

fn write<F, I, T, O>(
    buffer: &mut BytesMut,
    poll_queue: &mut VecDeque<I>,
    on_empty_queue: &mut VecDeque<O>,
    state: &mut EventOrSync,
    wrap: F,
) -> Option<WriteResult>
where
    F: Fn(I) -> LaneResponse<T>,
    T: StructuralWritable,
{
    match poll_queue.pop_front() {
        Some(elem) => {
            let mut encoder = ValueLaneResponseEncoder::default();
            encoder.encode(wrap(elem), buffer).expect(INFALLIBLE_SER);
            if !on_empty_queue.is_empty() {
                state.negate();
                Some(WriteResult::DataStillAvailable)
            } else if poll_queue.is_empty() {
                Some(WriteResult::Done)
            } else {
                Some(WriteResult::DataStillAvailable)
            }
        }
        None => {
            if on_empty_queue.is_empty() {
                Some(WriteResult::Done)
            } else {
                state.negate();
                None
            }
        }
    }
}

impl<T> LaneItem for SupplyLane<T>
where
    T: StructuralWritable,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let SupplyLane { inner, .. } = self;
        let mut guard = inner.borrow_mut();
        let SupplyLaneInner {
            sync_queue,
            event_queue,
            next,
        } = &mut *guard;

        loop {
            match next {
                EventOrSync::Event => {
                    if let Some(result) = write(
                        buffer,
                        event_queue,
                        sync_queue,
                        next,
                        LaneResponse::<T>::event,
                    ) {
                        return result;
                    }
                }
                EventOrSync::Sync => {
                    if let Some(result) = write(
                        buffer,
                        sync_queue,
                        event_queue,
                        next,
                        LaneResponse::<T>::synced,
                    ) {
                        return result;
                    }
                }
            }
        }
    }
}

pub struct Supply<Context, T> {
    projection: fn(&Context) -> &SupplyLane<T>,
    value: Option<T>,
}

impl<Context, T> Supply<Context, T> {
    pub fn new(projection: fn(&Context) -> &SupplyLane<T>, event: T) -> Self {
        Supply {
            projection,
            value: Some(event),
        }
    }
}

impl<Context, T> HandlerAction<Context> for Supply<Context, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let Supply { projection, value } = self;
        if let Some(value) = value.take() {
            let lane = projection(context);
            lane.push(value);
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id())),
                result: (),
            }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }
}

pub struct SupplyLaneSync<Context, T> {
    projection: fn(&Context) -> &SupplyLane<T>,
    id: Option<Uuid>,
}

impl<Context, T> SupplyLaneSync<Context, T> {
    pub fn new(projection: fn(&Context) -> &SupplyLane<T>, id: Uuid) -> Self {
        SupplyLaneSync {
            projection,
            id: Some(id),
        }
    }
}

impl<Context, T> HandlerAction<Context> for SupplyLaneSync<Context, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let SupplyLaneSync { projection, id } = self;
        if let Some(id) = id.take() {
            let lane = projection(context);
            lane.sync(id);
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}
