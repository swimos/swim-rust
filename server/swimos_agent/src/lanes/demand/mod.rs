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

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use swimos_api::protocol::agent::{LaneResponse, ValueLaneResponseEncoder};
use swimos_form::structural::write::StructuralWritable;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::{
    agent_model::WriteResult,
    event_handler::{ActionContext, HandlerAction, Modification, StepResult},
    item::AgentItem,
    meta::AgentMetadata,
};

use super::LaneItem;

pub mod lifecycle;
#[cfg(test)]
mod tests;

#[derive(Debug)]
struct DemandLaneInner<T> {
    computed_value: Option<T>,
    sync_queue: VecDeque<Uuid>,
}

impl<T> Default for DemandLaneInner<T> {
    fn default() -> Self {
        Self {
            computed_value: Default::default(),
            sync_queue: Default::default(),
        }
    }
}

/// A lane that is a stateless analogue of [`super::value::ValueLane`]. Rather than maintaining
/// a persistent state that can be queried, a demand lane computes a value, on demand, that is
/// sent on all uplinks attached to it.
///
/// A demand lane can be cued to produce a value by executing an instance of [`Cue`] (which can be
/// constructed using the [`crate::agent_lifecycle::utility::HandlerContext`]).
#[derive(Debug)]
pub struct DemandLane<T> {
    id: u64,
    inner: RefCell<DemandLaneInner<T>>,
    cued: Cell<bool>,
}

impl<T> DemandLane<T> {
    /// Create a demand lane with the specified ID (this needs to be unique within an agent).
    pub fn new(id: u64) -> Self {
        DemandLane {
            id,
            inner: Default::default(),
            cued: Cell::new(false),
        }
    }

    pub(crate) fn cue(&self) {
        self.cued.set(true)
    }

    pub(crate) fn sync(&self, id: Uuid) {
        let mut guard = self.inner.borrow_mut();
        guard.sync_queue.push_back(id);
    }
}

assert_impl_all!(DemandLane<()>: Send);

impl<T> AgentItem for DemandLane<T> {
    fn id(&self) -> u64 {
        self.id
    }
}

const INFALLIBLE_SER: &str = "Serializing a value to recon should be infallible.";

impl<T> LaneItem for DemandLane<T>
where
    T: StructuralWritable,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let DemandLane { inner, cued, .. } = self;
        let mut encoder = ValueLaneResponseEncoder::default();
        let mut guard = inner.borrow_mut();
        let DemandLaneInner {
            computed_value,
            sync_queue,
        } = &mut *guard;
        if let Some(value) = computed_value {
            if let Some(id) = sync_queue.pop_front() {
                let value_response = LaneResponse::sync_event(id, value);
                encoder
                    .encode(value_response, buffer)
                    .expect(INFALLIBLE_SER);
                let synced_response = LaneResponse::<&T>::synced(id);
                encoder
                    .encode(synced_response, buffer)
                    .expect(INFALLIBLE_SER);
                if cued.get() || !sync_queue.is_empty() {
                    WriteResult::DataStillAvailable
                } else {
                    *computed_value = None;
                    WriteResult::Done
                }
            } else if cued.get() {
                let response = LaneResponse::event(value);
                encoder.encode(response, buffer).expect(INFALLIBLE_SER);
                cued.set(false);
                *computed_value = None;
                WriteResult::Done
            } else {
                WriteResult::NoData
            }
        } else {
            WriteResult::NoData
        }
    }
}

enum DemandInner<T, H> {
    Pending(H),
    Complete(Option<T>),
}

pub struct Demand<Context, T, H> {
    projection: fn(&Context) -> &DemandLane<T>,
    state: DemandInner<T, H>,
}

impl<Context, T, H> Demand<Context, T, H> {
    pub fn new(projection: fn(&Context) -> &DemandLane<T>, compute_handler: H) -> Self {
        Demand {
            projection,
            state: DemandInner::Pending(compute_handler),
        }
    }
}

impl<Context, T, H> HandlerAction<Context> for Demand<Context, T, H>
where
    H: HandlerAction<Context, Completion = T>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let Demand { projection, state } = self;
        match state {
            DemandInner::Pending(h) => match h.step(action_context, meta, context) {
                StepResult::Continue { modified_item } => StepResult::Continue { modified_item },
                StepResult::Fail(err) => {
                    *state = DemandInner::Complete(None);
                    StepResult::Fail(err)
                }
                StepResult::Complete {
                    modified_item,
                    result,
                } => {
                    *state = DemandInner::Complete(Some(result));
                    StepResult::Continue { modified_item }
                }
            },
            DemandInner::Complete(maybe_val) => {
                if let Some(t) = std::mem::take(maybe_val) {
                    let lane = projection(context);

                    let mut guard = lane.inner.borrow_mut();
                    guard.computed_value = Some(t);
                    StepResult::Complete {
                        modified_item: Some(Modification::no_trigger(lane.id)),
                        result: (),
                    }
                } else {
                    StepResult::after_done()
                }
            }
        }
    }
}

pub struct Cue<Context, T> {
    projection: fn(&Context) -> &DemandLane<T>,
    cued: bool,
}

impl<Context, T> Cue<Context, T> {
    pub fn new(projection: fn(&Context) -> &DemandLane<T>) -> Self {
        Cue {
            projection,
            cued: false,
        }
    }
}

impl<Context, T> HandlerAction<Context> for Cue<Context, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let Cue { projection, cued } = self;
        if std::mem::replace(cued, true) {
            StepResult::after_done()
        } else {
            let lane = projection(context);
            lane.cue();
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id())),
                result: (),
            }
        }
    }
}

pub struct DemandLaneSync<Context, T> {
    projection: fn(&Context) -> &DemandLane<T>,
    id: Option<Uuid>,
}

impl<Context, T> DemandLaneSync<Context, T> {
    pub fn new(projection: fn(&Context) -> &DemandLane<T>, id: Uuid) -> Self {
        DemandLaneSync {
            projection,
            id: Some(id),
        }
    }
}

impl<Context, T> HandlerAction<Context> for DemandLaneSync<Context, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let DemandLaneSync { projection, id } = self;
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
