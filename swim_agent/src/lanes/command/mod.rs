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

use std::cell::{Cell, RefCell};

use bytes::BytesMut;
use static_assertions::assert_impl_all;
use swim_api::protocol::agent::{ValueLaneResponse, ValueLaneResponseEncoder};
use swim_form::structural::write::StructuralWritable;
use tokio_util::codec::Encoder;

use crate::{
    agent_model::WriteResult,
    event_handler::{EventHandler, Modification, StepResult},
    meta::AgentMetadata,
};

pub mod lifecycle;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct CommandLane<T> {
    id: u64,
    prev_command: RefCell<Option<T>>,
    dirty: Cell<bool>,
    //sync_queue: RefCell<VecDeque<Uuid>>, TODO Is syncing reasonable?
}

assert_impl_all!(CommandLane<()>: Send);

impl<T> CommandLane<T> {
    pub fn new(id: u64) -> Self {
        CommandLane {
            id,
            prev_command: Default::default(),
            dirty: Cell::new(false),
        }
    }

    pub fn command(&self, value: T) {
        let CommandLane {
            prev_command,
            dirty,
            ..
        } = self;
        let mut guard = prev_command.borrow_mut();
        *guard = Some(value);
        dirty.set(true);
    }

    pub(crate) fn with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Option<T>) -> R,
    {
        let CommandLane { prev_command, .. } = self;
        let guard = prev_command.borrow();
        f(&*guard)
    }
}

const INFALLIBLE_SER: &str = "Serializing a command to recon should be infallible.";

impl<T: StructuralWritable> CommandLane<T> {
    pub fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let CommandLane {
            prev_command,
            dirty,
            ..
        } = self;
        let mut encoder = ValueLaneResponseEncoder;
        if dirty.get() {
            let value_guard = prev_command.borrow();
            let response = ValueLaneResponse::event(&*value_guard);
            encoder.encode(response, buffer).expect(INFALLIBLE_SER);
            dirty.set(false);
            WriteResult::Done
        } else {
            WriteResult::NoData
        }
    }
}

pub struct DoCommand<Context, T> {
    projection: fn(&Context) -> &CommandLane<T>,
    command: Option<T>,
}

impl<Context, T> DoCommand<Context, T> {
    pub fn new(projection: fn(&Context) -> &CommandLane<T>, command: T) -> Self {
        DoCommand {
            projection,
            command: Some(command),
        }
    }
}

impl<Context, T> EventHandler<Context> for DoCommand<Context, T> {
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &Context) -> StepResult<Self::Completion> {
        let DoCommand {
            projection,
            command,
        } = self;
        if let Some(cmd) = command.take() {
            let lane = projection(context);
            lane.command(cmd);
            StepResult::Complete {
                modified_lane: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}
