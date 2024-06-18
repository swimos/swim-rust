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

use std::cell::{Cell, RefCell};

use bytes::BytesMut;
use futures::{Stream, StreamExt};
use static_assertions::assert_impl_all;
use swimos_agent_protocol::{encoding::lane::ValueLaneResponseEncoder, LaneResponse};
use swimos_api::error::FrameIoError;
use swimos_form::{read::RecognizerReadable, write::StructuralWritable};
use swimos_recon::parser::AsyncParseError;
use tokio_util::codec::Encoder;

use crate::{
    agent_model::WriteResult,
    event_handler::{
        ActionContext, AndThen, Decode, HandlerAction, HandlerActionExt, HandlerTrans,
        Modification, StepResult,
    },
    item::AgentItem,
    meta::AgentMetadata,
};

use super::{LaneItem, ProjTransform};

pub mod lifecycle;
#[cfg(test)]
mod tests;

/// Model of a command lane. An event is triggered when a command is received (either externally or
/// internally) but the lane does not maintain any record of its state.
#[derive(Debug)]
pub struct CommandLane<T> {
    id: u64,
    prev_command: RefCell<Option<T>>,
    dirty: Cell<bool>,
    //sync_queue: RefCell<VecDeque<Uuid>>, TODO Is syncing reasonable?
}

assert_impl_all!(CommandLane<()>: Send);

impl<T> CommandLane<T> {
    /// Create a command lane with the specified ID (this needs to be unique within an agent).
    pub fn new(id: u64) -> Self {
        CommandLane {
            id,
            prev_command: Default::default(),
            dirty: Cell::new(false),
        }
    }

    /// Execute a command against the lane.
    pub(crate) fn command(&self, value: T) {
        let CommandLane {
            prev_command,
            dirty,
            ..
        } = self;
        let mut guard = prev_command.borrow_mut();
        *guard = Some(value);
        dirty.set(true);
    }

    /// Consume the previous command that was executed against the lane.
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

/// An [`EventHandler`] instance that feeds a command to the command lane.
pub struct DoCommand<Context, T> {
    projection: fn(&Context) -> &CommandLane<T>,
    command: Option<T>,
}

impl<Context, T> DoCommand<Context, T> {
    /// # Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `command` - The command to feed.
    pub fn new(projection: fn(&Context) -> &CommandLane<T>, command: T) -> Self {
        DoCommand {
            projection,
            command: Some(command),
        }
    }
}

impl<Context, T> HandlerAction<Context> for DoCommand<Context, T> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let DoCommand {
            projection,
            command,
        } = self;
        if let Some(cmd) = command.take() {
            let lane = projection(context);
            lane.command(cmd);
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

impl<C, T> HandlerTrans<T> for ProjTransform<C, CommandLane<T>> {
    type Out = DoCommand<C, T>;

    fn transform(self, input: T) -> Self::Out {
        let ProjTransform { projection } = self;
        DoCommand::new(projection, input)
    }
}

pub type DecodeAndCommand<C, T> =
    AndThen<Decode<T>, DoCommand<C, T>, ProjTransform<C, CommandLane<T>>>;

/// Create an event handler that will decode an incoming command and apply it to a command lane.
pub fn decode_and_command<C, T: RecognizerReadable>(
    buffer: BytesMut,
    projection: fn(&C) -> &CommandLane<T>,
) -> DecodeAndCommand<C, T> {
    let decode: Decode<T> = Decode::new(buffer);
    decode.and_then(ProjTransform::new(projection))
}

impl<T: StructuralWritable> LaneItem for CommandLane<T> {
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let CommandLane {
            prev_command,
            dirty,
            ..
        } = self;
        let mut encoder = ValueLaneResponseEncoder::default();
        if dirty.get() {
            let value_guard = prev_command.borrow();
            if let Some(value) = &*value_guard {
                let response = LaneResponse::event(value);
                encoder.encode(response, buffer).expect(INFALLIBLE_SER);
                dirty.set(false);
                WriteResult::Done
            } else {
                WriteResult::NoData
            }
        } else {
            WriteResult::NoData
        }
    }
}

impl<T> AgentItem for CommandLane<T> {
    fn id(&self) -> u64 {
        self.id
    }
}

pub async fn init_command_lane<T, In>(
    mut input: In,
) -> Result<impl FnOnce(&CommandLane<T>), FrameIoError>
where
    In: Stream<Item = Result<BytesMut, FrameIoError>> + Unpin,
{
    let mut body = BytesMut::new();
    while let Some(bytes) = input.next().await {
        body = bytes?;
    }
    if !body.is_empty() {
        Err(AsyncParseError::UnconsumedInput.into())
    } else {
        Ok(|_: &CommandLane<T>| {})
    }
}
