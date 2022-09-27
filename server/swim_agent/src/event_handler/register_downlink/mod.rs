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

use swim_api::error::DownlinkRuntimeError;

use crate::{agent_model::downlink::handlers::BoxDownlinkChannel, meta::AgentMetadata};

use super::{ActionContext, DownlinkSpawner, HandlerAction, StepResult, WriteStream};

struct RegInner<Context> {
    channel: BoxDownlinkChannel<Context>,
    write_stream: WriteStream,
}

/// A [`HandlerAction`] that registers a downlink with the agent task.
pub struct RegisterHostedDownlink<Context> {
    inner: Option<RegInner<Context>>,
}

impl<Context> RegisterHostedDownlink<Context> {
    pub fn new(channel: BoxDownlinkChannel<Context>, write_stream: WriteStream) -> Self {
        RegisterHostedDownlink {
            inner: Some(RegInner {
                channel,
                write_stream,
            }),
        }
    }
}

impl<Context> HandlerAction<Context> for RegisterHostedDownlink<Context> {
    type Completion = Result<(), DownlinkRuntimeError>;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let RegisterHostedDownlink { inner } = self;
        if let Some(RegInner {
            channel,
            write_stream,
        }) = inner.take()
        {
            StepResult::done(action_context.spawn_downlink(channel, write_stream))
        } else {
            StepResult::after_done()
        }
    }
}
