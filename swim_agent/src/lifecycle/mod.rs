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

use bytes::Bytes;
use swim_model::Text;

use self::{on_stop::OnStop, on_start::OnStart, on_command::OnReceive};

pub mod on_command;
pub mod on_start;
pub mod on_stop;

pub trait AgentHandlers<'a, Context>:
    OnStart<'a, Context> + OnStop<'a, Context> + OnReceive<'a, Context>
{
}

pub trait AgentLifecycle<Context>: for<'a> AgentHandlers<'a, Context> {}

impl<L, Context> AgentLifecycle<Context> for L
where
    L: for<'a> AgentHandlers<'a, Context>,
{}

impl<'a, L, Context> AgentHandlers<'a, Context> for L
where
    L: OnStart<'a, Context> + OnStop<'a, Context> + OnReceive<'a, Context>,
{}

pub struct BasicAgentLifecycle<FStart, FStop, FRec> {
    on_start: FStart,
    on_stop: FStop,
    on_command: FRec,
}

impl<'a, FStart, FStop, FRec, Context> OnStart<'a, Context> for BasicAgentLifecycle<FStart, FStop, FRec>
where
    FStart: OnStart<'a, Context>,
    FStop: Send,
    FRec: Send,
{
    type OnStartHandler = FStart::OnStartHandler;

    fn on_start(&'a mut self) -> Self::OnStartHandler {
        self.on_start.on_start()
    }
}

impl<'a, FStart, FStop, FRec, Context> OnStop<'a, Context> for BasicAgentLifecycle<FStart, FStop, FRec>
where
    FStop: OnStop<'a, Context>,
    FStart: Send,
    FRec: Send,
{
    type OnStopHandler = FStop::OnStopHandler;

    fn on_stop(&'a mut self) -> Self::OnStopHandler {
        self.on_stop.on_stop()
    }
}

impl<'a, FStart, FStop, FRec, Context> OnReceive<'a, Context> for BasicAgentLifecycle<FStart, FStop, FRec>
where
    FRec: OnReceive<'a, Context>,
    FStart: Send,
    FStop: Send,
{
    type OnCommandHandler = FRec::OnCommandHandler;

    fn on_command(&'a mut self, lane: Text, body: Bytes) -> Self::OnCommandHandler {
        self.on_command.on_command(lane, body)
    }

    type OnSyncHandler = FRec::OnSyncHandler;

    fn on_sync(&'a mut self, lane: Text, id: uuid::Uuid) -> Self::OnSyncHandler {
        self.on_command.on_sync(lane, id)
    }
}