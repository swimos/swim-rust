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

use self::{on_stop::OnStop, on_start::OnStart, lane_event::LaneEvent};

pub mod lane_event;
pub mod on_start;
pub mod on_stop;

pub trait AgentHandlers<'a, Context>:
    OnStart<'a, Context> + OnStop<'a, Context> + LaneEvent<'a, Context>
{
}

pub trait AgentLifecycle<Context>: for<'a> AgentHandlers<'a, Context> {}

impl<L, Context> AgentLifecycle<Context> for L
where
    L: for<'a> AgentHandlers<'a, Context>,
{}

impl<'a, L, Context> AgentHandlers<'a, Context> for L
where
    L: OnStart<'a, Context> + OnStop<'a, Context> + LaneEvent<'a, Context>
{}

pub struct BasicAgentLifecycle<FStart, FStop, LaneEv> {
    on_start: FStart,
    on_stop: FStop,
    lane_event: LaneEv,
}

impl<'a, FStart, FStop, LaneEv, Context> OnStart<'a, Context> for BasicAgentLifecycle<FStart, FStop, LaneEv>
where
    FStart: OnStart<'a, Context>,
    FStop: Send,
    LaneEv: Send,
{
    type OnStartHandler = FStart::OnStartHandler;

    fn on_start(&'a self) -> Self::OnStartHandler {
        self.on_start.on_start()
    }
}

impl<'a, FStart, FStop, LaneEv, Context> OnStop<'a, Context> for BasicAgentLifecycle<FStart, FStop, LaneEv>
where
    FStop: OnStop<'a, Context>,
    FStart: Send,
    LaneEv: Send,
{
    type OnStopHandler = FStop::OnStopHandler;

    fn on_stop(&'a self) -> Self::OnStopHandler {
        self.on_stop.on_stop()
    }
}

impl<'a, FStart, FStop, LaneEv, Context> LaneEvent<'a, Context> for BasicAgentLifecycle<FStart, FStop, LaneEv>
where
    FStop: Send,
    FStart: Send,
    LaneEv: LaneEvent<'a, Context>,
{
    type LaneEventHandler = LaneEv::LaneEventHandler;

    fn lane_event(&'a self, lane_name: &str) -> Self::LaneEventHandler {
        self.lane_event.lane_event(lane_name)
    }
}
