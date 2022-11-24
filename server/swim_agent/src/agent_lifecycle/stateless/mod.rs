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

use std::marker::PhantomData;

use static_assertions::assert_impl_all;
use swim_api::handlers::{FnHandler, NoHandler};

use crate::agent_lifecycle::AgentLifecycle;

use super::{lane_event::LaneEvent, on_start::OnStart, on_stop::OnStop};

/// An implementation of [AgentLifecycle] with no shared state.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers run (provides access to the agent lanes).
/// * `FStart` - The `on_start` event handler.
/// * `FStop` - The `on_stop` event handler.
/// * `LaneEv` - The event handlers for all lanes in the agent.
#[derive(Debug)]
pub struct BasicAgentLifecycle<Context, FStart = NoHandler, FStop = NoHandler, LaneEv = NoHandler> {
    _context: PhantomData<fn(Context)>,
    on_start: FStart,
    on_stop: FStop,
    lane_event: LaneEv,
}

impl<Context> Default for BasicAgentLifecycle<Context> {
    fn default() -> Self {
        Self {
            _context: Default::default(),
            on_start: Default::default(),
            on_stop: Default::default(),
            lane_event: Default::default(),
        }
    }
}

assert_impl_all!(BasicAgentLifecycle<()>: AgentLifecycle<()>, Send);

impl<FStart, FStop, LaneEv, Context> OnStart<Context>
    for BasicAgentLifecycle<Context, FStart, FStop, LaneEv>
where
    FStart: OnStart<Context>,
    FStop: Send,
    LaneEv: Send,
{
    type OnStartHandler<'a> = FStart::OnStartHandler<'a> where Self: 'a;

    fn on_start<'a>(&'a self) -> Self::OnStartHandler<'a> {
        self.on_start.on_start()
    }
}

impl<'a, FStart, FStop, LaneEv, Context> OnStop<'a, Context>
    for BasicAgentLifecycle<Context, FStart, FStop, LaneEv>
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

impl<'a, FStart, FStop, LaneEv, Context> LaneEvent<'a, Context>
    for BasicAgentLifecycle<Context, FStart, FStop, LaneEv>
where
    FStop: Send,
    FStart: Send,
    LaneEv: LaneEvent<'a, Context>,
{
    type LaneEventHandler = LaneEv::LaneEventHandler;

    fn lane_event(&'a self, context: &Context, lane_name: &str) -> Option<Self::LaneEventHandler> {
        self.lane_event.lane_event(context, lane_name)
    }
}

impl<Context, FStart, FStop, LaneEv> BasicAgentLifecycle<Context, FStart, FStop, LaneEv> {
    pub fn on_start<F>(self, f: F) -> BasicAgentLifecycle<Context, FnHandler<F>, FStop, LaneEv>
    where
        FnHandler<F>: OnStart<Context>,
    {
        let BasicAgentLifecycle {
            on_stop,
            lane_event,
            ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_start: FnHandler(f),
            on_stop,
            lane_event,
        }
    }

    pub fn on_stop<F>(self, f: F) -> BasicAgentLifecycle<Context, FStart, FnHandler<F>, LaneEv>
    where
        FnHandler<F>: for<'a> OnStop<'a, Context>,
    {
        let BasicAgentLifecycle {
            on_start,
            lane_event,
            ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_start,
            on_stop: FnHandler(f),
            lane_event,
        }
    }

    pub fn on_lane_event<H>(self, handler: H) -> BasicAgentLifecycle<Context, FStart, FStop, H>
    where
        H: for<'a> LaneEvent<'a, Context>,
    {
        let BasicAgentLifecycle {
            on_start, on_stop, ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_start,
            on_stop,
            lane_event: handler,
        }
    }
}
