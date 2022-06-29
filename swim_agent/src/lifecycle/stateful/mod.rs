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

use static_assertions::assert_impl_all;
use swim_api::handlers::{FnHandler, NoHandler};

use crate::lifecycle::AgentLifecycle;

use super::{
    lane_event::{LaneEvent, LaneEventShared},
    on_start::{OnStart, OnStartShared},
    on_stop::{OnStop, OnStopShared},
    utility::HandlerContext,
};

pub struct StatefulAgentLifecycle<
    Context,
    State,
    FStart = NoHandler,
    FStop = NoHandler,
    LaneEv = NoHandler,
> {
    state: State,
    handler_context: HandlerContext<Context>,
    on_start: FStart,
    on_stop: FStop,
    lane_event: LaneEv,
}

assert_impl_all!(StatefulAgentLifecycle<(), ()>: AgentLifecycle<()>, Send);

impl<Context, State> StatefulAgentLifecycle<Context, State> {
    pub fn new(state: State) -> Self {
        StatefulAgentLifecycle {
            state,
            handler_context: HandlerContext::default(),
            on_start: NoHandler::default(),
            on_stop: NoHandler::default(),
            lane_event: NoHandler::default(),
        }
    }
}

impl<'a, FStart, FStop, LaneEv, Context, State> OnStart<'a, Context>
    for StatefulAgentLifecycle<Context, State, FStart, FStop, LaneEv>
where
    State: Send,
    FStart: OnStartShared<'a, Context, State>,
    FStop: Send,
    LaneEv: Send,
{
    type OnStartHandler = FStart::OnStartHandler;

    fn on_start(&'a self) -> Self::OnStartHandler {
        let StatefulAgentLifecycle {
            state,
            handler_context,
            on_start,
            ..
        } = self;
        on_start.on_start(state, *handler_context)
    }
}

impl<'a, FStart, FStop, LaneEv, Context, State> OnStop<'a, Context>
    for StatefulAgentLifecycle<Context, State, FStart, FStop, LaneEv>
where
    State: Send,
    FStop: OnStopShared<'a, Context, State>,
    FStart: Send,
    LaneEv: Send,
{
    type OnStopHandler = FStop::OnStopHandler;

    fn on_stop(&'a self) -> Self::OnStopHandler {
        let StatefulAgentLifecycle {
            state,
            handler_context,
            on_stop,
            ..
        } = self;
        on_stop.on_stop(state, *handler_context)
    }
}

impl<'a, FStart, FStop, LaneEv, Context, State> LaneEvent<'a, Context>
    for StatefulAgentLifecycle<Context, State, FStart, FStop, LaneEv>
where
    State: Send,
    FStart: Send,
    FStop: Send,
    LaneEv: LaneEventShared<'a, Context, State>,
{
    type LaneEventHandler = LaneEv::LaneEventHandler;

    fn lane_event(&'a self, context: &Context, lane_name: &str) -> Option<Self::LaneEventHandler> {
        let StatefulAgentLifecycle {
            state,
            handler_context,
            lane_event,
            ..
        } = self;
        lane_event.lane_event(state, *handler_context, context, lane_name)
    }
}

impl<Context, State, FStart, FStop, LaneEv>
    StatefulAgentLifecycle<Context, State, FStart, FStop, LaneEv>
{
    pub fn on_start<F>(
        self,
        f: F,
    ) -> StatefulAgentLifecycle<Context, State, FnHandler<F>, FStop, LaneEv>
    where
        FnHandler<F>: for<'a> OnStartShared<'a, Context, State>,
    {
        let StatefulAgentLifecycle {
            handler_context,
            state,
            on_stop,
            lane_event,
            ..
        } = self;
        StatefulAgentLifecycle {
            handler_context,
            state,
            on_start: FnHandler(f),
            on_stop,
            lane_event,
        }
    }

    pub fn on_stop<F>(
        self,
        f: F,
    ) -> StatefulAgentLifecycle<Context, State, FStart, FnHandler<F>, LaneEv>
    where
        FnHandler<F>: for<'a> OnStopShared<'a, Context, State>,
    {
        let StatefulAgentLifecycle {
            handler_context,
            state,
            on_start,
            lane_event,
            ..
        } = self;
        StatefulAgentLifecycle {
            handler_context,
            state,
            on_start,
            on_stop: FnHandler(f),
            lane_event,
        }
    }

    pub fn on_lane_event<H>(
        self,
        handler: H,
    ) -> StatefulAgentLifecycle<Context, State, FStart, FStop, H>
    where
        H: for<'a> LaneEventShared<'a, Context, State>,
    {
        let StatefulAgentLifecycle {
            handler_context,
            state,
            on_start,
            on_stop,
            ..
        } = self;
        StatefulAgentLifecycle {
            handler_context,
            state,
            on_start,
            on_stop,
            lane_event: handler,
        }
    }
}
