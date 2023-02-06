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

use crate::agent_lifecycle::AgentLifecycle;

use super::{
    lane_event::{LaneEvent, LaneEventShared},
    on_start::{OnStart, OnStartShared},
    on_stop::{OnStop, OnStopShared},
    utility::HandlerContext,
};

/// An implementation of [AgentLifecycle] with a common state that is shared bewteen all of the
/// lifecycle event handlers.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers run (provides access to the agent lanes).
/// * `State` - The state shared between the event handlers.
/// * `FStart` - The `on_start` event handler.
/// * `FStop` - The `on_stop` event handler.
/// * `LaneEv` - The event handlers for all lanes in the agent.
#[derive(Debug)]
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

impl<Context, State: Clone, FStart: Clone, FStop: Clone, LaneEv: Clone> Clone
    for StatefulAgentLifecycle<Context, State, FStart, FStop, LaneEv>
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            handler_context: self.handler_context,
            on_start: self.on_start.clone(),
            on_stop: self.on_stop.clone(),
            lane_event: self.lane_event.clone(),
        }
    }
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

impl<FStart, FStop, LaneEv, Context, State> OnStart<Context>
    for StatefulAgentLifecycle<Context, State, FStart, FStop, LaneEv>
where
    State: Send,
    FStart: OnStartShared<Context, State>,
    FStop: Send,
    LaneEv: Send,
{
    type OnStartHandler<'a> = FStart::OnStartHandler<'a> where Self: 'a;

    fn on_start(&self) -> Self::OnStartHandler<'_> {
        let StatefulAgentLifecycle {
            state,
            handler_context,
            on_start,
            ..
        } = self;
        on_start.on_start(state, *handler_context)
    }
}

impl<FStart, FStop, LaneEv, Context, State> OnStop<Context>
    for StatefulAgentLifecycle<Context, State, FStart, FStop, LaneEv>
where
    State: Send,
    FStop: OnStopShared<Context, State>,
    FStart: Send,
    LaneEv: Send,
{
    type OnStopHandler<'a> = FStop::OnStopHandler<'a>
    where
        Self: 'a;

    fn on_stop(&self) -> Self::OnStopHandler<'_> {
        let StatefulAgentLifecycle {
            state,
            handler_context,
            on_stop,
            ..
        } = self;
        on_stop.on_stop(state, *handler_context)
    }
}

impl<FStart, FStop, LaneEv, Context, State> LaneEvent<Context>
    for StatefulAgentLifecycle<Context, State, FStart, FStop, LaneEv>
where
    State: Send,
    FStart: Send,
    FStop: Send,
    LaneEv: LaneEventShared<Context, State>,
{
    type LaneEventHandler<'a> = LaneEv::LaneEventHandler<'a>
    where
        Self: 'a;

    fn lane_event<'a>(
        &'a self,
        context: &Context,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler<'a>> {
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
    /// Replace the `on_start` handler with another defined using a closure.
    pub fn on_start<F>(
        self,
        f: F,
    ) -> StatefulAgentLifecycle<Context, State, FnHandler<F>, FStop, LaneEv>
    where
        FnHandler<F>: OnStartShared<Context, State>,
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

    /// Replace the `on_stop` handler with another defined using a closure.
    pub fn on_stop<F>(
        self,
        f: F,
    ) -> StatefulAgentLifecycle<Context, State, FStart, FnHandler<F>, LaneEv>
    where
        FnHandler<F>: OnStopShared<Context, State>,
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

    /// Replace the lane event handlers with another implementation.
    pub fn on_lane_event<H>(
        self,
        handler: H,
    ) -> StatefulAgentLifecycle<Context, State, FStart, FStop, H>
    where
        H: LaneEventShared<Context, State>,
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
