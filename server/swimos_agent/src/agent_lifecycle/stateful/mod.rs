// Copyright 2015-2024 Swim Inc.
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
use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{agent_lifecycle::AgentLifecycle, event_handler::ActionContext, meta::AgentMetadata};

use super::{
    item_event::{ItemEvent, ItemEventShared},
    on_init::{OnInit, OnInitShared},
    on_start::{OnStart, OnStartShared},
    on_stop::{OnStop, OnStopShared},
    utility::HandlerContext,
};

/// An implementation of [AgentLifecycle] with a common state that is shared between all of the
/// lifecycle event handlers.
///
/// # Type Parameters
/// * `Context` - The context within which the event handlers run (provides access to the agent lanes).
/// * `State` - The state shared between the event handlers.
/// * `FStart` - The `on_start` event handler.
/// * `FStop` - The `on_stop` event handler.
/// * `LaneEv` - The event handlers for all lanes in the agent.
#[derive(Debug)]
pub struct StatefulAgentLifecycle<
    Context,
    State,
    FInit = NoHandler,
    FStart = NoHandler,
    FStop = NoHandler,
    ItemEv = NoHandler,
> {
    state: State,
    handler_context: HandlerContext<Context>,
    on_init: FInit,
    on_start: FStart,
    on_stop: FStop,
    item_event: ItemEv,
}

impl<Context, State: Clone, FInit: Clone, FStart: Clone, FStop: Clone, ItemEv: Clone> Clone
    for StatefulAgentLifecycle<Context, State, FInit, FStart, FStop, ItemEv>
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            handler_context: self.handler_context,
            on_init: self.on_init.clone(),
            on_start: self.on_start.clone(),
            on_stop: self.on_stop.clone(),
            item_event: self.item_event.clone(),
        }
    }
}

assert_impl_all!(StatefulAgentLifecycle<(), ()>: AgentLifecycle<()>, Send);

impl<Context, State> StatefulAgentLifecycle<Context, State> {
    pub fn new(state: State) -> Self {
        StatefulAgentLifecycle {
            state,
            handler_context: HandlerContext::default(),
            on_init: NoHandler,
            on_start: NoHandler,
            on_stop: NoHandler,
            item_event: NoHandler,
        }
    }
}

impl<FInit, FStart, FStop, ItemEv, Context, State> OnInit<Context>
    for StatefulAgentLifecycle<Context, State, FInit, FStart, FStop, ItemEv>
where
    State: Send,
    FInit: OnInitShared<Context, State>,
    FStart: Send,
    FStop: Send,
    ItemEv: Send,
{
    fn initialize(
        &self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) {
        let StatefulAgentLifecycle { state, on_init, .. } = self;
        on_init.initialize(state, action_context, meta, context)
    }
}

impl<FInit, FStart, FStop, ItemEv, Context, State> OnStart<Context>
    for StatefulAgentLifecycle<Context, State, FInit, FStart, FStop, ItemEv>
where
    State: Send,
    FInit: Send,
    FStart: OnStartShared<Context, State>,
    FStop: Send,
    ItemEv: Send,
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

impl<FInit, FStart, FStop, ItemEv, Context, State> OnStop<Context>
    for StatefulAgentLifecycle<Context, State, FInit, FStart, FStop, ItemEv>
where
    State: Send,
    FInit: Send,
    FStop: OnStopShared<Context, State>,
    FStart: Send,
    ItemEv: Send,
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

impl<FInit, FStart, FStop, ItemEv, Context, State> ItemEvent<Context>
    for StatefulAgentLifecycle<Context, State, FInit, FStart, FStop, ItemEv>
where
    FInit: Send,
    State: Send,
    FStart: Send,
    FStop: Send,
    ItemEv: ItemEventShared<Context, State>,
{
    type ItemEventHandler<'a> = ItemEv::ItemEventHandler<'a>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        let StatefulAgentLifecycle {
            state,
            handler_context,
            item_event,
            ..
        } = self;
        item_event.item_event(state, *handler_context, context, item_name)
    }
}

impl<Context, State, FInit, FStart, FStop, ItemEv>
    StatefulAgentLifecycle<Context, State, FInit, FStart, FStop, ItemEv>
{
    /// Replace the `on_start` handler with another defined using a closure.
    pub fn on_init<H>(
        self,
        handler: H,
    ) -> StatefulAgentLifecycle<Context, State, H, FStart, FStop, ItemEv>
    where
        H: OnInitShared<Context, State>,
    {
        let StatefulAgentLifecycle {
            handler_context,
            state,
            on_start,
            on_stop,
            item_event,
            ..
        } = self;
        StatefulAgentLifecycle {
            handler_context,
            state,
            on_init: handler,
            on_start,
            on_stop,
            item_event,
        }
    }

    /// Replace the `on_start` handler with another defined using a closure.
    pub fn on_start<F>(
        self,
        f: F,
    ) -> StatefulAgentLifecycle<Context, State, FInit, FnHandler<F>, FStop, ItemEv>
    where
        FnHandler<F>: OnStartShared<Context, State>,
    {
        let StatefulAgentLifecycle {
            handler_context,
            state,
            on_init,
            on_stop,
            item_event,
            ..
        } = self;
        StatefulAgentLifecycle {
            handler_context,
            state,
            on_init,
            on_start: FnHandler(f),
            on_stop,
            item_event,
        }
    }

    /// Replace the `on_stop` handler with another defined using a closure.
    pub fn on_stop<F>(
        self,
        f: F,
    ) -> StatefulAgentLifecycle<Context, State, FInit, FStart, FnHandler<F>, ItemEv>
    where
        FnHandler<F>: OnStopShared<Context, State>,
    {
        let StatefulAgentLifecycle {
            handler_context,
            state,
            on_init,
            on_start,
            item_event,
            ..
        } = self;
        StatefulAgentLifecycle {
            handler_context,
            state,
            on_init,
            on_start,
            on_stop: FnHandler(f),
            item_event,
        }
    }

    /// Replace the lane event handlers with another implementation.
    pub fn on_lane_event<H>(
        self,
        handler: H,
    ) -> StatefulAgentLifecycle<Context, State, FInit, FStart, FStop, H>
    where
        H: ItemEventShared<Context, State>,
    {
        let StatefulAgentLifecycle {
            handler_context,
            state,
            on_init,
            on_start,
            on_stop,
            ..
        } = self;
        StatefulAgentLifecycle {
            handler_context,
            state,
            on_init,
            on_start,
            on_stop,
            item_event: handler,
        }
    }
}
