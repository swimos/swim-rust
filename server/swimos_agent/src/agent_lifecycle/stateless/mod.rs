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

use std::marker::PhantomData;

use static_assertions::assert_impl_all;
use swimos_api::handlers::{FnHandler, NoHandler};

use crate::agent_lifecycle::AgentLifecycle;

use super::{item_event::ItemEvent, on_init::OnInit, on_start::OnStart, on_stop::OnStop};

/// An implementation of [AgentLifecycle] with no shared state.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers run (provides access to the agent lanes).
/// * `FInit` - Initializer called before any event handlers run.
/// * `FStart` - The `on_start` event handler.
/// * `FStop` - The `on_stop` event handler.
/// * `LaneEv` - The event handlers for all lanes in the agent.
#[derive(Debug)]
pub struct BasicAgentLifecycle<
    Context,
    FInit = NoHandler,
    FStart = NoHandler,
    FStop = NoHandler,
    ItemEv = NoHandler,
> {
    _context: PhantomData<fn(Context)>,
    on_init: FInit,
    on_start: FStart,
    on_stop: FStop,
    item_event: ItemEv,
}

impl<Context> Default for BasicAgentLifecycle<Context> {
    fn default() -> Self {
        Self {
            _context: Default::default(),
            on_init: Default::default(),
            on_start: Default::default(),
            on_stop: Default::default(),
            item_event: Default::default(),
        }
    }
}

assert_impl_all!(BasicAgentLifecycle<()>: AgentLifecycle<()>, Send);

impl<FInit, FStart, FStop, ItemEv, Context> OnInit<Context>
    for BasicAgentLifecycle<Context, FInit, FStart, FStop, ItemEv>
where
    FInit: OnInit<Context>,
    FStart: Send,
    FStop: Send,
    ItemEv: Send,
{
    fn initialize(
        &self,
        action_context: &mut crate::event_handler::ActionContext<Context>,
        meta: crate::meta::AgentMetadata,
        context: &Context,
    ) {
        self.on_init.initialize(action_context, meta, context)
    }
}

impl<FInit, FStart, FStop, ItemEv, Context> OnStart<Context>
    for BasicAgentLifecycle<Context, FInit, FStart, FStop, ItemEv>
where
    FInit: Send,
    FStart: OnStart<Context>,
    FStop: Send,
    ItemEv: Send,
{
    type OnStartHandler<'a> = FStart::OnStartHandler<'a> where Self: 'a;

    fn on_start(&self) -> Self::OnStartHandler<'_> {
        self.on_start.on_start()
    }
}

impl<FInit, FStart, FStop, ItemEv, Context> OnStop<Context>
    for BasicAgentLifecycle<Context, FInit, FStart, FStop, ItemEv>
where
    FInit: Send,
    FStop: OnStop<Context>,
    FStart: Send,
    ItemEv: Send,
{
    type OnStopHandler<'a> = FStop::OnStopHandler<'a>
    where
        Self: 'a;

    fn on_stop(&self) -> Self::OnStopHandler<'_> {
        self.on_stop.on_stop()
    }
}

impl<FInit, FStart, FStop, ItemEv, Context> ItemEvent<Context>
    for BasicAgentLifecycle<Context, FInit, FStart, FStop, ItemEv>
where
    FInit: Send,
    FStop: Send,
    FStart: Send,
    ItemEv: ItemEvent<Context>,
{
    type ItemEventHandler<'a> = ItemEv::ItemEventHandler<'a>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        self.item_event.item_event(context, item_name)
    }
}

impl<Context, FInit, FStart, FStop, ItemEv>
    BasicAgentLifecycle<Context, FInit, FStart, FStop, ItemEv>
{
    pub fn on_init<H>(self, handler: H) -> BasicAgentLifecycle<Context, H, FStart, FStop, ItemEv>
    where
        H: OnInit<Context>,
    {
        let BasicAgentLifecycle {
            on_start,
            on_stop,
            item_event,
            ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_init: handler,
            on_start,
            on_stop,
            item_event,
        }
    }

    pub fn on_start<F>(
        self,
        f: F,
    ) -> BasicAgentLifecycle<Context, FInit, FnHandler<F>, FStop, ItemEv>
    where
        FnHandler<F>: OnStart<Context>,
    {
        let BasicAgentLifecycle {
            on_init,
            on_stop,
            item_event,
            ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_init,
            on_start: FnHandler(f),
            on_stop,
            item_event,
        }
    }

    pub fn on_stop<F>(
        self,
        f: F,
    ) -> BasicAgentLifecycle<Context, FInit, FStart, FnHandler<F>, ItemEv>
    where
        FnHandler<F>: OnStop<Context>,
    {
        let BasicAgentLifecycle {
            on_init,
            on_start,
            item_event,
            ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_init,
            on_start,
            on_stop: FnHandler(f),
            item_event,
        }
    }

    pub fn on_lane_event<H>(
        self,
        handler: H,
    ) -> BasicAgentLifecycle<Context, FInit, FStart, FStop, H>
    where
        H: ItemEvent<Context>,
    {
        let BasicAgentLifecycle {
            on_init,
            on_start,
            on_stop,
            ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_init,
            on_start,
            on_stop,
            item_event: handler,
        }
    }
}
