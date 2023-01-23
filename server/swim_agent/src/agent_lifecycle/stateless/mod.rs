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

use super::{item_event::ItemEvent, on_start::OnStart, on_stop::OnStop};

/// An implementation of [AgentLifecycle] with no shared state.
///
/// #Type Parameters
/// * `Context` - The context within which the event handlers run (provides access to the agent lanes).
/// * `FStart` - The `on_start` event handler.
/// * `FStop` - The `on_stop` event handler.
/// * `LaneEv` - The event handlers for all lanes in the agent.
#[derive(Debug)]
pub struct BasicAgentLifecycle<Context, FStart = NoHandler, FStop = NoHandler, ItemEv = NoHandler> {
    _context: PhantomData<fn(Context)>,
    on_start: FStart,
    on_stop: FStop,
    item_event: ItemEv,
}

impl<Context> Default for BasicAgentLifecycle<Context> {
    fn default() -> Self {
        Self {
            _context: Default::default(),
            on_start: Default::default(),
            on_stop: Default::default(),
            item_event: Default::default(),
        }
    }
}

assert_impl_all!(BasicAgentLifecycle<()>: AgentLifecycle<()>, Send);

impl<FStart, FStop, ItemEv, Context> OnStart<Context>
    for BasicAgentLifecycle<Context, FStart, FStop, ItemEv>
where
    FStart: OnStart<Context>,
    FStop: Send,
    ItemEv: Send,
{
    type OnStartHandler<'a> = FStart::OnStartHandler<'a> where Self: 'a;

    fn on_start(&self) -> Self::OnStartHandler<'_> {
        self.on_start.on_start()
    }
}

impl<FStart, FStop, ItemEv, Context> OnStop<Context>
    for BasicAgentLifecycle<Context, FStart, FStop, ItemEv>
where
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

impl<FStart, FStop, ItemEv, Context> ItemEvent<Context>
    for BasicAgentLifecycle<Context, FStart, FStop, ItemEv>
where
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

impl<Context, FStart, FStop, ItemEv> BasicAgentLifecycle<Context, FStart, FStop, ItemEv> {
    pub fn on_start<F>(self, f: F) -> BasicAgentLifecycle<Context, FnHandler<F>, FStop, ItemEv>
    where
        FnHandler<F>: OnStart<Context>,
    {
        let BasicAgentLifecycle {
            on_stop,
            item_event,
            ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_start: FnHandler(f),
            on_stop,
            item_event,
        }
    }

    pub fn on_stop<F>(self, f: F) -> BasicAgentLifecycle<Context, FStart, FnHandler<F>, ItemEv>
    where
        FnHandler<F>: OnStop<Context>,
    {
        let BasicAgentLifecycle {
            on_start,
            item_event,
            ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_start,
            on_stop: FnHandler(f),
            item_event,
        }
    }

    pub fn on_lane_event<H>(self, handler: H) -> BasicAgentLifecycle<Context, FStart, FStop, H>
    where
        H: ItemEvent<Context>,
    {
        let BasicAgentLifecycle {
            on_start, on_stop, ..
        } = self;
        BasicAgentLifecycle {
            _context: Default::default(),
            on_start,
            on_stop,
            item_event: handler,
        }
    }
}
