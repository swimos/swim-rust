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

use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::event_handler::{EventConsumeFn, EventHandler, UnitHandler};

use super::utility::HandlerContext;

/// Lifecycle event for the `on_timer` event of an agent.
pub trait OnTimer<Context>: Send {
    /// # Arguments
    /// * `timer_id` - An arbitrary ID to distinguish between different timer events.
    fn on_timer(&self, timer_id: u64) -> impl EventHandler<Context> + '_;
}

/// Lifecycle event for the `on_timer` event of an agent where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnTimerShared<Context, Shared>: Send {
    type OnTimerHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `timer_id` - An arbitrary ID to distinguish between different timer events.
    fn on_timer<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        timer_id: u64,
    ) -> Self::OnTimerHandler<'a>;
}

impl<Context> OnTimer<Context> for NoHandler {
    fn on_timer(&self, _timer_id: u64) -> impl EventHandler<Context> + '_ {
        UnitHandler::default()
    }
}

impl<Context, F, H> OnTimer<Context> for FnHandler<F>
where
    F: Fn(u64) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    fn on_timer(&self, timer_id: u64) -> impl EventHandler<Context> + '_ {
        let FnHandler(f) = self;
        f(timer_id)
    }
}

impl<Context, Shared> OnTimerShared<Context, Shared> for NoHandler {
    type OnTimerHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_timer<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _timer_id: u64,
    ) -> Self::OnTimerHandler<'a> {
        Default::default()
    }
}

impl<Context, Shared, F> OnTimerShared<Context, Shared> for FnHandler<F>
where
    F: for<'a> EventConsumeFn<'a, Context, Shared, u64> + Send,
{
    type OnTimerHandler<'a> = <F as EventConsumeFn<'a, Context, Shared, u64>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_timer<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        timer_id: u64,
    ) -> Self::OnTimerHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, timer_id)
    }
}
