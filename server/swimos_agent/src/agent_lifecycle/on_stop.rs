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

use crate::event_handler::{EventHandler, HandlerFn0, UnitHandler};

use super::utility::HandlerContext;

/// Lifecycle event for the `on_stop` event of an agent.
pub trait OnStop<Context>: Send {
    fn on_stop(&self) -> impl EventHandler<Context> + '_;
}

/// Lifecycle event for the `on_stop` event of an agent where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnStopShared<Context, Shared>: Send {
    type OnStopHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn on_stop<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnStopHandler<'a>;
}

impl<Context> OnStop<Context> for NoHandler {
    fn on_stop(&self) -> impl EventHandler<Context> + '_ {
        UnitHandler::default()
    }
}

impl<Context, Shared> OnStopShared<Context, Shared> for NoHandler {
    type OnStopHandler<'a>
        = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_stop<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnStopHandler<'a> {
        Default::default()
    }
}

impl<Context, F, H> OnStop<Context> for FnHandler<F>
where
    F: Fn() -> H + Send,
    H: EventHandler<Context> + 'static,
{
    fn on_stop(&self) -> impl EventHandler<Context> + '_ {
        let FnHandler(f) = self;
        f()
    }
}

impl<Context, F, Shared> OnStopShared<Context, Shared> for FnHandler<F>
where
    F: for<'a> HandlerFn0<'a, Context, Shared> + Send,
{
    type OnStopHandler<'a>
        = <F as HandlerFn0<'a, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_stop<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnStopHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context)
    }
}
