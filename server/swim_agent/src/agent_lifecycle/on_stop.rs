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

use swim_api::handlers::{FnHandler, NoHandler};

use crate::event_handler::{EventHandler, UnitHandler};

use super::utility::HandlerContext;

/// Lifecycle event for the `on_stop` event of an agent.
pub trait OnStop<'a, Context>: Send {
    type OnStopHandler: EventHandler<Context> + 'a;

    fn on_stop(&'a self) -> Self::OnStopHandler;
}

/// Lifecycle event for the `on_stop` event of an agent where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnStopShared<'a, Context, Shared>: Send {
    type OnStopHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn on_stop(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnStopHandler;
}

impl<'a, Context> OnStop<'a, Context> for NoHandler {
    type OnStopHandler = UnitHandler;

    fn on_stop(&'a self) -> Self::OnStopHandler {
        Default::default()
    }
}

impl<'a, Context, Shared> OnStopShared<'a, Context, Shared> for NoHandler {
    type OnStopHandler = UnitHandler;

    fn on_stop(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnStopHandler {
        Default::default()
    }
}

impl<'a, Context, F, H> OnStop<'a, Context> for FnHandler<F>
where
    F: Fn() -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnStopHandler = H;

    fn on_stop(&'a self) -> Self::OnStopHandler {
        let FnHandler(f) = self;
        f()
    }
}

impl<'a, Context, F, H, Shared> OnStopShared<'a, Context, Shared> for FnHandler<F>
where
    Shared: 'static,
    F: Fn(&'a Shared, HandlerContext<Context>) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnStopHandler = H;

    fn on_stop(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnStopHandler {
        let FnHandler(f) = self;
        f(shared, handler_context)
    }
}
