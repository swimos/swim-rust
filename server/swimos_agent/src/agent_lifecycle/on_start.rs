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

use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::event_handler::{EventHandler, HandlerFn0, UnitHandler};

use super::utility::HandlerContext;

/// Lifecycle event for the `on_start` event of an agent.
pub trait OnStart<Context>: Send {
    type OnStartHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn on_start(&self) -> Self::OnStartHandler<'_>;
}

/// Lifecycle event for the `on_start` event of an agent where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnStartShared<Context, Shared>: Send {
    type OnStartHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn on_start<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnStartHandler<'a>;
}

impl<Context> OnStart<Context> for NoHandler {
    type OnStartHandler<'a> = UnitHandler where Self: 'a;

    fn on_start(&self) -> Self::OnStartHandler<'_> {
        Default::default()
    }
}

impl<Context, F, H> OnStart<Context> for FnHandler<F>
where
    F: Fn() -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnStartHandler<'a> = H
    where
        Self: 'a;

    fn on_start(&self) -> Self::OnStartHandler<'_> {
        let FnHandler(f) = self;
        f()
    }
}

impl<Context, Shared> OnStartShared<Context, Shared> for NoHandler {
    type OnStartHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_start<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnStartHandler<'a> {
        Default::default()
    }
}

impl<Context, Shared, F> OnStartShared<Context, Shared> for FnHandler<F>
where
    F: for<'a> HandlerFn0<'a, Context, Shared> + Send,
{
    type OnStartHandler<'a> = <F as HandlerFn0<'a, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_start<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnStartHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context)
    }
}
