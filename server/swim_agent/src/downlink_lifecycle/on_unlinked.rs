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

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, UnitHandler, HandlerFn0},
};

use super::{LiftShared, WithHandlerContext};

/// Lifecycle event for the `on_unlinked` event of a downlink, from an agent.
pub trait OnUnlinked<Context>: Send {
    type OnUnlinkedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn on_unlinked<'a>(&'a self) -> Self::OnUnlinkedHandler<'a>;
}

/// Lifecycle event for the `on_unlinked` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnUnlinkedShared<Context, Shared>: Send {
    type OnUnlinkedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn on_unlinked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnUnlinkedHandler<'a>;
}

impl<Context> OnUnlinked<Context> for NoHandler {
    type OnUnlinkedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_unlinked<'a>(&'a self) -> Self::OnUnlinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<Context, Shared> OnUnlinkedShared<Context, Shared> for NoHandler {
    type OnUnlinkedHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnUnlinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<Context, F, H> OnUnlinked<Context> for FnHandler<F>
where
    F: Fn() -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUnlinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_unlinked<'a>(&'a self) -> Self::OnUnlinkedHandler<'a> {
        let FnHandler(f) = self;
        f()
    }
}

impl<Context, Shared, F> OnUnlinkedShared<Context, Shared> for FnHandler<F>
where
    F: for<'a> HandlerFn0<'a, Context, Shared> + Send,
{
    type OnUnlinkedHandler<'a> = <F as HandlerFn0<'a, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnUnlinkedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context)
    }
}

impl<Context, F, H> OnUnlinked<Context> for WithHandlerContext<Context, F>
where
    F: Fn(HandlerContext<Context>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUnlinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_unlinked<'a>(&'a self) -> Self::OnUnlinkedHandler<'a> {
        let WithHandlerContext {
            inner,
            handler_context,
        } = self;
        inner(*handler_context)
    }
}

impl<Context, Shared, F> OnUnlinkedShared<Context, Shared> for LiftShared<F, Shared>
where
    F: OnUnlinked<Context> + Send,
{
    type OnUnlinkedHandler<'a> = F::OnUnlinkedHandler<'a>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnUnlinkedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_unlinked()
    }
}
