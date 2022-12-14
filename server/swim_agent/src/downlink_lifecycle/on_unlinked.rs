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
    event_handler::{EventHandler, UnitHandler},
};

use super::{LiftShared, WithHandlerContext};

/// Lifecycle event for the `on_unlinked` event of a downlink, from an agent.
pub trait OnUnlinked<'a, Context>: Send {
    type OnUnlinkedHandler: EventHandler<Context> + 'a;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler;
}

/// Lifecycle event for the `on_unlinked` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnUnlinkedShared<'a, Context, Shared>: Send {
    type OnUnlinkedHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn on_unlinked(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnUnlinkedHandler;
}

impl<'a, Context> OnUnlinked<'a, Context> for NoHandler {
    type OnUnlinkedHandler = UnitHandler;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler {
        UnitHandler::default()
    }
}

impl<'a, Context, Shared> OnUnlinkedShared<'a, Context, Shared> for NoHandler {
    type OnUnlinkedHandler = UnitHandler;

    fn on_unlinked(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnUnlinkedHandler {
        UnitHandler::default()
    }
}

impl<'a, Context, F, H> OnUnlinked<'a, Context> for FnHandler<F>
where
    F: Fn() -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnUnlinkedHandler = H;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler {
        let FnHandler(f) = self;
        f()
    }
}

impl<'a, Context, Shared, F, H> OnUnlinkedShared<'a, Context, Shared> for FnHandler<F>
where
    F: Fn(&'a Shared, HandlerContext<Context>) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnUnlinkedHandler = H;

    fn on_unlinked(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnUnlinkedHandler {
        let FnHandler(f) = self;
        f(shared, handler_context)
    }
}

impl<'a, Context, F, H> OnUnlinked<'a, Context> for WithHandlerContext<Context, F>
where
    F: Fn(HandlerContext<Context>) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnUnlinkedHandler = H;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler {
        let WithHandlerContext {
            inner,
            handler_context,
        } = self;
        inner(*handler_context)
    }
}

impl<'a, Context, Shared, F> OnUnlinkedShared<'a, Context, Shared> for LiftShared<F, Shared>
where
    F: OnUnlinked<'a, Context> + Send,
{
    type OnUnlinkedHandler = F::OnUnlinkedHandler;

    fn on_unlinked(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnUnlinkedHandler {
        let LiftShared { inner, .. } = self;
        inner.on_unlinked()
    }
}
