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

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, HandlerFn0, UnitHandler},
};

use crate::lifecycle_fn::{LiftShared, WithHandlerContext};

/// Lifecycle event for the `on_linked` event of a downlink, from an agent.
pub trait OnLinked<Context>: Send {
    type OnLinkedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_>;
}

/// Lifecycle event for the `on_linked` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnLinkedShared<Context, Shared>: Send {
    type OnLinkedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn on_linked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnLinkedHandler<'a>;
}

impl<Context> OnLinked<Context> for NoHandler {
    type OnLinkedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        UnitHandler::default()
    }
}

impl<Context, Shared> OnLinkedShared<Context, Shared> for NoHandler {
    type OnLinkedHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnLinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<Context, F, H> OnLinked<Context> for FnHandler<F>
where
    F: Fn() -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnLinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let FnHandler(f) = self;
        f()
    }
}

impl<Context, Shared, F> OnLinkedShared<Context, Shared> for FnHandler<F>
where
    F: for<'a> HandlerFn0<'a, Context, Shared> + Send,
{
    type OnLinkedHandler<'a> = <F as HandlerFn0<'a, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnLinkedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context)
    }
}

impl<Context, F, H> OnLinked<Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnLinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let WithHandlerContext { inner } = self;
        inner(Default::default())
    }
}

impl<Context, Shared, F> OnLinkedShared<Context, Shared> for LiftShared<F, Shared>
where
    F: OnLinked<Context> + Send,
{
    type OnLinkedHandler<'a> = F::OnLinkedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnLinkedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_linked()
    }
}
