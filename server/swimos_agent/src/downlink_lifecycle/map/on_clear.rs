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

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, TakeFn, UnitHandler},
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

/// Lifecycle event for the `on_clear` event of a downlink, from an agent.
pub trait OnDownlinkClear<M, Context>: Send {
    type OnClearHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `map` - The old state of the map.
    fn on_clear(&self, map: M) -> Self::OnClearHandler<'_>;
}

/// Lifecycle event for the `on_clear` event of a downlink, from an agent, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkClearShared<M, Context, Shared>: Send {
    type OnClearHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `map` - The old state of the map.
    fn on_clear<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: M,
    ) -> Self::OnClearHandler<'a>;
}

impl<M, Context> OnDownlinkClear<M, Context> for NoHandler {
    type OnClearHandler<'a>
        = UnitHandler
    where
        Self: 'a;

    fn on_clear(&self, _map: M) -> Self::OnClearHandler<'_> {
        UnitHandler::default()
    }
}

impl<M, Context, Shared> OnDownlinkClearShared<M, Context, Shared> for NoHandler {
    type OnClearHandler<'a>
        = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: M,
    ) -> Self::OnClearHandler<'a> {
        UnitHandler::default()
    }
}

impl<M, Context, F, H> OnDownlinkClear<M, Context> for FnHandler<F>
where
    F: Fn(M) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnClearHandler<'a>
        = H
    where
        Self: 'a;

    fn on_clear(&self, map: M) -> Self::OnClearHandler<'_> {
        let FnHandler(f) = self;
        f(map)
    }
}

impl<M, Context, Shared, F> OnDownlinkClearShared<M, Context, Shared> for FnHandler<F>
where
    F: for<'a> TakeFn<'a, Context, Shared, M> + Send,
{
    type OnClearHandler<'a>
        = <F as TakeFn<'a, Context, Shared, M>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: M,
    ) -> Self::OnClearHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map)
    }
}

impl<Context, M, F, H> OnDownlinkClear<M, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, M) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnClearHandler<'a>
        = H
    where
        Self: 'a;

    fn on_clear(&self, map: M) -> Self::OnClearHandler<'_> {
        let WithHandlerContext { inner } = self;
        inner(Default::default(), map)
    }
}

impl<M, Context, Shared, F> OnDownlinkClearShared<M, Context, Shared> for LiftShared<F, Shared>
where
    F: OnDownlinkClear<M, Context> + Send,
{
    type OnClearHandler<'a>
        = F::OnClearHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        map: M,
    ) -> Self::OnClearHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_clear(map)
    }
}
