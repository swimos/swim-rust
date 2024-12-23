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
};

/// Lifecycle event for the `on_clear` event of a map lane.
pub trait OnClear<M, Context>: Send {
    type OnClearHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `before` - The contents of the map before it was cleared.
    fn on_clear(&self, before: M) -> Self::OnClearHandler<'_>;
}

/// Lifecycle event for the `on_clear` event of a map lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnClearShared<M, Context, Shared>: Send {
    type OnClearHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `before` - The contents of the map before it was cleared.
    fn on_clear<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        before: M,
    ) -> Self::OnClearHandler<'a>;
}

impl<M, Context> OnClear<M, Context> for NoHandler {
    type OnClearHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_clear(&self, _before: M) -> Self::OnClearHandler<'_> {
        UnitHandler::default()
    }
}

impl<M, Context, Shared> OnClearShared<M, Context, Shared> for NoHandler {
    type OnClearHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _before: M,
    ) -> Self::OnClearHandler<'a> {
        UnitHandler::default()
    }
}

impl<M, Context, F, H> OnClear<M, Context> for FnHandler<F>
where
    F: Fn(M) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnClearHandler<'a> = H
    where
        Self: 'a;

    fn on_clear(&self, before: M) -> Self::OnClearHandler<'_> {
        let FnHandler(f) = self;
        f(before)
    }
}

impl<M, Context, Shared, F> OnClearShared<M, Context, Shared> for FnHandler<F>
where
    F: for<'a> TakeFn<'a, Context, Shared, M> + Send,
{
    type OnClearHandler<'a> = <F as TakeFn<'a, Context, Shared, M>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        before: M,
    ) -> Self::OnClearHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, before)
    }
}
