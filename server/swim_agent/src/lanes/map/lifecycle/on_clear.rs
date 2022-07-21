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

use std::collections::HashMap;

use swim_api::handlers::{FnHandler, NoHandler};

use crate::{
    event_handler::{EventHandler, UnitHandler},
    lifecycle::utility::HandlerContext,
};

/// Lifecycle event for the `on_clear` event of a map lane.
pub trait OnClear<'a, K, V, Context>: Send {
    type OnClearHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `before` - The contents of the map before it was cleared.
    fn on_clear(&'a self, before: HashMap<K, V>) -> Self::OnClearHandler;
}

/// Lifecycle event for the `on_clear` event of a map lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnClearShared<'a, K, V, Context, Shared>: Send {
    type OnClearHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `before` - The contents of the map before it was cleared.
    fn on_clear(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        before: HashMap<K, V>,
    ) -> Self::OnClearHandler;
}

impl<'a, K, V, Context> OnClear<'a, K, V, Context> for NoHandler {
    type OnClearHandler = UnitHandler;

    fn on_clear(&'a self, _before: HashMap<K, V>) -> Self::OnClearHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, Shared> OnClearShared<'a, K, V, Context, Shared> for NoHandler {
    type OnClearHandler = UnitHandler;

    fn on_clear(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _before: HashMap<K, V>,
    ) -> Self::OnClearHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, F, H> OnClear<'a, K, V, Context> for FnHandler<F>
where
    F: Fn(HashMap<K, V>) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnClearHandler = H;

    fn on_clear(&'a self, before: HashMap<K, V>) -> Self::OnClearHandler {
        let FnHandler(f) = self;
        f(before)
    }
}

impl<'a, K, V, Context, Shared, F, H> OnClearShared<'a, K, V, Context, Shared> for FnHandler<F>
where
    Shared: 'static,
    F: Fn(&'a Shared, HandlerContext<Context>, HashMap<K, V>) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnClearHandler = H;

    fn on_clear(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        before: HashMap<K, V>,
    ) -> Self::OnClearHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, before)
    }
}
