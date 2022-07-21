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
    event_handler::{HandlerAction, UnitHandler},
    lifecycle::utility::HandlerContext,
};

/// Lifecycle event for the `on_remove` event of a map lane.
pub trait OnRemove<'a, K, V, Context>: Send {
    type OnRemoveHandler: HandlerAction<Context, Completion = ()> + 'a;

    /// #Arguments
    /// * `map` - The current contents of the map.
    /// * `key` - The key that was removed.
    /// * `prev_value` - The value that was removed.
    fn on_remove(&'a self, map: &HashMap<K, V>, key: K, prev_value: V) -> Self::OnRemoveHandler;
}

/// Lifecycle event for the `on_remove` event of a map lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnRemoveShared<'a, K, V, Context, Shared>: Send {
    type OnRemoveHandler: HandlerAction<Context, Completion = ()> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `map` - The current contents of the map.
    /// * `key` - The key that was removed.
    /// * `prev_value` - The value that was removed.
    fn on_remove(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: V,
    ) -> Self::OnRemoveHandler;
}

impl<'a, K, V, Context> OnRemove<'a, K, V, Context> for NoHandler {
    type OnRemoveHandler = UnitHandler;

    fn on_remove(&'a self, _map: &HashMap<K, V>, _key: K, _prev_value: V) -> Self::OnRemoveHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, Shared> OnRemoveShared<'a, K, V, Context, Shared> for NoHandler {
    type OnRemoveHandler = UnitHandler;

    fn on_remove(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: &HashMap<K, V>,
        _key: K,
        _prev_value: V,
    ) -> Self::OnRemoveHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, F, H> OnRemove<'a, K, V, Context> for FnHandler<F>
where
    F: Fn(&HashMap<K, V>, K, V) -> H + Send,
    H: HandlerAction<Context, Completion = ()> + 'a,
{
    type OnRemoveHandler = H;

    fn on_remove(&'a self, map: &HashMap<K, V>, key: K, prev_value: V) -> Self::OnRemoveHandler {
        let FnHandler(f) = self;
        f(map, key, prev_value)
    }
}

impl<'a, K, V, Context, Shared, F, H> OnRemoveShared<'a, K, V, Context, Shared> for FnHandler<F>
where
    Shared: 'static,
    F: Fn(&'a Shared, HandlerContext<Context>, &HashMap<K, V>, K, V) -> H + Send,
    H: HandlerAction<Context, Completion = ()> + 'a,
{
    type OnRemoveHandler = H;

    fn on_remove(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: V,
    ) -> Self::OnRemoveHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, map, key, prev_value)
    }
}
