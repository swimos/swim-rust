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
    event_handler::{EventHandler, MapRemoveFn, UnitHandler},
};

/// Lifecycle event for the `on_remove` event of a map lane.
pub trait OnRemove<K, V, M, Context>: Send {
    type OnRemoveHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `map` - The current contents of the map.
    /// * `key` - The key that was removed.
    /// * `prev_value` - The value that was removed.
    fn on_remove<'a>(&'a self, map: &M, key: K, prev_value: V) -> Self::OnRemoveHandler<'a>;
}

/// Lifecycle event for the `on_remove` event of a map lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnRemoveShared<K, V, M, Context, Shared>: Send {
    type OnRemoveHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `map` - The current contents of the map.
    /// * `key` - The key that was removed.
    /// * `prev_value` - The value that was removed.
    fn on_remove<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &M,
        key: K,
        prev_value: V,
    ) -> Self::OnRemoveHandler<'a>;
}

impl<K, V, M, Context> OnRemove<K, V, M, Context> for NoHandler {
    type OnRemoveHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_remove<'a>(&'a self, _map: &M, _key: K, _prev_value: V) -> Self::OnRemoveHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, M, Context, Shared> OnRemoveShared<K, V, M, Context, Shared> for NoHandler {
    type OnRemoveHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: &M,
        _key: K,
        _prev_value: V,
    ) -> Self::OnRemoveHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, M, Context, F, H> OnRemove<K, V, M, Context> for FnHandler<F>
where
    F: Fn(&M, K, V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnRemoveHandler<'a> = H
    where
        Self: 'a;

    fn on_remove<'a>(&'a self, map: &M, key: K, prev_value: V) -> Self::OnRemoveHandler<'a> {
        let FnHandler(f) = self;
        f(map, key, prev_value)
    }
}

impl<K, V, M, Context, Shared, F> OnRemoveShared<K, V, M, Context, Shared> for FnHandler<F>
where
    F: for<'a> MapRemoveFn<'a, Context, Shared, K, V, M> + Send,
{
    type OnRemoveHandler<'a> = <F as MapRemoveFn<'a, Context, Shared, K, V, M>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &M,
        key: K,
        prev_value: V,
    ) -> Self::OnRemoveHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map, key, prev_value)
    }
}
