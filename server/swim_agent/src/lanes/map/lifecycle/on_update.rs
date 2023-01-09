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
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, MapUpdateFn, UnitHandler},
};

/// Lifecycle event for the `on_update` event of a map lane.
pub trait OnUpdate<K, V, Context>: Send {
    type OnUpdateHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `map` - The current contents of the map.
    /// * `key` - The key that was removed.
    /// * `prev_value` - The value that was replaced (if any).
    /// * `new_value` - The updated value.
    fn on_update<'a>(
        &'a self,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a>;
}

/// Lifecycle event for the `on_update` event of a map lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnUpdateShared<K, V, Context, Shared>: Send {
    type OnUpdateHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `map` - The current contents of the map.
    /// * `key` - The key that was removed.
    /// * `prev_value` - The value that was replaced (if any).
    /// * `new_value` - The updated value.
    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a>;
}

impl<K, V, Context> OnUpdate<K, V, Context> for NoHandler {
    type OnUpdateHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        _map: &HashMap<K, V>,
        _key: K,
        _prev_value: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, Context, Shared> OnUpdateShared<K, V, Context, Shared> for NoHandler {
    type OnUpdateHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: &HashMap<K, V>,
        _key: K,
        _prev_value: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, Context, F, H> OnUpdate<K, V, Context> for FnHandler<F>
where
    F: Fn(&HashMap<K, V>, K, Option<V>, &V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a> = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let FnHandler(f) = self;
        f(map, key, prev_value, new_value)
    }
}

impl<K, V, Context, Shared, F> OnUpdateShared<K, V, Context, Shared> for FnHandler<F>
where
    F: for<'a> MapUpdateFn<'a, Context, Shared, K, V> + Send,
{
    type OnUpdateHandler<'a> = <F as MapUpdateFn<'a, Context, Shared, K, V>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map, key, prev_value, new_value)
    }
}
