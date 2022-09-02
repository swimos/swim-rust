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
    event_handler::{EventHandler, UnitHandler},
};

/// Lifecycle event for the `on_remove` event of a downlink, from an agent.
pub trait OnDownlinkRemove<'a, K, V, Context>: Send {
    type OnRemoveHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `key` - The key that has been removed.
    /// * `map` - The current state of the map.
    /// * `removed` - The removed value.
    fn on_remove(&'a self, key: K, map: &HashMap<K, V>, removed: V) -> Self::OnRemoveHandler;
}

/// Lifecycle event for the `on_remove` event of a downlink, from an agent, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkRemoveShared<'a, K, V, Context, Shared>: Send {
    type OnRemoveHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `key` - The key that has been remove.
    /// * `map` - The current state of the map.
    /// * `removed` - The removed value.
    fn on_remove(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &HashMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveHandler;
}

impl<'a, K, V, Context> OnDownlinkRemove<'a, K, V, Context> for NoHandler {
    type OnRemoveHandler = UnitHandler;

    fn on_remove(&'a self, _key: K, _map: &HashMap<K, V>, _removed: V) -> Self::OnRemoveHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, Shared> OnDownlinkRemoveShared<'a, K, V, Context, Shared> for NoHandler {
    type OnRemoveHandler = UnitHandler;

    fn on_remove(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _key: K,
        _map: &HashMap<K, V>,
        _removed: V,
    ) -> Self::OnRemoveHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, F, H> OnDownlinkRemove<'a, K, V, Context> for FnHandler<F>
where
    F: Fn(K, &HashMap<K, V>, V) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnRemoveHandler = H;

    fn on_remove(&'a self, key: K, map: &HashMap<K, V>, removed: V) -> Self::OnRemoveHandler {
        let FnHandler(f) = self;
        f(key, map, removed)
    }
}

impl<'a, K, V, Context, Shared, F, H> OnDownlinkRemoveShared<'a, K, V, Context, Shared>
    for FnHandler<F>
where
    F: Fn(&'a Shared, HandlerContext<Context>, K, &HashMap<K, V>, V) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnRemoveHandler = H;

    fn on_remove(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &HashMap<K, V>,
        removed: V,
    ) -> Self::OnRemoveHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, key, map, removed)
    }
}
