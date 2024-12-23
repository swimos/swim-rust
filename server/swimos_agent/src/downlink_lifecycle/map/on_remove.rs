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
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

/// Lifecycle event for the `on_remove` event of a downlink, from an agent.
pub trait OnDownlinkRemove<K, V, M, Context>: Send {
    type OnRemoveHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `key` - The key that has been removed.
    /// * `map` - The current state of the map.
    /// * `removed` - The removed value.
    fn on_remove<'a>(&'a self, key: K, map: &M, removed: V) -> Self::OnRemoveHandler<'a>;
}

/// Lifecycle event for the `on_remove` event of a downlink, from an agent, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkRemoveShared<K, V, M, Context, Shared>: Send {
    type OnRemoveHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `key` - The key that has been remove.
    /// * `map` - The current state of the map.
    /// * `removed` - The removed value.
    fn on_remove<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &M,
        removed: V,
    ) -> Self::OnRemoveHandler<'a>;
}

impl<K, V, M, Context> OnDownlinkRemove<K, V, M, Context> for NoHandler {
    type OnRemoveHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_remove<'a>(&'a self, _key: K, _map: &M, _removed: V) -> Self::OnRemoveHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, M, Context, Shared> OnDownlinkRemoveShared<K, V, M, Context, Shared> for NoHandler {
    type OnRemoveHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _key: K,
        _map: &M,
        _removed: V,
    ) -> Self::OnRemoveHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, M, Context, F, H> OnDownlinkRemove<K, V, M, Context> for FnHandler<F>
where
    F: Fn(K, &M, V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnRemoveHandler<'a> = H
    where
        Self: 'a;

    fn on_remove<'a>(&'a self, key: K, map: &M, removed: V) -> Self::OnRemoveHandler<'a> {
        let FnHandler(f) = self;
        f(key, map, removed)
    }
}

impl<K, V, M, Context, Shared, F> OnDownlinkRemoveShared<K, V, M, Context, Shared> for FnHandler<F>
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
        key: K,
        map: &M,
        removed: V,
    ) -> Self::OnRemoveHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map, key, removed)
    }
}

impl<Context, K, V, M, F, H> OnDownlinkRemove<K, V, M, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, K, &M, V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnRemoveHandler<'a> = H
    where
        Self: 'a;

    fn on_remove<'a>(&'a self, key: K, map: &M, removed: V) -> Self::OnRemoveHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(Default::default(), key, map, removed)
    }
}

impl<K, V, M, Context, Shared, F> OnDownlinkRemoveShared<K, V, M, Context, Shared>
    for LiftShared<F, Shared>
where
    F: OnDownlinkRemove<K, V, M, Context> + Send,
{
    type OnRemoveHandler<'a> = F::OnRemoveHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_remove<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        key: K,
        map: &M,
        removed: V,
    ) -> Self::OnRemoveHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_remove(key, map, removed)
    }
}
