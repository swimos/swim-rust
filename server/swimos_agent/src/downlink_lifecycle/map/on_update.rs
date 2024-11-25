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

use std::borrow::Borrow;

use swimos_utilities::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, MapUpdateBorrowFn, MapUpdateFn, UnitHandler},
    lifecycle_fn::{LiftShared, WithHandlerContext, WithHandlerContextBorrow},
};

/// Lifecycle event for the `on_update` event of a downlink, from an agent.
pub trait OnDownlinkUpdate<K, V, M, Context>: Send {
    type OnUpdateHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `key` - The key that has been updated.
    /// * `map` - The current state of the map.
    /// * `previous` - The previous value, if any.
    /// * `new_value` - The new value of the entry.
    fn on_update<'a>(
        &'a self,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a>;
}

/// Lifecycle event for the `on_update` event of a downlink, from an agent, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkUpdateShared<K, V, M, Context, Shared>: Send {
    type OnUpdateHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `key` - The key that has been updated.
    /// * `map` - The current state of the map.
    /// * `previous` - The previous value, if any.
    /// * `new_value` - The new value of the entry.
    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a>;
}

impl<K, V, M, Context> OnDownlinkUpdate<K, V, M, Context> for NoHandler {
    type OnUpdateHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        _key: K,
        _map: &M,
        _previous: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, M, Context, Shared> OnDownlinkUpdateShared<K, V, M, Context, Shared> for NoHandler {
    type OnUpdateHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _key: K,
        _map: &M,
        _previous: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, M, Context, F, H> OnDownlinkUpdate<K, V, M, Context> for FnHandler<F>
where
    F: Fn(K, &M, Option<V>, &V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a> = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let FnHandler(f) = self;
        f(key, map, previous, new_value)
    }
}

impl<B, K, V, M, Context, F, H> OnDownlinkUpdate<K, V, M, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: Fn(K, &M, Option<V>, &B) -> H + Send + 'static,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a> = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        (self.as_ref())(key, map, previous, new_value.borrow())
    }
}

impl<Context, K, V, M, F, H> OnDownlinkUpdate<K, V, M, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, K, &M, Option<V>, &V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a> = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(Default::default(), key, map, previous, new_value)
    }
}

impl<Context, K, V, M, B, F, H> OnDownlinkUpdate<K, V, M, Context>
    for WithHandlerContextBorrow<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: Fn(HandlerContext<Context>, K, &M, Option<V>, &B) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a> = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let WithHandlerContextBorrow { inner, .. } = self;
        inner(Default::default(), key, map, previous, new_value.borrow())
    }
}

impl<K, V, M, Context, Shared, F> OnDownlinkUpdateShared<K, V, M, Context, Shared> for FnHandler<F>
where
    F: for<'a> MapUpdateFn<'a, Context, Shared, K, V, M> + Send,
{
    type OnUpdateHandler<'a> = <F as MapUpdateFn<'a, Context, Shared, K, V, M>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map, key, previous, new_value)
    }
}

impl<B, K, V, M, Context, Shared, F> OnDownlinkUpdateShared<K, V, M, Context, Shared>
    for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: for<'a> MapUpdateBorrowFn<'a, Context, Shared, K, V, M, B> + Send,
{
    type OnUpdateHandler<'a> = <F as MapUpdateBorrowFn<'a, Context, Shared, K, V, M, B>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        (self.as_ref()).make_handler(
            shared,
            handler_context,
            map,
            key,
            previous,
            new_value.borrow(),
        )
    }
}

impl<K, V, M, Context, Shared, F> OnDownlinkUpdateShared<K, V, M, Context, Shared>
    for LiftShared<F, Shared>
where
    F: OnDownlinkUpdate<K, V, M, Context> + Send,
{
    type OnUpdateHandler<'a> = F::OnUpdateHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        key: K,
        map: &M,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_update(key, map, previous, new_value)
    }
}
