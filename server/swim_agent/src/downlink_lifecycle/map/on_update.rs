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

use std::{borrow::Borrow, collections::HashMap};

use swim_api::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    downlink_lifecycle::{LiftShared, WithHandlerContext},
    event_handler::{EventHandler, MapUpdateBorrowFn, MapUpdateFn, UnitHandler},
};

/// Lifecycle event for the `on_update` event of a downlink, from an agent.
pub trait OnDownlinkUpdate<K, V, Context>: Send {
    type OnUpdateHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `key` - The key that has been updated.
    /// * `map` - The current state of the map.
    /// * `previous` - The previous value, if any.
    /// * `new_value` - The new value of the entry.
    fn on_update<'a>(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a>;
}

/// Lifecycle event for the `on_update` event of a downlink, from an agent, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkUpdateShared<K, V, Context, Shared>: Send {
    type OnUpdateHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
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
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a>;
}

impl<K, V, Context> OnDownlinkUpdate<K, V, Context> for NoHandler {
    type OnUpdateHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        _key: K,
        _map: &HashMap<K, V>,
        _previous: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, Context, Shared> OnDownlinkUpdateShared<K, V, Context, Shared> for NoHandler {
    type OnUpdateHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _key: K,
        _map: &HashMap<K, V>,
        _previous: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, Context, F, H> OnDownlinkUpdate<K, V, Context> for FnHandler<F>
where
    F: Fn(K, &HashMap<K, V>, Option<V>, &V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a> = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let FnHandler(f) = self;
        f(key, map, previous, new_value)
    }
}

impl<B, K, V, Context, F, H> OnDownlinkUpdate<K, V, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: Fn(K, &HashMap<K, V>, Option<V>, &B) -> H + Send + 'static,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a> = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        (self.as_ref())(key, map, previous, new_value.borrow())
    }
}

impl<Context, K, V, F, H> OnDownlinkUpdate<K, V, Context> for WithHandlerContext<Context, F>
where
    F: Fn(HandlerContext<Context>, K, &HashMap<K, V>, Option<V>, &V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a> = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let WithHandlerContext {
            inner,
            handler_context,
        } = self;
        inner(*handler_context, key, map, previous, new_value)
    }
}

impl<K, V, Context, Shared, F> OnDownlinkUpdateShared<K, V, Context, Shared> for FnHandler<F>
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
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map, key, previous, new_value)
    }
}

impl<B, K, V, Context, Shared, F> OnDownlinkUpdateShared<K, V, Context, Shared>
    for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: for<'a> MapUpdateBorrowFn<'a, Context, Shared, K, V, B> + Send,
{
    type OnUpdateHandler<'a> = <F as MapUpdateBorrowFn<'a, Context, Shared, K, V, B>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &HashMap<K, V>,
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

impl<K, V, Context, Shared, F> OnDownlinkUpdateShared<K, V, Context, Shared>
    for LiftShared<F, Shared>
where
    F: OnDownlinkUpdate<K, V, Context> + Send,
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
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_update(key, map, previous, new_value)
    }
}
