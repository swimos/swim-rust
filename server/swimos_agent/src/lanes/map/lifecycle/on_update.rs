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
};

/// Lifecycle event for the `on_update` event of a map lane.
pub trait OnUpdate<K, V, M, Context>: Send {
    type OnUpdateHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `map` - The current contents of the map.
    /// * `key` - The key that was removed.
    /// * `prev_value` - The value that was replaced (if any).
    /// * `new_value` - The updated value.
    fn on_update<'a>(
        &'a self,
        map: &M,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a>;
}

/// Lifecycle event for the `on_update` event of a map lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnUpdateShared<K, V, M, Context, Shared>: Send {
    type OnUpdateHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
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
        map: &M,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a>;
}

impl<K, V, M, Context> OnUpdate<K, V, M, Context> for NoHandler {
    type OnUpdateHandler<'a>
        = UnitHandler
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        _map: &M,
        _key: K,
        _prev_value: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, M, Context, Shared> OnUpdateShared<K, V, M, Context, Shared> for NoHandler {
    type OnUpdateHandler<'a>
        = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: &M,
        _key: K,
        _prev_value: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, M, Context, F, H> OnUpdate<K, V, M, Context> for FnHandler<F>
where
    F: Fn(&M, K, Option<V>, &V) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a>
        = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        map: &M,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let FnHandler(f) = self;
        f(map, key, prev_value, new_value)
    }
}

impl<K, V, M, Context, Shared, F> OnUpdateShared<K, V, M, Context, Shared> for FnHandler<F>
where
    F: for<'a> MapUpdateFn<'a, Context, Shared, K, V, M> + Send,
{
    type OnUpdateHandler<'a>
        = <F as MapUpdateFn<'a, Context, Shared, K, V, M>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &M,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map, key, prev_value, new_value)
    }
}

impl<K, V, M, B, Context, F, H> OnUpdate<K, V, M, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: Fn(&M, K, Option<V>, &B) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnUpdateHandler<'a>
        = H
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        map: &M,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let f_ref = self.as_ref();
        f_ref(map, key, prev_value, new_value.borrow())
    }
}

impl<K, V, M, B, Context, Shared, F> OnUpdateShared<K, V, M, Context, Shared>
    for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: for<'a> MapUpdateBorrowFn<'a, Context, Shared, K, V, M, B> + Send,
{
    type OnUpdateHandler<'a>
        = <F as MapUpdateBorrowFn<'a, Context, Shared, K, V, M, B>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_update<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &M,
        key: K,
        prev_value: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler<'a> {
        let f_ref = self.as_ref();
        f_ref.make_handler(
            shared,
            handler_context,
            map,
            key,
            prev_value,
            new_value.borrow(),
        )
    }
}
