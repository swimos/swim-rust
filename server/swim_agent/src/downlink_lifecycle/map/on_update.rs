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
    event_handler::{EventHandler, UnitHandler},
};

/// Lifecycle event for the `on_update` event of a downlink, from an agent.
pub trait OnDownlinkUpdate<'a, K, V, Context>: Send {
    type OnUpdateHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `key` - The key that has been updated.
    /// * `map` - The current state of the map.
    /// * `previous` - The previous value, if any.
    /// * `new_value` - The new value of the entry.
    fn on_update(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler;
}

/// Lifecycle event for the `on_update` event of a downlink, from an agent, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkUpdateShared<'a, K, V, Context, Shared>: Send {
    type OnUpdateHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `key` - The key that has been updated.
    /// * `map` - The current state of the map.
    /// * `previous` - The previous value, if any.
    /// * `new_value` - The new value of the entry.
    fn on_update(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler;
}

impl<'a, K, V, Context> OnDownlinkUpdate<'a, K, V, Context> for NoHandler {
    type OnUpdateHandler = UnitHandler;

    fn on_update(
        &'a self,
        _key: K,
        _map: &HashMap<K, V>,
        _previous: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, Shared> OnDownlinkUpdateShared<'a, K, V, Context, Shared> for NoHandler {
    type OnUpdateHandler = UnitHandler;

    fn on_update(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _key: K,
        _map: &HashMap<K, V>,
        _previous: Option<V>,
        _new_value: &V,
    ) -> Self::OnUpdateHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, F, H> OnDownlinkUpdate<'a, K, V, Context> for FnHandler<F>
where
    F: Fn(K, &HashMap<K, V>, Option<V>, &V) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnUpdateHandler = H;

    fn on_update(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler {
        let FnHandler(f) = self;
        f(key, map, previous, new_value)
    }
}

impl<'a, B, K, V, Context, F, H> OnDownlinkUpdate<'a, K, V, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: Fn(K, &HashMap<K, V>, Option<V>, &B) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnUpdateHandler = H;

    fn on_update(
        &'a self,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler {
        (self.as_ref())(key, map, previous, new_value.borrow())
    }
}

impl<'a, K, V, Context, Shared, F, H> OnDownlinkUpdateShared<'a, K, V, Context, Shared>
    for FnHandler<F>
where
    F: Fn(&'a Shared, HandlerContext<Context>, K, &HashMap<K, V>, Option<V>, &V) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnUpdateHandler = H;

    fn on_update(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, key, map, previous, new_value)
    }
}

impl<'a, B, K, V, Context, Shared, F, H> OnDownlinkUpdateShared<'a, K, V, Context, Shared>
    for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: Fn(&'a Shared, HandlerContext<Context>, K, &HashMap<K, V>, Option<V>, &B) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnUpdateHandler = H;

    fn on_update(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        map: &HashMap<K, V>,
        previous: Option<V>,
        new_value: &V,
    ) -> Self::OnUpdateHandler {
        (self.as_ref())(
            shared,
            handler_context,
            key,
            map,
            previous,
            new_value.borrow(),
        )
    }
}
