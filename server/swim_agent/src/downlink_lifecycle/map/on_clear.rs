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
    downlink_lifecycle::{LiftShared, WithHandlerContext},
    event_handler::{EventHandler, TakeFn, UnitHandler},
};

/// Lifecycle event for the `on_clear` event of a downlink, from an agent.
pub trait OnDownlinkClear<K, V, Context>: Send {
    type OnClearHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `map` - The old state of the map.
    fn on_clear(&self, map: HashMap<K, V>) -> Self::OnClearHandler<'_>;
}

/// Lifecycle event for the `on_clear` event of a downlink, from an agent, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkClearShared<K, V, Context, Shared>: Send {
    type OnClearHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `map` - The old state of the map.
    fn on_clear<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: HashMap<K, V>,
    ) -> Self::OnClearHandler<'a>;
}

impl<K, V, Context> OnDownlinkClear<K, V, Context> for NoHandler {
    type OnClearHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_clear(&self, _map: HashMap<K, V>) -> Self::OnClearHandler<'_> {
        UnitHandler::default()
    }
}

impl<K, V, Context, Shared> OnDownlinkClearShared<K, V, Context, Shared> for NoHandler {
    type OnClearHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: HashMap<K, V>,
    ) -> Self::OnClearHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, Context, F, H> OnDownlinkClear<K, V, Context> for FnHandler<F>
where
    F: Fn(HashMap<K, V>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnClearHandler<'a> = H
    where
        Self: 'a;

    fn on_clear(&self, map: HashMap<K, V>) -> Self::OnClearHandler<'_> {
        let FnHandler(f) = self;
        f(map)
    }
}

impl<K, V, Context, Shared, F> OnDownlinkClearShared<K, V, Context, Shared> for FnHandler<F>
where
    F: for<'a> TakeFn<'a, Context, Shared, HashMap<K, V>> + Send,
{
    type OnClearHandler<'a> = <F as TakeFn<'a, Context, Shared, HashMap<K, V>>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: HashMap<K, V>,
    ) -> Self::OnClearHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map)
    }
}

impl<Context, K, V, F, H> OnDownlinkClear<K, V, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, HashMap<K, V>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnClearHandler<'a> = H
    where
        Self: 'a;

    fn on_clear(&self, map: HashMap<K, V>) -> Self::OnClearHandler<'_> {
        let WithHandlerContext { inner } = self;
        inner(Default::default(), map)
    }
}

impl<K, V, Context, Shared, F> OnDownlinkClearShared<K, V, Context, Shared>
    for LiftShared<F, Shared>
where
    F: OnDownlinkClear<K, V, Context> + Send,
{
    type OnClearHandler<'a> = F::OnClearHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_clear<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        map: HashMap<K, V>,
    ) -> Self::OnClearHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_clear(map)
    }
}
