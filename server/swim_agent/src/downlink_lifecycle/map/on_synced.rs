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
    event_handler::{EventFn, EventHandler, UnitHandler},
};

/// Lifecycle event for the `on_synced` event of a downlink, from an agent.
pub trait OnMapSynced<K, V, Context>: Send {
    type OnSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `map` - The synced state of the map.
    fn on_synced<'a>(&'a self, map: &HashMap<K, V>) -> Self::OnSyncedHandler<'a>;
}

/// Lifecycle event for the `on_synced` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnMapSyncedShared<K, V, Context, Shared>: Send {
    type OnSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `map` - The synced state of the map.
    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
    ) -> Self::OnSyncedHandler<'a>;
}

impl<K, V, Context> OnMapSynced<K, V, Context> for NoHandler {
    type OnSyncedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, _map: &HashMap<K, V>) -> Self::OnSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, Context, Shared> OnMapSyncedShared<K, V, Context, Shared> for NoHandler {
    type OnSyncedHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: &HashMap<K, V>,
    ) -> Self::OnSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, Context, F, H> OnMapSynced<K, V, Context> for FnHandler<F>
where
    F: Fn(&HashMap<K, V>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSyncedHandler<'a> = H
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, map: &HashMap<K, V>) -> Self::OnSyncedHandler<'a> {
        let FnHandler(f) = self;
        f(map)
    }
}

impl<K, V, Context, Shared, F> OnMapSyncedShared<K, V, Context, Shared> for FnHandler<F>
where
    F: for<'a> EventFn<'a, Context, Shared, HashMap<K, V>> + Send,
{
    type OnSyncedHandler<'a> = <F as EventFn<'a, Context, Shared, HashMap<K, V>>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
    ) -> Self::OnSyncedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, map)
    }
}

impl<Context, K, V, F, H> OnMapSynced<K, V, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, &HashMap<K, V>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSyncedHandler<'a> = H
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, map: &HashMap<K, V>) -> Self::OnSyncedHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(Default::default(), map)
    }
}

impl<K, V, Context, Shared, F> OnMapSyncedShared<K, V, Context, Shared> for LiftShared<F, Shared>
where
    F: OnMapSynced<K, V, Context> + Send,
{
    type OnSyncedHandler<'a> = F::OnSyncedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        map: &HashMap<K, V>,
    ) -> Self::OnSyncedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_synced(map)
    }
}
