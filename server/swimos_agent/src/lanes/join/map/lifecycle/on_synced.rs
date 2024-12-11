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

use std::collections::HashSet;

use swimos_api::address::Address;
use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

use super::JoinMapHandlerSyncedFn;

/// Lifecycle event for the `on_synced` event of a join map lane downlink.
pub trait OnJoinMapSynced<L, K, Context>: Send {
    type OnJoinMapSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a>;
}

/// Lifecycle event for the `on_synced` event of a join map lane downlink, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnJoinMapSyncedShared<L, K, Context, Shared>: Send {
    type OnJoinMapSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a>;
}

impl<L, K, Context> OnJoinMapSynced<L, K, Context> for NoHandler {
    type OnJoinMapSyncedHandler<'a>
        = UnitHandler
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        _link: L,
        _remote: Address<&str>,
        _keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<L, K, Context, Shared> OnJoinMapSyncedShared<L, K, Context, Shared> for NoHandler {
    type OnJoinMapSyncedHandler<'a>
        = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _link: L,
        _remote: Address<&str>,
        _keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<L, K, Context, F, H> OnJoinMapSynced<L, K, Context> for FnHandler<F>
where
    F: Fn(L, Address<&str>, &HashSet<K>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnJoinMapSyncedHandler<'a>
        = H
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        let FnHandler(f) = self;
        f(link, remote, keys)
    }
}

impl<L, K, Context, Shared, F> OnJoinMapSyncedShared<L, K, Context, Shared> for FnHandler<F>
where
    F: for<'a> JoinMapHandlerSyncedFn<'a, Context, Shared, L, K, ()> + Send,
{
    type OnJoinMapSyncedHandler<'a>
        = <F as JoinMapHandlerSyncedFn<'a, Context, Shared, L, K, ()>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, link, remote, keys)
    }
}

impl<Context, L, K, F, H> OnJoinMapSynced<L, K, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, L, Address<&str>, &HashSet<K>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnJoinMapSyncedHandler<'a>
        = H
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(HandlerContext::default(), link, remote, keys)
    }
}

impl<L, K, Context, Shared, F> OnJoinMapSyncedShared<L, K, Context, Shared>
    for LiftShared<F, Shared>
where
    F: OnJoinMapSynced<L, K, Context> + Send,
{
    type OnJoinMapSyncedHandler<'a>
        = F::OnJoinMapSyncedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: &HashSet<K>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_synced(link, remote, keys)
    }
}
