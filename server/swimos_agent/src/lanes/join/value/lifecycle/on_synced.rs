// Copyright 2015-2023 Swim Inc.
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

use swimos_api::address::Address;
use swimos_utilities::handlers::{BorrowHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
    lifecycle_fn::{LiftShared, WithHandlerContextBorrow},
};

/// Lifecycle event for the `on_synced` event of a join value lane downlink.
pub trait OnJoinValueSynced<K, V, Context>: Send {
    type OnJoinValueSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a>;
}

/// Lifecycle event for the `on_synced` event of a join value lane downlink, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnJoinValueSyncedShared<K, V, Context, Shared>: Send {
    type OnJoinValueSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a>;
}

impl<K, V, Context> OnJoinValueSynced<K, V, Context> for NoHandler {
    type OnJoinValueSyncedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        _key: K,
        _remote: Address<&str>,
        _value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, V, B, Context, F, H> OnJoinValueSynced<K, V, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: Fn(K, Address<&str>, Option<&B>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnJoinValueSyncedHandler<'a> = H
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a> {
        (self.as_ref())(key, remote, value.map(Borrow::borrow))
    }
}

impl<Context, K, V, B, F, H> OnJoinValueSynced<K, V, Context> for WithHandlerContextBorrow<F, B>
where
    B: ?Sized,
    V: Borrow<B>,
    F: Fn(HandlerContext<Context>, K, Address<&str>, Option<&B>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnJoinValueSyncedHandler<'a> = H
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a> {
        let WithHandlerContextBorrow { inner, .. } = self;
        inner(
            HandlerContext::default(),
            key,
            remote,
            value.map(Borrow::borrow),
        )
    }
}

impl<K, V, Context, Shared, F> OnJoinValueSyncedShared<K, V, Context, Shared>
    for LiftShared<F, Shared>
where
    F: OnJoinValueSynced<K, V, Context> + Send,
{
    type OnJoinValueSyncedHandler<'a> = F::OnJoinValueSyncedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
        value: Option<&V>,
    ) -> Self::OnJoinValueSyncedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_synced(key, remote, value)
    }
}
