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

use swim_api::handlers::{FnHandler, NoHandler};
use swim_model::address::Address;

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

use super::JoinValueHandlerFn0;

/// Lifecycle event for the `on_linked` event of a join value lane downlink.
pub trait OnJoinValueLinked<K, Context>: Send {
    type OnJoinValueLinkedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn on_linked<'a>(&'a self, key: K, remote: Address<&str>)
        -> Self::OnJoinValueLinkedHandler<'a>;
}

/// Lifecycle event for the `on_linked` event of a join value lane downlink, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnJoinValueLinkedShared<K, Context, Shared>: Send {
    type OnJoinValueLinkedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a>;
}

impl<K, Context> OnJoinValueLinked<K, Context> for NoHandler {
    type OnJoinValueLinkedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        _key: K,
        _remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, Context, F, H> OnJoinValueLinked<K, Context> for FnHandler<F>
where
    F: Fn(K, Address<&str>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnJoinValueLinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        let FnHandler(f) = self;
        f(key, remote)
    }
}

impl<K, Context, Shared> OnJoinValueLinkedShared<K, Context, Shared> for NoHandler {
    type OnJoinValueLinkedHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _key: K,
        _remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<K, Context, Shared, F> OnJoinValueLinkedShared<K, Context, Shared> for FnHandler<F>
where
    F: for<'a> JoinValueHandlerFn0<'a, Context, Shared, K, ()> + Send,
{
    type OnJoinValueLinkedHandler<'a> = <F as JoinValueHandlerFn0<'a, Context, Shared, K, ()>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, key, remote)
    }
}

impl<Context, K, F, H> OnJoinValueLinked<K, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, K, Address<&str>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnJoinValueLinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(HandlerContext::default(), key, remote)
    }
}

impl<K, Context, Shared, F> OnJoinValueLinkedShared<K, Context, Shared> for LiftShared<F, Shared>
where
    F: OnJoinValueLinked<K, Context> + Send,
{
    type OnJoinValueLinkedHandler<'a> = F::OnJoinValueLinkedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_linked(key, remote)
    }
}
