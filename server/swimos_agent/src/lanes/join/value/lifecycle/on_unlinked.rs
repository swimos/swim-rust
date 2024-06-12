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

use swimos_api::address::Address;
use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{ConstHandler, HandlerAction},
    lanes::join::{JoinHandlerFn, LinkClosedResponse},
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

/// Lifecycle event for the `on_unlinked` event of a join value lane downlink.
pub trait OnJoinValueUnlinked<K, Context>: Send {
    type OnJoinValueUnlinkedHandler<'a>: HandlerAction<Context, Completion = LinkClosedResponse>
        + 'a
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a>;
}

/// Lifecycle event for the `on_unlinked` event of a join value lane downlink, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnJoinValueUnlinkedShared<K, Context, Shared>: Send {
    type OnJoinValueUnlinkedHandler<'a>: HandlerAction<Context, Completion = LinkClosedResponse>
        + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a>;
}

impl<K, Context> OnJoinValueUnlinked<K, Context> for NoHandler {
    type OnJoinValueUnlinkedHandler<'a> = ConstHandler<LinkClosedResponse>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _key: K,
        _remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        ConstHandler::default()
    }
}

impl<K, Context, F, H> OnJoinValueUnlinked<K, Context> for FnHandler<F>
where
    F: Fn(K, Address<&str>) -> H + Send,
    H: HandlerAction<Context, Completion = LinkClosedResponse> + 'static,
{
    type OnJoinValueUnlinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        let FnHandler(f) = self;
        f(key, remote)
    }
}

impl<K, Context, Shared> OnJoinValueUnlinkedShared<K, Context, Shared> for NoHandler {
    type OnJoinValueUnlinkedHandler<'a> = ConstHandler<LinkClosedResponse>
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _key: K,
        _remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        ConstHandler::default()
    }
}

impl<K, Context, Shared, F> OnJoinValueUnlinkedShared<K, Context, Shared> for FnHandler<F>
where
    F: for<'a> JoinHandlerFn<'a, Context, Shared, K, LinkClosedResponse> + Send,
{
    type OnJoinValueUnlinkedHandler<'a> = <F as JoinHandlerFn<'a, Context, Shared, K, LinkClosedResponse>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, key, remote)
    }
}

impl<Context, K, F, H> OnJoinValueUnlinked<K, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, K, Address<&str>) -> H + Send,
    H: HandlerAction<Context, Completion = LinkClosedResponse> + 'static,
{
    type OnJoinValueUnlinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(HandlerContext::default(), key, remote)
    }
}

impl<K, Context, Shared, F> OnJoinValueUnlinkedShared<K, Context, Shared> for LiftShared<F, Shared>
where
    F: OnJoinValueUnlinked<K, Context> + Send,
{
    type OnJoinValueUnlinkedHandler<'a> = F::OnJoinValueUnlinkedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        key: K,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_unlinked(key, remote)
    }
}
