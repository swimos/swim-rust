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

use std::collections::HashSet;

use swimos_api::address::Address;
use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{ConstHandler, HandlerAction},
    lanes::LinkClosedResponse,
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

use super::JoinMapHandlerStoppedFn;

/// Lifecycle event for the `on_unlinked` event of a join map lane downlink.
pub trait OnJoinMapUnlinked<L, K, Context>: Send {
    type OnJoinMapUnlinkedHandler<'a>: HandlerAction<Context, Completion = LinkClosedResponse> + 'a
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a>;
}

/// Lifecycle event for the `on_unlinked` event of a join map lane downlink, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnJoinMapUnlinkedShared<L, K, Context, Shared>: Send {
    type OnJoinMapUnlinkedHandler<'a>: HandlerAction<Context, Completion = LinkClosedResponse> + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a>;
}

impl<L, K, Context> OnJoinMapUnlinked<L, K, Context> for NoHandler {
    type OnJoinMapUnlinkedHandler<'a> = ConstHandler<LinkClosedResponse>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _link: L,
        _remote: Address<&str>,
        _keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        ConstHandler::default()
    }
}

impl<L, K, Context, Shared> OnJoinMapUnlinkedShared<L, K, Context, Shared> for NoHandler {
    type OnJoinMapUnlinkedHandler<'a> = ConstHandler<LinkClosedResponse>
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _link: L,
        _remote: Address<&str>,
        _keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        ConstHandler::default()
    }
}

impl<L, K, Context, F, H> OnJoinMapUnlinked<L, K, Context> for FnHandler<F>
where
    F: Fn(L, Address<&str>, HashSet<K>) -> H + Send,
    H: HandlerAction<Context, Completion = LinkClosedResponse> + 'static,
{
    type OnJoinMapUnlinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        let FnHandler(f) = self;
        f(link, remote, keys)
    }
}

impl<L, K, Context, Shared, F> OnJoinMapUnlinkedShared<L, K, Context, Shared> for FnHandler<F>
where
    F: for<'a> JoinMapHandlerStoppedFn<'a, Context, Shared, L, K, LinkClosedResponse> + Send,
{
    type OnJoinMapUnlinkedHandler<'a> = <F as JoinMapHandlerStoppedFn<'a, Context, Shared, L, K, LinkClosedResponse>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, link, remote, keys)
    }
}

impl<Context, L, K, F, H> OnJoinMapUnlinked<L, K, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, L, Address<&str>, HashSet<K>) -> H + Send,
    H: HandlerAction<Context, Completion = LinkClosedResponse> + 'static,
{
    type OnJoinMapUnlinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(HandlerContext::default(), link, remote, keys)
    }
}

impl<L, K, Context, Shared, F> OnJoinMapUnlinkedShared<L, K, Context, Shared>
    for LiftShared<F, Shared>
where
    F: OnJoinMapUnlinked<L, K, Context> + Send,
{
    type OnJoinMapUnlinkedHandler<'a> = F::OnJoinMapUnlinkedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_unlinked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
        keys: HashSet<K>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_unlinked(link, remote, keys)
    }
}
