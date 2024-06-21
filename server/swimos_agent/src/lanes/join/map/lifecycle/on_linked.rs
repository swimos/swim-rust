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

use swimos_api::address::Address;
use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
    lanes::join::JoinHandlerFn,
    lifecycle_fn::{LiftShared, WithHandlerContext},
};

/// Lifecycle event for the `on_linked` event of a join map lane downlink.
pub trait OnJoinMapLinked<L, Context>: Send {
    type OnJoinMapLinkedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    fn on_linked<'a>(&'a self, link: L, remote: Address<&str>) -> Self::OnJoinMapLinkedHandler<'a>;
}

/// Lifecycle event for the `on_linked` event of a join map lane downlink, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnJoinMapLinkedShared<L, Context, Shared>: Send {
    type OnJoinMapLinkedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
    ) -> Self::OnJoinMapLinkedHandler<'a>;
}

impl<L, Context> OnJoinMapLinked<L, Context> for NoHandler {
    type OnJoinMapLinkedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        _link: L,
        _remote: Address<&str>,
    ) -> Self::OnJoinMapLinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<L, Context, F, H> OnJoinMapLinked<L, Context> for FnHandler<F>
where
    F: Fn(L, Address<&str>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnJoinMapLinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_linked<'a>(&'a self, link: L, remote: Address<&str>) -> Self::OnJoinMapLinkedHandler<'a> {
        let FnHandler(f) = self;
        f(link, remote)
    }
}

impl<L, Context, Shared> OnJoinMapLinkedShared<L, Context, Shared> for NoHandler {
    type OnJoinMapLinkedHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _link: L,
        _remote: Address<&str>,
    ) -> Self::OnJoinMapLinkedHandler<'a> {
        UnitHandler::default()
    }
}

impl<L, Context, Shared, F> OnJoinMapLinkedShared<L, Context, Shared> for FnHandler<F>
where
    F: for<'a> JoinHandlerFn<'a, Context, Shared, L, ()> + Send,
{
    type OnJoinMapLinkedHandler<'a> = <F as JoinHandlerFn<'a, Context, Shared, L, ()>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
    ) -> Self::OnJoinMapLinkedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, link, remote)
    }
}

impl<Context, L, F, H> OnJoinMapLinked<L, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, L, Address<&str>) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnJoinMapLinkedHandler<'a> = H
    where
        Self: 'a;

    fn on_linked<'a>(&'a self, link: L, remote: Address<&str>) -> Self::OnJoinMapLinkedHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(HandlerContext::default(), link, remote)
    }
}

impl<L, Context, Shared, F> OnJoinMapLinkedShared<L, Context, Shared> for LiftShared<F, Shared>
where
    F: OnJoinMapLinked<L, Context> + Send,
{
    type OnJoinMapLinkedHandler<'a> = F::OnJoinMapLinkedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_linked<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        link: L,
        remote: Address<&str>,
    ) -> Self::OnJoinMapLinkedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_linked(link, remote)
    }
}
