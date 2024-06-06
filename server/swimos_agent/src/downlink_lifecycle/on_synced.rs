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

use swimos_utilities::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventFn, EventHandler, UnitHandler},
};

use crate::lifecycle_fn::{LiftShared, WithHandlerContext, WithHandlerContextBorrow};

/// Lifecycle event for the `on_synced` event of a downlink, from an agent.
pub trait OnSynced<T, Context>: Send {
    type OnSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `value` - The synced value.
    fn on_synced<'a>(&'a self, value: &T) -> Self::OnSyncedHandler<'a>;
}

/// Lifecycle event for the `on_synced` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnSyncedShared<T, Context, Shared>: Send {
    type OnSyncedHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `value` - The synced value.
    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnSyncedHandler<'a>;
}

impl<T, Context> OnSynced<T, Context> for NoHandler {
    type OnSyncedHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, _value: &T) -> Self::OnSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<T, Context, Shared> OnSyncedShared<T, Context, Shared> for NoHandler {
    type OnSyncedHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _value: &T,
    ) -> Self::OnSyncedHandler<'a> {
        UnitHandler::default()
    }
}

impl<T, Context, F, H> OnSynced<T, Context> for FnHandler<F>
where
    F: Fn(&T) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSyncedHandler<'a> = H
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &T) -> Self::OnSyncedHandler<'a> {
        let FnHandler(f) = self;
        f(value)
    }
}

impl<B, T, Context, F, H> OnSynced<T, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(&B) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSyncedHandler<'a> = H
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &T) -> Self::OnSyncedHandler<'a> {
        (self.as_ref())(value.borrow())
    }
}

impl<T, Context, Shared, F> OnSyncedShared<T, Context, Shared> for FnHandler<F>
where
    F: for<'a> EventFn<'a, Context, Shared, T> + Send,
{
    type OnSyncedHandler<'a> = <F as EventFn<'a, Context, Shared, T>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnSyncedHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, value)
    }
}

impl<B, T, Context, Shared, F> OnSyncedShared<T, Context, Shared> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: for<'a> EventFn<'a, Context, Shared, B> + Send,
{
    type OnSyncedHandler<'a> = <F as EventFn<'a, Context, Shared, B>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnSyncedHandler<'a> {
        (self.as_ref()).make_handler(shared, handler_context, value.borrow())
    }
}

impl<Context, T, F, H> OnSynced<T, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, &T) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSyncedHandler<'a> = H
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &T) -> Self::OnSyncedHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(Default::default(), value)
    }
}

impl<Context, T, B, F, H> OnSynced<T, Context> for WithHandlerContextBorrow<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(HandlerContext<Context>, &B) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSyncedHandler<'a> = H
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &T) -> Self::OnSyncedHandler<'a> {
        let WithHandlerContextBorrow { inner, .. } = self;
        inner(Default::default(), value.borrow())
    }
}

impl<T, Context, Shared, F> OnSyncedShared<T, Context, Shared> for LiftShared<F, Shared>
where
    F: OnSynced<T, Context> + Send,
{
    type OnSyncedHandler<'a> = F::OnSyncedHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_synced<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnSyncedHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_synced(value)
    }
}
