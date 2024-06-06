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
    event_handler::{EventHandler, UnitHandler, UpdateBorrowFn, UpdateFn},
    lifecycle_fn::{LiftShared, WithHandlerContext, WithHandlerContextBorrow},
};

/// Lifecycle event for the `on_set` event of a downlink, from an agent.
pub trait OnDownlinkSet<T, Context>: Send {
    type OnSetHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `previous` - The previous value.
    /// * `value` - The new value.
    fn on_set<'a>(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler<'a>;
}

/// Lifecycle event for the `on_set` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkSetShared<T, Context, Shared>: Send {
    type OnSetHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `previous` - The previous value.
    /// * `new_value` - The new value.
    fn on_set<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        previous: Option<T>,
        new_value: &T,
    ) -> Self::OnSetHandler<'a>;
}

impl<T, Context> OnDownlinkSet<T, Context> for NoHandler {
    type OnSetHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_set<'a>(&'a self, _previous: Option<T>, _new_value: &T) -> Self::OnSetHandler<'a> {
        UnitHandler::default()
    }
}

impl<T, Context, Shared> OnDownlinkSetShared<T, Context, Shared> for NoHandler {
    type OnSetHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_set<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _previous: Option<T>,
        _new_value: &T,
    ) -> Self::OnSetHandler<'a> {
        UnitHandler::default()
    }
}

impl<T, Context, F, H> OnDownlinkSet<T, Context> for FnHandler<F>
where
    F: Fn(Option<T>, &T) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSetHandler<'a> = H
    where
        Self: 'a;

    fn on_set<'a>(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler<'a> {
        let FnHandler(f) = self;
        f(previous, new_value)
    }
}

impl<B, T, Context, F, H> OnDownlinkSet<T, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(Option<T>, &B) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSetHandler<'a> = H
    where
        Self: 'a;

    fn on_set<'a>(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler<'a> {
        (self.as_ref())(previous, new_value.borrow())
    }
}

impl<T, Context, Shared, F> OnDownlinkSetShared<T, Context, Shared> for FnHandler<F>
where
    F: for<'a> UpdateFn<'a, Context, Shared, T> + Send,
{
    type OnSetHandler<'a> = <F as UpdateFn<'a, Context, Shared, T>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_set<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        previous: Option<T>,
        new_value: &T,
    ) -> Self::OnSetHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, new_value, previous)
    }
}

impl<B, T, Context, Shared, F> OnDownlinkSetShared<T, Context, Shared> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: for<'a> UpdateBorrowFn<'a, Context, Shared, T, B> + Send,
{
    type OnSetHandler<'a> = <F as UpdateBorrowFn<'a, Context, Shared, T, B>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_set<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        previous: Option<T>,
        new_value: &T,
    ) -> Self::OnSetHandler<'a> {
        (self.as_ref()).make_handler(shared, handler_context, new_value.borrow(), previous)
    }
}

impl<Context, T, F, H> OnDownlinkSet<T, Context> for WithHandlerContext<F>
where
    F: Fn(HandlerContext<Context>, Option<T>, &T) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSetHandler<'a> = H
    where
        Self: 'a;

    fn on_set<'a>(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler<'a> {
        let WithHandlerContext { inner } = self;
        inner(HandlerContext::default(), previous, new_value)
    }
}

impl<Context, T, B, F, H> OnDownlinkSet<T, Context> for WithHandlerContextBorrow<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(HandlerContext<Context>, Option<T>, &B) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnSetHandler<'a> = H
    where
        Self: 'a;

    fn on_set<'a>(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler<'a> {
        let WithHandlerContextBorrow { inner, .. } = self;
        inner(HandlerContext::default(), previous, new_value.borrow())
    }
}

impl<T, Context, Shared, F> OnDownlinkSetShared<T, Context, Shared> for LiftShared<F, Shared>
where
    F: OnDownlinkSet<T, Context> + Send,
{
    type OnSetHandler<'a> = F::OnSetHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_set<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        previous: Option<T>,
        new_value: &T,
    ) -> Self::OnSetHandler<'a> {
        let LiftShared { inner, .. } = self;
        inner.on_set(previous, new_value)
    }
}
