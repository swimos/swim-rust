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

use std::borrow::Borrow;

use swim_api::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
};

/// Lifecycle event for the `on_set` event of a downlink, from an agent.
pub trait OnDownlinkSet<'a, T, Context>: Send {
    type OnSetHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `previous` - The previous value.
    /// * `value` - The new value.
    fn on_set(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler;
}

/// Lifecycle event for the `on_set` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkSetShared<'a, T, Context, Shared>: Send {
    type OnSetHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `previous` - The previous value.
    /// * `value` - The new value.
    fn on_set(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        previous: Option<T>,
        new_value: &T,
    ) -> Self::OnSetHandler;
}

impl<'a, T, Context> OnDownlinkSet<'a, T, Context> for NoHandler {
    type OnSetHandler = UnitHandler;

    fn on_set(&'a self, _previous: Option<T>, _new_value: &T) -> Self::OnSetHandler {
        UnitHandler::default()
    }
}

impl<'a, T, Context, Shared> OnDownlinkSetShared<'a, T, Context, Shared> for NoHandler {
    type OnSetHandler = UnitHandler;

    fn on_set(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _previous: Option<T>,
        _new_value: &T,
    ) -> Self::OnSetHandler {
        UnitHandler::default()
    }
}

impl<'a, T, Context, F, H> OnDownlinkSet<'a, T, Context> for FnHandler<F>
where
    F: Fn(Option<T>, &T) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnSetHandler = H;

    fn on_set(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler {
        let FnHandler(f) = self;
        f(previous, new_value)
    }
}

impl<'a, B, T, Context, F, H> OnDownlinkSet<'a, T, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(Option<T>, &B) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnSetHandler = H;

    fn on_set(&'a self, previous: Option<T>, new_value: &T) -> Self::OnSetHandler {
        (self.as_ref())(previous, new_value.borrow())
    }
}

impl<'a, T, Context, Shared, F, H> OnDownlinkSetShared<'a, T, Context, Shared> for FnHandler<F>
where
    F: Fn(&'a Shared, HandlerContext<Context>, Option<T>, &T) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnSetHandler = H;

    fn on_set(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        previous: Option<T>,
        new_value: &T,
    ) -> Self::OnSetHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, previous, new_value)
    }
}

impl<'a, B, T, Context, Shared, F, H> OnDownlinkSetShared<'a, T, Context, Shared>
    for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(&'a Shared, HandlerContext<Context>, Option<T>, &B) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnSetHandler = H;

    fn on_set(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        previous: Option<T>,
        new_value: &T,
    ) -> Self::OnSetHandler {
        (self.as_ref())(shared, handler_context, previous, new_value.borrow())
    }
}
