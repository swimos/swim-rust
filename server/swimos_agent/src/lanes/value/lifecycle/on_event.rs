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
    agent_lifecycle::HandlerContext,
    event_handler::{EventFn, EventHandler, UnitHandler},
};

/// Event handler to be called each time the value of a value lane changes, consuming only the new value.
pub trait OnEvent<T, Context>: Send {
    type OnEventHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `value` - The new value.
    fn on_event<'a>(&'a self, value: &T) -> Self::OnEventHandler<'a>;
}

/// Event handler to be called each time the value of a value lane changes, consuming only the new value.
/// The event handler has access to some shared state (shared with other event handlers in the same agent).
pub trait OnEventShared<T, Context, Shared>: Send {
    type OnEventHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `existing` - The existing value, if it is defined.
    /// * `value` - The new value.
    fn on_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnEventHandler<'a>;
}

impl<T, Context> OnEvent<T, Context> for NoHandler {
    type OnEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_event<'a>(&'a self, _value: &T) -> Self::OnEventHandler<'a> {
        Default::default()
    }
}

impl<T, Context, Shared> OnEventShared<T, Context, Shared> for NoHandler {
    type OnEventHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _value: &T,
    ) -> Self::OnEventHandler<'a> {
        Default::default()
    }
}

impl<T, Context, F, H> OnEvent<T, Context> for FnHandler<F>
where
    T: 'static,
    F: Fn(&T) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnEventHandler<'a> = H
    where
        Self: 'a;

    fn on_event<'a>(&'a self, value: &T) -> Self::OnEventHandler<'a> {
        let FnHandler(f) = self;
        f(value)
    }
}

impl<T, Context, Shared, F> OnEventShared<T, Context, Shared> for FnHandler<F>
where
    T: 'static,
    F: for<'a> EventFn<'a, Context, Shared, T> + Send,
{
    type OnEventHandler<'a> = <F as EventFn<'a, Context, Shared, T>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnEventHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, value)
    }
}

impl<B, T, Context, F, H> OnEvent<T, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(&B) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnEventHandler<'a> = H
    where
        Self: 'a;

    fn on_event<'a>(&'a self, value: &T) -> Self::OnEventHandler<'a> {
        (self.as_ref())(value.borrow())
    }
}

impl<B, T, Context, Shared, F> OnEventShared<T, Context, Shared> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: for<'a> EventFn<'a, Context, Shared, B> + Send,
{
    type OnEventHandler<'a> = <F as EventFn<'a, Context, Shared, B>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnEventHandler<'a> {
        (self.as_ref()).make_handler(shared, handler_context, value.borrow())
    }
}
