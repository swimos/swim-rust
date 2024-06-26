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

use std::borrow::Borrow;

use swimos_utilities::handlers::{BorrowHandler, FnHandler, NoHandler};

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventFn, EventHandler, UnitHandler},
};

/// Lifecycle event for the `on_command` event of a command lane.
pub trait OnCommand<T, Context>: Send {
    type OnCommandHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// # Arguments
    /// * `value` - The command value.
    fn on_command<'a>(&'a self, value: &T) -> Self::OnCommandHandler<'a>;
}

/// Lifecycle event for the `on_command` event of a command lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnCommandShared<T, Context, Shared>: Send {
    type OnCommandHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// # Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `value` - The command value.
    fn on_command<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnCommandHandler<'a>;
}

impl<T, Context> OnCommand<T, Context> for NoHandler {
    type OnCommandHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_command<'a>(&'a self, _value: &T) -> Self::OnCommandHandler<'a> {
        UnitHandler::default()
    }
}

impl<T, Context, Shared> OnCommandShared<T, Context, Shared> for NoHandler {
    type OnCommandHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_command<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _value: &T,
    ) -> Self::OnCommandHandler<'a> {
        UnitHandler::default()
    }
}

impl<T, Context, F, H> OnCommand<T, Context> for FnHandler<F>
where
    F: Fn(&T) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnCommandHandler<'a> = H
    where
        Self: 'a;

    fn on_command<'a>(&'a self, value: &T) -> Self::OnCommandHandler<'a> {
        let FnHandler(f) = self;
        f(value)
    }
}

impl<T, Context, Shared, F> OnCommandShared<T, Context, Shared> for FnHandler<F>
where
    F: for<'a> EventFn<'a, Context, Shared, T> + Send,
{
    type OnCommandHandler<'a> = <F as EventFn<'a, Context, Shared, T>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_command<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnCommandHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, value)
    }
}

impl<B, T, Context, F, H> OnCommand<T, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(&B) -> H + Send,
    H: EventHandler<Context> + 'static,
{
    type OnCommandHandler<'a> = H
    where
        Self: 'a;

    fn on_command<'a>(&'a self, value: &T) -> Self::OnCommandHandler<'a> {
        (self.as_ref())(value.borrow())
    }
}

impl<B, T, Context, Shared, F> OnCommandShared<T, Context, Shared> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: for<'a> EventFn<'a, Context, Shared, B> + Send,
{
    type OnCommandHandler<'a> = <F as EventFn<'a, Context, Shared, B>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_command<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnCommandHandler<'a> {
        (self.as_ref()).make_handler(shared, handler_context, value.borrow())
    }
}
