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

use crate::{
    event_handler::{EventHandler, UnitHandler},
    lifecycle::utility::HandlerContext,
};

/// Event handler to be called each time the value of a value lane changes, consuming only the new value.
pub trait OnEvent<'a, T, Context>: Send {
    type OnEventHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `value` - The new value.
    fn on_event(&'a self, value: &T) -> Self::OnEventHandler;
}

/// Event handler to be called each time the value of a value lane changes, consuming only the new value.
/// The event handler has access to some shared state (shared with other event handlers in the same agent).
pub trait OnEventShared<'a, T, Context, Shared>: Send {
    type OnEventHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `existing` - The existing value, if it is defined.
    /// * `value` - The new value.
    fn on_event(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnEventHandler;
}

impl<'a, T, Context> OnEvent<'a, T, Context> for NoHandler {
    type OnEventHandler = UnitHandler;

    fn on_event(&'a self, _value: &T) -> Self::OnEventHandler {
        Default::default()
    }
}

impl<'a, T, Context, Shared> OnEventShared<'a, T, Context, Shared> for NoHandler {
    type OnEventHandler = UnitHandler;

    fn on_event(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _value: &T,
    ) -> Self::OnEventHandler {
        Default::default()
    }
}

impl<'a, T, Context, F, H> OnEvent<'a, T, Context> for FnHandler<F>
where
    T: 'static,
    F: Fn(&T) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnEventHandler = H;

    fn on_event(&'a self, value: &T) -> Self::OnEventHandler {
        let FnHandler(f) = self;
        f(value)
    }
}

impl<'a, T, Context, Shared, F, H> OnEventShared<'a, T, Context, Shared> for FnHandler<F>
where
    T: 'static,
    F: Fn(&'a Shared, HandlerContext<Context>, &T) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnEventHandler = H;

    fn on_event(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnEventHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, value)
    }
}
