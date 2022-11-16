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
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
};

/// Event handler to be called each time the value of a value lane changes, consuming the new value
/// and the previous value that was replaced.
pub trait OnSet<'a, T, Context>: Send {
    type OnSetHandler: EventHandler<Context> + 'a;
    /// #Arguments
    /// * `existing` - The existing value, if it is defined.
    /// * `new_value` - The replacement value.
    fn on_set(&'a self, existing: Option<T>, new_value: &T) -> Self::OnSetHandler;
}

/// Event handler to be called each time the value of a value lane changes, cconsuming the new value
/// and the previous value that was replaced. The event handler has access to some shared state (shared
/// with other event handlers in the same agent).
pub trait OnSetShared<'a, T, Context, Shared>: Send {
    type OnSetHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `existing` - The existing value, if it is defined.
    /// * `new_value` - The replacement value.
    fn on_set(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        new_value: &T,
        existing: Option<T>,
    ) -> Self::OnSetHandler;
}

impl<'a, T, Context> OnSet<'a, T, Context> for NoHandler {
    type OnSetHandler = UnitHandler;

    fn on_set(&'a self, _existing: Option<T>, _new_value: &T) -> Self::OnSetHandler {
        Default::default()
    }
}

impl<'a, T, Context, Shared> OnSetShared<'a, T, Context, Shared> for NoHandler {
    type OnSetHandler = UnitHandler;

    fn on_set(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _new_value: &T,
        _existing: Option<T>,
    ) -> Self::OnSetHandler {
        Default::default()
    }
}

impl<'a, T, Context, F, H> OnSet<'a, T, Context> for FnHandler<F>
where
    T: 'static,
    F: Fn(Option<T>, &T) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnSetHandler = H;

    fn on_set(&'a self, existing: Option<T>, new_value: &T) -> Self::OnSetHandler {
        let FnHandler(f) = self;
        f(existing, new_value)
    }
}

impl<'a, T, Context, Shared, F, H> OnSetShared<'a, T, Context, Shared> for FnHandler<F>
where
    T: 'static,
    F: Fn(&'a Shared, HandlerContext<Context>, &T, Option<T>) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnSetHandler = H;

    fn on_set(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        new_value: &T,
        existing: Option<T>,
    ) -> Self::OnSetHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, new_value, existing)
    }
}
