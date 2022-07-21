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

/// Lifecycle event for the `on_command` event of a command lane.
pub trait OnCommand<'a, T, Context>: Send {
    type OnCommandHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `value` - The command value.
    fn on_command(&'a self, value: &T) -> Self::OnCommandHandler;
}

/// Lifecycle event for the `on_command` event of a command lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnCommandShared<'a, T, Context, Shared>: Send {
    type OnCommandHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `value` - The command value.
    fn on_command(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnCommandHandler;
}

impl<'a, T, Context> OnCommand<'a, T, Context> for NoHandler {
    type OnCommandHandler = UnitHandler;

    fn on_command(&'a self, _value: &T) -> Self::OnCommandHandler {
        UnitHandler::default()
    }
}

impl<'a, T, Context, Shared> OnCommandShared<'a, T, Context, Shared> for NoHandler {
    type OnCommandHandler = UnitHandler;

    fn on_command(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _value: &T,
    ) -> Self::OnCommandHandler {
        UnitHandler::default()
    }
}

impl<'a, T, Context, F, H> OnCommand<'a, T, Context> for FnHandler<F>
where
    F: Fn(&T) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnCommandHandler = H;

    fn on_command(&'a self, value: &T) -> Self::OnCommandHandler {
        let FnHandler(f) = self;
        f(value)
    }
}

impl<'a, T, Context, Shared, F, H> OnCommandShared<'a, T, Context, Shared> for FnHandler<F>
where
    F: Fn(&'a Shared, HandlerContext<Context>, &T) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
    Shared: 'static,
{
    type OnCommandHandler = H;

    fn on_command(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnCommandHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, value)
    }
}
