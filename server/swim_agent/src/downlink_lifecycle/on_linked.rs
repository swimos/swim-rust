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

/// Lifecycle event for the `on_linked` event of a downlink, from an agent.
pub trait OnLinked<'a, Context>: Send {
    type OnLinkedHandler: EventHandler<Context> + 'a;

    fn on_linked(&'a self) -> Self::OnLinkedHandler;
}

/// Lifecycle event for the `on_linked` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnLinkedShared<'a, Context, Shared>: Send {
    type OnLinkedHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn on_linked(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnLinkedHandler;
}

impl<'a, Context> OnLinked<'a, Context> for NoHandler {
    type OnLinkedHandler = UnitHandler;

    fn on_linked(&'a self) -> Self::OnLinkedHandler {
        UnitHandler::default()
    }
}

impl<'a, Context, Shared> OnLinkedShared<'a, Context, Shared> for NoHandler {
    type OnLinkedHandler = UnitHandler;

    fn on_linked(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnLinkedHandler {
        UnitHandler::default()
    }
}

impl<'a, Context, F, H> OnLinked<'a, Context> for FnHandler<F>
where
    F: Fn() -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnLinkedHandler = H;

    fn on_linked(&'a self) -> Self::OnLinkedHandler {
        let FnHandler(f) = self;
        f()
    }
}

impl<'a, Context, Shared, F, H> OnLinkedShared<'a, Context, Shared> for FnHandler<F>
where
    F: Fn(&'a Shared, HandlerContext<Context>) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnLinkedHandler = H;

    fn on_linked(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnLinkedHandler {
        let FnHandler(f) = self;
        f(shared, handler_context)
    }
}
