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

/// Lifecycle event for the `on_synced` event of a downlink, from an agent.
pub trait OnSynced<'a, T, Context>: Send {
    type OnSyncedHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `value` - The synced value.
    fn on_synced(&'a self, value: &T) -> Self::OnSyncedHandler;
}

/// Lifecycle event for the `on_synced` event of a downlink, from an agent,where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnSyncedShared<'a, T, Context, Shared>: Send {
    type OnSyncedHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `value` - The synced value.
    fn on_synced(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnSyncedHandler;
}

impl<'a, T, Context> OnSynced<'a, T, Context> for NoHandler {
    type OnSyncedHandler = UnitHandler;

    fn on_synced(&'a self, _value: &T) -> Self::OnSyncedHandler {
        UnitHandler::default()
    }
}

impl<'a, T, Context, Shared> OnSyncedShared<'a, T, Context, Shared> for NoHandler {
    type OnSyncedHandler = UnitHandler;

    fn on_synced(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _value: &T,
    ) -> Self::OnSyncedHandler {
        UnitHandler::default()
    }
}

impl<'a, T, Context, F, H> OnSynced<'a, T, Context> for FnHandler<F>
where
    F: Fn(&T) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnSyncedHandler = H;

    fn on_synced(&'a self, value: &T) -> Self::OnSyncedHandler {
        let FnHandler(f) = self;
        f(value)
    }
}

impl<'a, B, T, Context, F, H> OnSynced<'a, T, Context> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(&B) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnSyncedHandler = H;

    fn on_synced(&'a self, value: &T) -> Self::OnSyncedHandler {
        (self.as_ref())(value.borrow())
    }
}

impl<'a, T, Context, Shared, F, H> OnSyncedShared<'a, T, Context, Shared> for FnHandler<F>
where
    F: Fn(&'a Shared, HandlerContext<Context>, &T) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnSyncedHandler = H;

    fn on_synced(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnSyncedHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, value)
    }
}

impl<'a, B, T, Context, Shared, F, H> OnSyncedShared<'a, T, Context, Shared> for BorrowHandler<F, B>
where
    B: ?Sized,
    T: Borrow<B>,
    F: Fn(&'a Shared, HandlerContext<Context>, &B) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnSyncedHandler = H;

    fn on_synced(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        value: &T,
    ) -> Self::OnSyncedHandler {
        (self.as_ref())(shared, handler_context, value.borrow())
    }
}
