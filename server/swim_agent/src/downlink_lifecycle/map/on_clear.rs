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

use std::collections::HashMap;

use swim_api::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, UnitHandler}, downlink_lifecycle::{WithHandlerContext, LiftShared},
};

/// Lifecycle event for the `on_clear` event of a downlink, from an agent.
pub trait OnDownlinkClear<'a, K, V, Context>: Send {
    type OnClearHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `map` - The old state of the map.
    fn on_clear(&'a self, map: HashMap<K, V>) -> Self::OnClearHandler;
}

/// Lifecycle event for the `on_clear` event of a downlink, from an agent, where the event
/// handler has shared state with other handlers for the same downlink.
pub trait OnDownlinkClearShared<'a, K, V, Context, Shared>: Send {
    type OnClearHandler: EventHandler<Context> + 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `map` - The old state of the map.
    fn on_clear(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: HashMap<K, V>,
    ) -> Self::OnClearHandler;
}

impl<'a, K, V, Context> OnDownlinkClear<'a, K, V, Context> for NoHandler {
    type OnClearHandler = UnitHandler;

    fn on_clear(&'a self, _map: HashMap<K, V>) -> Self::OnClearHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, Shared> OnDownlinkClearShared<'a, K, V, Context, Shared> for NoHandler {
    type OnClearHandler = UnitHandler;

    fn on_clear(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _map: HashMap<K, V>,
    ) -> Self::OnClearHandler {
        UnitHandler::default()
    }
}

impl<'a, K, V, Context, F, H> OnDownlinkClear<'a, K, V, Context> for FnHandler<F>
where
    F: Fn(HashMap<K, V>) -> H + Send + 'a,
    H: EventHandler<Context> + 'a,
{
    type OnClearHandler = H;

    fn on_clear(&'a self, map: HashMap<K, V>) -> Self::OnClearHandler {
        let FnHandler(f) = self;
        f(map)
    }
}

impl<'a, K, V, Context, Shared, F, H> OnDownlinkClearShared<'a, K, V, Context, Shared>
    for FnHandler<F>
where
    F: Fn(&'a Shared, HandlerContext<Context>, HashMap<K, V>) -> H + Send + 'a,
    Shared: 'a,
    H: EventHandler<Context> + 'a,
{
    type OnClearHandler = H;

    fn on_clear(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        map: HashMap<K, V>,
    ) -> Self::OnClearHandler {
        let FnHandler(f) = self;
        f(shared, handler_context, map)
    }
}

impl<'a, Context, K, V, F, H> OnDownlinkClear<'a, K, V, Context> for WithHandlerContext<Context, F>
where
    F: Fn(HandlerContext<Context>, HashMap<K, V>) -> H + Send,
    H: EventHandler<Context> + 'a,
{
    type OnClearHandler = H;

    fn on_clear(&'a self, map: HashMap<K, V>) -> Self::OnClearHandler {
        let WithHandlerContext {
            inner,
            handler_context,
        } = self;
        inner(*handler_context, map)
    }
}

impl<'a, K, V, Context, Shared, F> OnDownlinkClearShared<'a, K, V, Context, Shared>
    for LiftShared<F, Shared>
where
    F: OnDownlinkClear<'a, K, V, Context> + Send,
{
    type OnClearHandler = F::OnClearHandler;

    fn on_clear(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        map: HashMap<K, V>,
    ) -> Self::OnClearHandler {
        let LiftShared { inner, .. } = self;
        inner.on_clear(map)
    }
}
