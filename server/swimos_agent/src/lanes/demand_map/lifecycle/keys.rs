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

use std::collections::HashSet;

use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{ConstHandler, CueFn0, HandlerAction},
};

/// Lifecycle event for the `keys` event of a demand-map lane.
pub trait Keys<K, Context>: Send {
    type KeysHandler<'a>: HandlerAction<Context, Completion = HashSet<K>> + 'a
    where
        Self: 'a;

    fn keys(&self) -> Self::KeysHandler<'_>;
}

/// Lifecycle event for the `keys` event of a demand-map lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait KeysShared<K, Context, Shared>: Send {
    type KeysHandler<'a>: HandlerAction<Context, Completion = HashSet<K>> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn keys<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::KeysHandler<'a>;
}

impl<K, Context> Keys<K, Context> for NoHandler
where
    K: 'static,
{
    type KeysHandler<'a> = ConstHandler<HashSet<K>>
    where
        Self: 'a;

    fn keys(&self) -> Self::KeysHandler<'_> {
        ConstHandler::from(HashSet::new())
    }
}

impl<K, Context, Shared> KeysShared<K, Context, Shared> for NoHandler
where
    K: 'static,
{
    type KeysHandler<'a> = ConstHandler<HashSet<K>>
    where
        Self: 'a,
        Shared: 'a;

    fn keys<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::KeysHandler<'a> {
        ConstHandler::from(HashSet::new())
    }
}

impl<K, Context, F, H> Keys<K, Context> for FnHandler<F>
where
    K: 'static,
    F: Fn() -> H + Send + 'static,
    H: HandlerAction<Context, Completion = HashSet<K>> + 'static,
{
    type KeysHandler<'a> = H
    where
        Self: 'a;

    fn keys(&self) -> Self::KeysHandler<'_> {
        let FnHandler(f) = self;
        f()
    }
}

impl<K, Context, Shared, F> KeysShared<K, Context, Shared> for FnHandler<F>
where
    K: 'static,
    F: for<'a> CueFn0<'a, HashSet<K>, Context, Shared> + Send,
{
    type KeysHandler<'a> = <F as CueFn0<'a, HashSet<K>, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn keys<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::KeysHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context)
    }
}
