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

use swimos_api::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{ConstHandler, CueFn1, HandlerAction},
};

/// Lifecycle event for the `on_cue_key` event of a demand-map lane.
pub trait OnCueKey<K, V, Context>: Send {
    type OnCueKeyHandler<'a>: HandlerAction<Context, Completion = Option<V>> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `key` - The key to cue.
    fn on_cue_key(&self, key: K) -> Self::OnCueKeyHandler<'_>;
}

/// Lifecycle event for the `on_cue_key` event of a demand-map lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnCueKeyShared<K, V, Context, Shared>: Send {
    type OnCueKeyHandler<'a>: HandlerAction<Context, Completion = Option<V>> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `key` - The key to cue.
    fn on_cue_key<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
    ) -> Self::OnCueKeyHandler<'a>;
}

impl<K, V, Context> OnCueKey<K, V, Context> for NoHandler
where
    V: 'static,
{
    type OnCueKeyHandler<'a> = ConstHandler<Option<V>>
    where
        Self: 'a;

    fn on_cue_key(&self, _key: K) -> Self::OnCueKeyHandler<'_> {
        ConstHandler::from(None)
    }
}

impl<K, V, Context, Shared> OnCueKeyShared<K, V, Context, Shared> for NoHandler
where
    V: 'static,
{
    type OnCueKeyHandler<'a> = ConstHandler<Option<V>>
    where
        Self: 'a,
        Shared: 'a;

    fn on_cue_key<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _key: K,
    ) -> Self::OnCueKeyHandler<'a> {
        ConstHandler::from(None)
    }
}

impl<K, V, Context, F, H> OnCueKey<K, V, Context> for FnHandler<F>
where
    F: Fn(K) -> H + Send,
    H: HandlerAction<Context, Completion = Option<V>> + 'static,
    V: 'static,
{
    type OnCueKeyHandler<'a> = H
    where
        Self: 'a;

    fn on_cue_key(&self, key: K) -> Self::OnCueKeyHandler<'_> {
        let FnHandler(f) = self;
        f(key)
    }
}

impl<K, V, Context, Shared, F> OnCueKeyShared<K, V, Context, Shared> for FnHandler<F>
where
    V: 'static,
    F: for<'a> CueFn1<'a, K, Option<V>, Context, Shared> + Send,
{
    type OnCueKeyHandler<'a> = <F as CueFn1<'a, K, Option<V>, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_cue_key<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        key: K,
    ) -> Self::OnCueKeyHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, key)
    }
}
