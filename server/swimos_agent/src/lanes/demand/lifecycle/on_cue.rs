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

use std::marker::PhantomData;

use swimos_api::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{ActionContext, CueFn0, EventHandlerError, HandlerAction, StepResult},
    meta::AgentMetadata,
};

/// Lifecycle event for the `on_cue` event of a demand lane.
pub trait OnCue<T, Context>: Send {
    type OnCueHandler<'a>: HandlerAction<Context, Completion = T> + 'a
    where
        Self: 'a;

    fn on_cue(&self) -> Self::OnCueHandler<'_>;
}

/// Lifecycle event for the `on_cue` event of a demand lane where the event handler
/// has shared state with other handlers for the same agent.
pub trait OnCueShared<T, Context, Shared>: Send {
    type OnCueHandler<'a>: HandlerAction<Context, Completion = T> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    fn on_cue<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnCueHandler<'a>;
}

pub struct CueUndefined<T>(PhantomData<T>);

impl<T> Default for CueUndefined<T> {
    fn default() -> Self {
        CueUndefined(Default::default())
    }
}

impl<Context, T> HandlerAction<Context> for CueUndefined<T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        StepResult::Fail(EventHandlerError::DemandCueUndefined)
    }
}

impl<T, Context> OnCue<T, Context> for NoHandler
where
    T: 'static,
{
    type OnCueHandler<'a> = CueUndefined<T>
    where
        Self: 'a;

    fn on_cue(&self) -> Self::OnCueHandler<'_> {
        CueUndefined::default()
    }
}

impl<T, Context, Shared> OnCueShared<T, Context, Shared> for NoHandler
where
    T: 'static,
{
    type OnCueHandler<'a> = CueUndefined<T>
    where
        Self: 'a,
        Shared: 'a;

    fn on_cue<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
    ) -> Self::OnCueHandler<'a> {
        CueUndefined::default()
    }
}

impl<T, Context, F, H> OnCue<T, Context> for FnHandler<F>
where
    F: Fn() -> H + Send,
    H: HandlerAction<Context, Completion = T> + 'static,
    T: 'static,
{
    type OnCueHandler<'a> = H
    where
        Self: 'a;

    fn on_cue(&self) -> Self::OnCueHandler<'_> {
        let FnHandler(f) = self;
        f()
    }
}

impl<T, Context, Shared, F> OnCueShared<T, Context, Shared> for FnHandler<F>
where
    T: 'static,
    F: for<'a> CueFn0<'a, T, Context, Shared> + Send,
{
    type OnCueHandler<'a> = <F as CueFn0<'a, T, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_cue<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
    ) -> Self::OnCueHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context)
    }
}
