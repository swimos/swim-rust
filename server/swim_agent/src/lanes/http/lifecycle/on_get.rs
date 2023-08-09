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

use swim_api::handlers::{NoHandler, FnHandler};

use crate::{event_handler::{HandlerAction, ActionContext, StepResult, EventHandlerError, GetFn0}, agent_lifecycle::utility::HandlerContext, meta::AgentMetadata};

use super::HttpRequestContext;

pub trait OnGet<T, Context>: Send {
    
    type OnGetHandler<'a>: HandlerAction<Context, Completion = T> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `http_context` - Metadata associated with the HTTP request.
    fn on_get(&self, http_context: HttpRequestContext) -> Self::OnGetHandler<'_>;
}

pub trait OnGetShared<T, Context, Shared>: Send {
    type OnGetHandler<'a>: HandlerAction<Context, Completion = T> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `http_context` - Metadata associated with the HTTP request.
    fn on_get<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::OnGetHandler<'a>;
}

#[derive(Debug)]
pub struct GetUndefined<T>(PhantomData<fn()->T>);

impl<T> Default for GetUndefined<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T, Context> HandlerAction<Context> for GetUndefined<T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        StepResult::Fail(EventHandlerError::HttpGetUndefined)
    }
}

impl<T, Context> OnGet<T, Context> for NoHandler
where
    T: 'static,
{
    type OnGetHandler<'a> = GetUndefined<T>
    where
        Self: 'a;

    fn on_get(&self, _http_context: HttpRequestContext) -> Self::OnGetHandler<'_> {
        GetUndefined::default()
    }
}

impl<T, Context, Shared> OnGetShared<T, Context, Shared> for NoHandler
where
    T: 'static,
{
    type OnGetHandler<'a> = GetUndefined<T>
    where
        Self: 'a,
        Shared: 'a;

    fn on_get<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _http_context: HttpRequestContext,
    ) -> Self::OnGetHandler<'a> {
        GetUndefined::default()
    }
}

impl<T, Context, F, H> OnGet<T, Context> for FnHandler<F>
where
    F: Fn() -> H + Send,
    H: HandlerAction<Context, Completion = T> + 'static,
    T: 'static,
{
    type OnGetHandler<'a> = H
    where
        Self: 'a;

    fn on_get(&self, _http_context: HttpRequestContext) -> Self::OnGetHandler<'_> {
        let FnHandler(f) = self;
        f()
    }
}

impl<T, Context, Shared, F> OnGetShared<T, Context, Shared> for FnHandler<F>
where
    T: 'static,
    F: for<'a> GetFn0<'a, T, Context, Shared> + Send,
{
    type OnGetHandler<'a> = <F as GetFn0<'a, T, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_get<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext
    ) -> Self::OnGetHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, http_context)
    }
}