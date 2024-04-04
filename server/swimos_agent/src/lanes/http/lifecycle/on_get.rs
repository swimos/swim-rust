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
    event_handler::{ActionContext, EventHandlerError, GetFn, HandlerAction, StepResult},
    lanes::http::Response,
    meta::AgentMetadata,
};

use super::HttpRequestContext;

/// Event handler to be called each time a GET request is called for an HTTP lane.
pub trait OnGet<T, Context>: Send {
    type OnGetHandler<'a>: HandlerAction<Context, Completion = Response<T>> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `http_context` - Metadata associated with the HTTP request.
    fn on_get(&self, http_context: HttpRequestContext) -> Self::OnGetHandler<'_>;
}

/// Event handler to be called each time a GET request is called for an HTTP lane.
/// The event handler has access to some shared state (shared with other event handlers in the same agent).
pub trait OnGetShared<T, Context, Shared>: Send {
    type OnGetHandler<'a>: HandlerAction<Context, Completion = Response<T>> + 'a
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
pub struct GetUndefined<T>(PhantomData<fn() -> T>);

impl<T> Default for GetUndefined<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T, Context> HandlerAction<Context> for GetUndefined<T> {
    type Completion = Response<T>;

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
    H: HandlerAction<Context, Completion = Response<T>> + 'static,
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
    F: for<'a> GetFn<'a, T, Context, Shared> + Send,
{
    type OnGetHandler<'a> = <F as GetFn<'a, T, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_get<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::OnGetHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, http_context)
    }
}
