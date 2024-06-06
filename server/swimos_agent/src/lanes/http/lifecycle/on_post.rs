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

use swimos_utilities::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{HandlerAction, RequestFn1},
    lanes::http::UnitResponse,
};

use super::{HttpRequestContext, UnsupportedHandler};

/// Event handler to be called each time a POST request is called for an HTTP lane.
pub trait OnPost<T, Context>: Send {
    type OnPostHandler<'a>: HandlerAction<Context, Completion = UnitResponse> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `http_context` - Metadata associated with the HTTP request.
    /// * `value` - The value posted to the lane.
    fn on_post(&self, http_context: HttpRequestContext, value: T) -> Self::OnPostHandler<'_>;
}

/// Event handler to be called each time a POST request is called for an HTTP lane.
/// The event handler has access to some shared state (shared with other event handlers in the same agent).
pub trait OnPostShared<T, Context, Shared>: Send {
    type OnPostHandler<'a>: HandlerAction<Context, Completion = UnitResponse> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `http_context` - Metadata associated with the HTTP request.
    /// * `value` - The body of the post request.
    fn on_post<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
        value: T,
    ) -> Self::OnPostHandler<'a>;
}

impl<T, Context> OnPost<T, Context> for NoHandler {
    type OnPostHandler<'a> = UnsupportedHandler
    where
        Self: 'a;

    fn on_post(&self, _http_context: HttpRequestContext, _value: T) -> Self::OnPostHandler<'_> {
        UnsupportedHandler
    }
}

impl<T, Context, Shared> OnPostShared<T, Context, Shared> for NoHandler {
    type OnPostHandler<'a> = UnsupportedHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_post<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _http_context: HttpRequestContext,
        _value: T,
    ) -> Self::OnPostHandler<'a> {
        UnsupportedHandler
    }
}

impl<T, Context, F, H> OnPost<T, Context> for FnHandler<F>
where
    F: Fn(HttpRequestContext, T) -> H + Send,
    H: HandlerAction<Context, Completion = UnitResponse> + 'static,
{
    type OnPostHandler<'a> = H
    where
        Self: 'a;

    fn on_post(&self, http_context: HttpRequestContext, value: T) -> Self::OnPostHandler<'_> {
        let FnHandler(f) = self;
        f(http_context, value)
    }
}

impl<T, Context, Shared, F> OnPostShared<T, Context, Shared> for FnHandler<F>
where
    F: for<'a> RequestFn1<'a, T, Context, Shared> + Send,
{
    type OnPostHandler<'a> = <F as RequestFn1<'a, T, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_post<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
        value: T,
    ) -> Self::OnPostHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, http_context, value)
    }
}
