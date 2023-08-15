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

use swim_api::handlers::{FnHandler, NoHandler};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{HandlerAction, RequestFn0},
    lanes::http::model::UnitResponse,
};

use super::{HttpRequestContext, UnsupportedHandler};

pub trait OnDelete<Context>: Send {
    type OnDeleteHandler<'a>: HandlerAction<Context, Completion = UnitResponse> + 'a
    where
        Self: 'a;

    /// #Arguments
    /// * `http_context` - Metadata associated with the HTTP request.
    fn on_delete(&self, http_context: HttpRequestContext) -> Self::OnDeleteHandler<'_>;
}

pub trait OnDeleteShared<Context, Shared>: Send {
    type OnDeleteHandler<'a>: HandlerAction<Context, Completion = UnitResponse> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `http_context` - Metadata associated with the HTTP request.
    fn on_delete<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::OnDeleteHandler<'a>;
}

impl<Context> OnDelete<Context> for NoHandler {
    type OnDeleteHandler<'a> = UnsupportedHandler
    where
        Self: 'a;

    fn on_delete(&self, _http_context: HttpRequestContext) -> Self::OnDeleteHandler<'_> {
        UnsupportedHandler
    }
}

impl<Context, Shared> OnDeleteShared<Context, Shared> for NoHandler {
    type OnDeleteHandler<'a> = UnsupportedHandler
    where
        Self: 'a,
        Shared: 'a;

    fn on_delete<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _http_context: HttpRequestContext,
    ) -> Self::OnDeleteHandler<'a> {
        UnsupportedHandler
    }
}

impl<Context, F, H> OnDelete<Context> for FnHandler<F>
where
    F: Fn(HttpRequestContext) -> H + Send,
    H: HandlerAction<Context, Completion = UnitResponse> + 'static,
{
    type OnDeleteHandler<'a> = H
    where
        Self: 'a;

    fn on_delete(&self, http_context: HttpRequestContext) -> Self::OnDeleteHandler<'_> {
        let FnHandler(f) = self;
        f(http_context)
    }
}

impl<Context, Shared, F> OnDeleteShared<Context, Shared> for FnHandler<F>
where
    F: for<'a> RequestFn0<'a, Context, Shared> + Send,
{
    type OnDeleteHandler<'a> = <F as RequestFn0<'a, Context, Shared>>::Handler
    where
        Self: 'a,
        Shared: 'a;

    fn on_delete<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::OnDeleteHandler<'a> {
        let FnHandler(f) = self;
        f.make_handler(shared, handler_context, http_context)
    }
}
