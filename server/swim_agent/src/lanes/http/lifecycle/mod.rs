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

use swim_api::handlers::{FnHandler, NoHandler};
use swim_model::http::{Header, Uri};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{ActionContext, HandlerAction, StepResult},
    meta::AgentMetadata,
};

use self::{
    on_delete::{OnDelete, OnDeleteShared},
    on_get::{OnGet, OnGetShared},
    on_post::{OnPost, OnPostShared},
    on_put::OnPutShared,
};

use super::model::Response;

pub mod on_delete;
pub mod on_get;
pub mod on_post;
pub mod on_put;

pub trait HttpLaneLifecycle<Get, Post, Put, Context>:
    OnGet<Get, Context> + OnPost<Post, Context> + OnPost<Put, Context> + OnDelete<Context>
{
}

impl<Context, Get, Post, Put, LC> HttpLaneLifecycle<Get, Post, Put, Context> for LC where
    LC: OnGet<Get, Context> + OnPost<Post, Context> + OnPost<Put, Context> + OnDelete<Context>
{
}

pub struct HttpRequestContext {
    uri: Uri,
    headers: Vec<Header>,
}

impl HttpRequestContext {
    pub(crate) fn new(uri: Uri, headers: Vec<Header>) -> Self {
        HttpRequestContext { uri, headers }
    }

    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    pub fn headers(&self) -> &[Header] {
        self.headers.as_slice()
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct UnsupportedHandler;

impl<Context> HandlerAction<Context> for UnsupportedHandler {
    type Completion = Response<()>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        StepResult::done(Response::not_supported())
    }
}

pub struct StatefulHttpLaneLifecycle<
    Context,
    Shared,
    Get,
    Post,
    Put,
    FGet = NoHandler,
    FPost = NoHandler,
    FPut = NoHandler,
    FDel = NoHandler,
> {
    _type: PhantomData<fn(Context, Shared, Get) -> (Post, Put)>,
    on_get: FGet,
    on_post: FPost,
    on_put: FPut,
    on_delete: FDel,
}

impl<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel> Clone
    for StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel>
where
    FGet: Clone,
    FPost: Clone,
    FPut: Clone,
    FDel: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _type: self._type.clone(),
            on_get: self.on_get.clone(),
            on_post: self.on_post.clone(),
            on_put: self.on_put.clone(),
            on_delete: self.on_delete.clone(),
        }
    }
}

impl<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel> Default
    for StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel>
where
    FGet: Default,
    FPost: Default,
    FPut: Default,
    FDel: Default,
{
    fn default() -> Self {
        Self {
            _type: Default::default(),
            on_get: Default::default(),
            on_post: Default::default(),
            on_put: Default::default(),
            on_delete: Default::default(),
        }
    }
}

impl<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel>
    StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel>
{
    pub fn on_get<F>(
        self,
        f: F,
    ) -> StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FnHandler<F>, FPost, FPut, FDel>
    where
        FnHandler<F>: OnGetShared<Get, Context, Shared>,
    {
        StatefulHttpLaneLifecycle {
            _type: Default::default(),
            on_get: FnHandler(f),
            on_post: self.on_post,
            on_put: self.on_put,
            on_delete: self.on_delete,
        }
    }

    pub fn on_post<F>(
        self,
        f: F,
    ) -> StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FnHandler<F>, FPut, FDel>
    where
        FnHandler<F>: OnPostShared<Post, Context, Shared>,
    {
        StatefulHttpLaneLifecycle {
            _type: Default::default(),
            on_get: self.on_get,
            on_post: FnHandler(f),
            on_put: self.on_put,
            on_delete: self.on_delete,
        }
    }

    pub fn on_put<F>(
        self,
        f: F,
    ) -> StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FnHandler<F>, FDel>
    where
        FnHandler<F>: OnPutShared<Put, Context, Shared>,
    {
        StatefulHttpLaneLifecycle {
            _type: Default::default(),
            on_get: self.on_get,
            on_post: self.on_post,
            on_put: FnHandler(f),
            on_delete: self.on_delete,
        }
    }

    pub fn on_delete<F>(
        self,
        f: F,
    ) -> StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FnHandler<F>>
    where
        FnHandler<F>: OnDeleteShared<Context, Shared>,
    {
        StatefulHttpLaneLifecycle {
            _type: Default::default(),
            on_get: self.on_get,
            on_post: self.on_post,
            on_put: self.on_put,
            on_delete: FnHandler(f),
        }
    }
}

impl<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel> OnGetShared<Get, Context, Shared>
    for StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel>
where
    FGet: OnGetShared<Get, Context, Shared>,
    FPost: Send,
    FPut: Send,
    FDel: Send,
{
    type OnGetHandler<'a> = <FGet as OnGetShared<Get, Context, Shared>>::OnGetHandler<'a>
    where
        Self: 'a;

    fn on_get<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::OnGetHandler<'a> {
        self.on_get.on_get(shared, handler_context, http_context)
    }
}

impl<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel> OnPostShared<Post, Context, Shared>
    for StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel>
where
    FGet: Send,
    FPost: OnPostShared<Post, Context, Shared>,
    FPut: Send,
    FDel: Send,
{
    type OnPostHandler<'a> = <FPost as OnPostShared<Post, Context, Shared>>::OnPostHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_post<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
        value: Post
    ) -> Self::OnPostHandler<'a> {
        self.on_post.on_post(shared, handler_context, http_context, value)
    }
}

impl<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel> OnPutShared<Put, Context, Shared>
    for StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel>
where
    FGet: Send,
    FPost: Send,
    FPut: OnPutShared<Put, Context, Shared>,
    FDel: Send,
{
    type OnPutHandler<'a> = <FPut as OnPutShared<Put, Context, Shared>>::OnPutHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_put<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
        value: Put
    ) -> Self::OnPutHandler<'a> {
        self.on_put.on_put(shared, handler_context, http_context, value)
    }
}

impl<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel> OnDeleteShared<Context, Shared>
    for StatefulHttpLaneLifecycle<Context, Shared, Get, Post, Put, FGet, FPost, FPut, FDel>
where
    FGet: Send,
    FPost: Send,
    FPut: Send,
    FDel: OnDeleteShared<Context, Shared>,
{
    type OnDeleteHandler<'a> = <FDel as OnDeleteShared<Context, Shared>>::OnDeleteHandler<'a>
    where
        Self: 'a,
        Shared: 'a;

    fn on_delete<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        http_context: HttpRequestContext,
    ) -> Self::OnDeleteHandler<'a> {
        self.on_delete.on_delete(shared, handler_context, http_context)
    }
}