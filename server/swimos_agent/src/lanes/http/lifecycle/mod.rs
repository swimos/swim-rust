// Copyright 2015-2024 Swim Inc.
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

use bytes::{Bytes, BytesMut};
use mime::Mime;
use std::marker::PhantomData;
use swimos_api::{
    agent::{HttpResponseSender, RawHttpLaneResponse},
    http::{Header, HttpResponse, StatusCode, Uri, Version},
};
use swimos_utilities::handlers::{FnHandler, NoHandler};
use tracing::debug;

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::{ActionContext, HandlerAction, StepResult},
    meta::AgentMetadata,
};
use crate::{
    agent_model::AgentDescription,
    event_handler::{Described, EventHandlerError},
};

use self::{
    on_delete::{OnDelete, OnDeleteShared},
    on_get::{OnGet, OnGetShared},
    on_post::{OnPost, OnPostShared},
    on_put::{OnPut, OnPutShared},
};

use super::headers::{add_content_len_header, content_type_header, Headers};
use super::HttpLaneCodec;
use super::{
    model::{MethodAndPayload, Request, Response},
    RequestAndChannel,
};

pub mod on_delete;
pub mod on_get;
pub mod on_post;
pub mod on_put;
#[cfg(test)]
mod tests;

/// Trait for the lifecycle of an HTTP lane.
///
/// # Type Parameters
/// * `Get` - The type of the payloads of responses to GET requests to the lane.
/// * `Post` - The type of the payloads of incoming POST requests.
/// * `Put` - The type of the payloads of incoming PUT requests.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
pub trait HttpLaneLifecycle<Get, Post, Put, Context>:
    OnGet<Get, Context> + OnPost<Post, Context> + OnPut<Put, Context> + OnDelete<Context>
{
}

impl<Context, Get, Post, Put, LC> HttpLaneLifecycle<Get, Post, Put, Context> for LC where
    LC: OnGet<Get, Context> + OnPost<Post, Context> + OnPut<Put, Context> + OnDelete<Context>
{
}

/// Trait for the lifecycle of an HTTP lane where the lifecycle has access to some shared state (shared
/// with all other lifecycles in the agent).
///
/// # Type Parameters
/// * `Get` - The type of the payloads of responses to GET requests to the lane.
/// * `Post` - The type of the payloads of incoming POST requests.
/// * `Put` - The type of the payloads of incoming PUT requests.
/// * `Context` - The context within which the event handlers execute (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
pub trait HttpLaneLifecycleShared<Get, Post, Put, Context, Shared>:
    OnGetShared<Get, Context, Shared>
    + OnPostShared<Post, Context, Shared>
    + OnPutShared<Put, Context, Shared>
    + OnDeleteShared<Context, Shared>
{
}

impl<Context, Shared, Get, Post, Put, LC> HttpLaneLifecycleShared<Get, Post, Put, Context, Shared>
    for LC
where
    LC: OnGetShared<Get, Context, Shared>
        + OnPostShared<Post, Context, Shared>
        + OnPutShared<Put, Context, Shared>
        + OnDeleteShared<Context, Shared>,
{
}

/// A context that is passed to all event handlers on an HTTP lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequestContext {
    uri: Uri,
    headers: Vec<Header>,
}

impl HttpRequestContext {
    pub(crate) fn new(uri: Uri, headers: Vec<Header>) -> Self {
        HttpRequestContext { uri, headers }
    }

    /// The URI from the request that generated this event.
    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    /// The HTTP headers from the request that generated this event.
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

    fn describe(
        &self,
        _context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        f.debug_tuple("UnsupportedHandler").finish()
    }
}

type SharedHttpLifecycleType<Context, Shared, Get, Post, Put> =
    fn(Context, Shared, Get) -> (Post, Put);

/// A lifecycle for an HTTP lane with some shared state (shard with other lifecycles in the same agent).
///
/// # Type Parameters
/// * `Context` - The context for the event handlers (providing access to the agent lanes).
/// * `Shared` - The shared state to which the lifecycle has access.
/// * `Get` - The type of the payloads of responses to GET requests to the lane.
/// * `Post` - The type of the payloads of incoming POST requests.
/// * `Put` - The type of the payloads of incoming PUT requests.
/// * `FGet` - The 'on_get' event handler.
/// * `FPost` - The 'on_post' event handler.
/// * `FPut` - The 'on_put' event handler.
/// * `FDel` - The 'on_delete' event handler.
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
    _type: PhantomData<SharedHttpLifecycleType<Context, Shared, Get, Post, Put>>,
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
            _type: self._type,
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
    /// Replace the `on_get` handler with another derived from a closure.
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

    /// Replace the `on_post` handler with another derived from a closure.
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

    /// Replace the `on_put` handler with another derived from a closure.
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

    /// Replace the `on_delete` handler with another derived from a closure.
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
        value: Post,
    ) -> Self::OnPostHandler<'a> {
        self.on_post
            .on_post(shared, handler_context, http_context, value)
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
        value: Put,
    ) -> Self::OnPutHandler<'a> {
        self.on_put
            .on_put(shared, handler_context, http_context, value)
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
        self.on_delete
            .on_delete(shared, handler_context, http_context)
    }
}

enum HttpLifecycleHandlerInner<'a, Context, Get, Post, Put, LC>
where
    LC: HttpLaneLifecycle<Get, Post, Put, Context> + 'a,
{
    Get(Option<Mime>, <LC as OnGet<Get, Context>>::OnGetHandler<'a>),
    Head(Option<Mime>, <LC as OnGet<Get, Context>>::OnGetHandler<'a>),
    Post(<LC as OnPost<Post, Context>>::OnPostHandler<'a>),
    Put(<LC as OnPut<Put, Context>>::OnPutHandler<'a>),
    Delete(<LC as OnDelete<Context>>::OnDeleteHandler<'a>),
}

pub struct HttpLifecycleHandler<'a, Context, Get, Post, Put, Codec, LC>
where
    LC: HttpLaneLifecycle<Get, Post, Put, Context> + 'a,
    Codec: HttpLaneCodec<Get>,
{
    inner: HttpLifecycleHandlerInner<'a, Context, Get, Post, Put, LC>,
    response_tx: Option<HttpResponseSender>,
    codec: Codec,
}

fn extract_accepts(headers: &[Header]) -> Vec<Mime> {
    let header_reader = Headers::new(headers);
    header_reader
        .accept()
        .filter_map(|r| r.ok())
        .collect::<Vec<_>>()
}

impl<'a, Context, Get, Post, Put, Codec, LC>
    HttpLifecycleHandler<'a, Context, Get, Post, Put, Codec, LC>
where
    LC: HttpLaneLifecycle<Get, Post, Put, Context>,
    Codec: HttpLaneCodec<Get>,
{
    pub fn new(req: RequestAndChannel<Post, Put>, codec: Codec, lifecycle: &'a LC) -> Self {
        let RequestAndChannel {
            request:
                Request {
                    method_and_payload,
                    uri,
                    headers,
                },
            response_tx,
        } = req;
        let http_context = HttpRequestContext::new(uri, headers);
        let inner = match method_and_payload {
            MethodAndPayload::Get => {
                let accepts = extract_accepts(http_context.headers.as_slice());

                HttpLifecycleHandlerInner::Get(
                    codec.select_codec(&accepts).cloned(),
                    lifecycle.on_get(http_context),
                )
            }
            MethodAndPayload::Head => {
                let accepts = extract_accepts(http_context.headers.as_slice());
                HttpLifecycleHandlerInner::Head(
                    codec.select_codec(&accepts).cloned(),
                    lifecycle.on_get(http_context),
                )
            }
            MethodAndPayload::Post(body) => {
                HttpLifecycleHandlerInner::Post(lifecycle.on_post(http_context, body))
            }
            MethodAndPayload::Put(body) => {
                HttpLifecycleHandlerInner::Put(lifecycle.on_put(http_context, body))
            }
            MethodAndPayload::Delete => {
                HttpLifecycleHandlerInner::Delete(lifecycle.on_delete(http_context))
            }
        };
        HttpLifecycleHandler {
            inner,
            response_tx: Some(response_tx),
            codec,
        }
    }
}

macro_rules! step {
    ($step_result:expr, $response_tx:expr, $to_bytes:expr) => (step!($step_result, $response_tx, $to_bytes,));
    ($step_result:expr, $response_tx:expr, $to_bytes:expr, $($err_pat:pat => $err_expr: expr)?) => {
        match $step_result {
            StepResult::Continue { modified_item } => return StepResult::Continue { modified_item },
            $(StepResult::Fail($err_pat) => $err_expr,)?
            StepResult::Fail(err) => {
                $response_tx.take();
                return StepResult::Fail(err);
            }
            StepResult::Complete {
                modified_item,
                result,
            } => {
                let response = $to_bytes(HttpResponse::from(result));
                (modified_item, response)
            }
        }
    };
}

impl<Context, Get, Post, Put, Codec, LC> HandlerAction<Context>
    for HttpLifecycleHandler<'_, Context, Get, Post, Put, Codec, LC>
where
    Context: AgentDescription,
    Codec: HttpLaneCodec<Get>,
    LC: HttpLaneLifecycle<Get, Post, Put, Context>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let HttpLifecycleHandler {
            inner,
            response_tx,
            codec,
        } = self;
        if response_tx.is_none() {
            return StepResult::after_done();
        }

        let (modified_item, response) = match inner {
            HttpLifecycleHandlerInner::Get(content_type, h) => {
                let encode_to_bytes = |response: HttpResponse<Get>| {
                    response_to_bytes(codec, content_type.as_ref(), response)
                };

                step!(
                    h.step(action_context, meta, context),
                    response_tx,
                    encode_to_bytes,
                    EventHandlerError::HttpGetUndefined => (None, not_supported())
                )
            }
            HttpLifecycleHandlerInner::Head(content_type, h) => {
                let head_to_bytes = |response: HttpResponse<Get>| {
                    discard_to_bytes(codec, content_type.as_ref(), response)
                };
                step!(
                    h.step(action_context, meta, context),
                    response_tx,
                    head_to_bytes,
                    EventHandlerError::HttpGetUndefined => (None, not_supported())
                )
            }
            HttpLifecycleHandlerInner::Post(h) => step!(
                h.step(action_context, meta, context),
                response_tx,
                empty_response_to_bytes
            ),
            HttpLifecycleHandlerInner::Put(h) => step!(
                h.step(action_context, meta, context),
                response_tx,
                empty_response_to_bytes
            ),
            HttpLifecycleHandlerInner::Delete(h) => step!(
                h.step(action_context, meta, context),
                response_tx,
                empty_response_to_bytes
            ),
        };
        if let Some(tx) = response_tx.take() {
            if let Err(response) = tx.send(response) {
                debug!(response = ?response, "HTTP request terminated before the response was provided.");
                StepResult::done(())
            } else {
                StepResult::Complete {
                    modified_item,
                    result: (),
                }
            }
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let HttpLifecycleHandler {
            inner, response_tx, ..
        } = self;
        if response_tx.is_none() {
            f.debug_tuple("HttpLifecycleHandler")
                .field(&"<<CONSUMED>>")
                .finish()
        } else {
            match inner {
                HttpLifecycleHandlerInner::Get(mime, handler) => f
                    .debug_struct("HttpLifecycleHandler")
                    .field("method", &"GET")
                    .field("mime_type", mime)
                    .field("handler", &Described::new(context, handler))
                    .finish(),
                HttpLifecycleHandlerInner::Head(mime, handler) => f
                    .debug_struct("HttpLifecycleHandler")
                    .field("method", &"HEAD")
                    .field("mime_type", mime)
                    .field("handler", &Described::new(context, handler))
                    .finish(),
                HttpLifecycleHandlerInner::Post(handler) => f
                    .debug_struct("HttpLifecycleHandler")
                    .field("method", &"POST")
                    .field("handler", &Described::new(context, handler))
                    .finish(),
                HttpLifecycleHandlerInner::Put(handler) => f
                    .debug_struct("HttpLifecycleHandler")
                    .field("method", &"PUT")
                    .field("handler", &Described::new(context, handler))
                    .finish(),
                HttpLifecycleHandlerInner::Delete(handler) => f
                    .debug_struct("HttpLifecycleHandler")
                    .field("method", &"DELETE")
                    .field("handler", &Described::new(context, handler))
                    .finish(),
            }
        }
    }
}

fn not_supported() -> RawHttpLaneResponse {
    RawHttpLaneResponse {
        status_code: StatusCode::METHOD_NOT_ALLOWED,
        version: Version::HTTP_1_1,
        headers: vec![],
        payload: Bytes::new(),
    }
}

fn response_to_bytes<T, Codec>(
    codec: &Codec,
    content_type: Option<&Mime>,
    response: HttpResponse<T>,
) -> RawHttpLaneResponse
where
    Codec: HttpLaneCodec<T>,
{
    let mut response = if let Some(content_type) = content_type {
        let HttpResponse {
            status_code,
            version,
            mut headers,
            payload,
        } = response;
        let mut buffer = BytesMut::new();
        if codec.encode(content_type, &payload, &mut buffer).is_ok() {
            let payload = buffer.freeze();
            headers.push(content_type_header(content_type));
            HttpResponse {
                status_code,
                version,
                headers,
                payload,
            }
        } else {
            server_error()
        }
    } else {
        bad_content_type()
    };
    add_content_len_header(&mut response);
    response
}

fn server_error() -> RawHttpLaneResponse {
    RawHttpLaneResponse {
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
        version: Version::HTTP_1_1,
        headers: vec![],
        payload: Bytes::new(),
    }
}

fn bad_content_type() -> RawHttpLaneResponse {
    RawHttpLaneResponse {
        status_code: StatusCode::UNSUPPORTED_MEDIA_TYPE,
        version: Version::HTTP_1_1,
        headers: vec![],
        payload: Bytes::new(),
    }
}

fn empty_response_to_bytes(response: HttpResponse<()>) -> RawHttpLaneResponse {
    response.map(|_| Bytes::new())
}

fn discard_to_bytes<T, Codec>(
    codec: &Codec,
    content_type: Option<&Mime>,
    response: HttpResponse<T>,
) -> RawHttpLaneResponse
where
    Codec: HttpLaneCodec<T>,
{
    response_to_bytes(codec, content_type, response).map(|_| Bytes::new())
}

enum HttpLifecycleHandlerSharedInner<'a, Context, Shared, Get, Post, Put, LC>
where
    Shared: 'a,
    LC: HttpLaneLifecycleShared<Get, Post, Put, Context, Shared> + 'a,
{
    Get(
        Option<Mime>,
        <LC as OnGetShared<Get, Context, Shared>>::OnGetHandler<'a>,
    ),
    Head(
        Option<Mime>,
        <LC as OnGetShared<Get, Context, Shared>>::OnGetHandler<'a>,
    ),
    Post(<LC as OnPostShared<Post, Context, Shared>>::OnPostHandler<'a>),
    Put(<LC as OnPutShared<Put, Context, Shared>>::OnPutHandler<'a>),
    Delete(<LC as OnDeleteShared<Context, Shared>>::OnDeleteHandler<'a>),
}

pub struct HttpLifecycleHandlerShared<'a, Context, Shared, Get, Post, Put, Codec, LC>
where
    LC: HttpLaneLifecycleShared<Get, Post, Put, Context, Shared> + 'a,
{
    inner: HttpLifecycleHandlerSharedInner<'a, Context, Shared, Get, Post, Put, LC>,
    response_tx: Option<HttpResponseSender>,
    codec: Codec,
}

impl<'a, Context, Shared, Get, Post, Put, Codec, LC>
    HttpLifecycleHandlerShared<'a, Context, Shared, Get, Post, Put, Codec, LC>
where
    Shared: 'a,
    LC: HttpLaneLifecycleShared<Get, Post, Put, Context, Shared>,
    Codec: HttpLaneCodec<Get>,
{
    pub fn new(
        req: RequestAndChannel<Post, Put>,
        shared: &'a Shared,
        codec: Codec,
        lifecycle: &'a LC,
    ) -> Self {
        let RequestAndChannel {
            request:
                Request {
                    method_and_payload,
                    uri,
                    headers,
                },
            response_tx,
        } = req;
        let http_context = HttpRequestContext::new(uri, headers);
        let handler_context = HandlerContext::default();
        let inner =
            match method_and_payload {
                MethodAndPayload::Get => {
                    let accepts = extract_accepts(http_context.headers.as_slice());

                    HttpLifecycleHandlerSharedInner::Get(
                        codec.select_codec(&accepts).cloned(),
                        lifecycle.on_get(shared, handler_context, http_context),
                    )
                }
                MethodAndPayload::Head => {
                    let accepts = extract_accepts(http_context.headers.as_slice());

                    HttpLifecycleHandlerSharedInner::Head(
                        codec.select_codec(&accepts).cloned(),
                        lifecycle.on_get(shared, handler_context, http_context),
                    )
                }
                MethodAndPayload::Post(body) => HttpLifecycleHandlerSharedInner::Post(
                    lifecycle.on_post(shared, handler_context, http_context, body),
                ),
                MethodAndPayload::Put(body) => HttpLifecycleHandlerSharedInner::Put(
                    lifecycle.on_put(shared, handler_context, http_context, body),
                ),
                MethodAndPayload::Delete => HttpLifecycleHandlerSharedInner::Delete(
                    lifecycle.on_delete(shared, handler_context, http_context),
                ),
            };
        HttpLifecycleHandlerShared {
            inner,
            response_tx: Some(response_tx),
            codec,
        }
    }
}

impl<Context, Shared, Get, Post, Put, Codec, LC> HandlerAction<Context>
    for HttpLifecycleHandlerShared<'_, Context, Shared, Get, Post, Put, Codec, LC>
where
    Context: AgentDescription,
    Codec: HttpLaneCodec<Get>,
    LC: HttpLaneLifecycleShared<Get, Post, Put, Context, Shared>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let HttpLifecycleHandlerShared {
            inner,
            response_tx,
            codec,
        } = self;
        if response_tx.is_none() {
            return StepResult::after_done();
        }
        let (modified_item, response) = match inner {
            HttpLifecycleHandlerSharedInner::Get(content_type, h) => {
                let encode_to_bytes = |response: HttpResponse<Get>| {
                    response_to_bytes(codec, content_type.as_ref(), response)
                };
                step!(
                    h.step(action_context, meta, context),
                    response_tx,
                    encode_to_bytes,
                    EventHandlerError::HttpGetUndefined => (None, not_supported())
                )
            }
            HttpLifecycleHandlerSharedInner::Head(content_type, h) => {
                let head_to_bytes = |response: HttpResponse<Get>| {
                    discard_to_bytes(codec, content_type.as_ref(), response)
                };
                step!(
                    h.step(action_context, meta, context),
                    response_tx,
                    head_to_bytes,
                    EventHandlerError::HttpGetUndefined => (None, not_supported())
                )
            }
            HttpLifecycleHandlerSharedInner::Post(h) => step!(
                h.step(action_context, meta, context),
                response_tx,
                empty_response_to_bytes
            ),
            HttpLifecycleHandlerSharedInner::Put(h) => step!(
                h.step(action_context, meta, context),
                response_tx,
                empty_response_to_bytes
            ),
            HttpLifecycleHandlerSharedInner::Delete(h) => step!(
                h.step(action_context, meta, context),
                response_tx,
                empty_response_to_bytes
            ),
        };
        if let Some(tx) = response_tx.take() {
            if let Err(response) = tx.send(response) {
                debug!(response = ?response, "HTTP request terminated before the response was provided.");
                StepResult::done(())
            } else {
                StepResult::Complete {
                    modified_item,
                    result: (),
                }
            }
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let HttpLifecycleHandlerShared {
            inner, response_tx, ..
        } = self;
        if response_tx.is_none() {
            f.debug_tuple("HttpLifecycleHandlerShared")
                .field(&"<<CONSUMED>>")
                .finish()
        } else {
            match inner {
                HttpLifecycleHandlerSharedInner::Get(mime, handler) => f
                    .debug_struct("HttpLifecycleHandlerShared")
                    .field("method", &"GET")
                    .field("mime_type", mime)
                    .field("handler", &Described::new(context, handler))
                    .finish(),
                HttpLifecycleHandlerSharedInner::Head(mime, handler) => f
                    .debug_struct("HttpLifecycleHandlerShared")
                    .field("method", &"HEAD")
                    .field("mime_type", mime)
                    .field("handler", &Described::new(context, handler))
                    .finish(),
                HttpLifecycleHandlerSharedInner::Post(handler) => f
                    .debug_struct("HttpLifecycleHandlerShared")
                    .field("method", &"POST")
                    .field("handler", &Described::new(context, handler))
                    .finish(),
                HttpLifecycleHandlerSharedInner::Put(handler) => f
                    .debug_struct("HttpLifecycleHandlerShared")
                    .field("method", &"PUT")
                    .field("handler", &Described::new(context, handler))
                    .finish(),
                HttpLifecycleHandlerSharedInner::Delete(handler) => f
                    .debug_struct("HttpLifecycleHandlerShared")
                    .field("method", &"DELETE")
                    .field("handler", &Described::new(context, handler))
                    .finish(),
            }
        }
    }
}
