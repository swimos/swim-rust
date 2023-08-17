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

use std::{borrow::Cow, cell::RefCell, marker::PhantomData};

use bytes::Bytes;
use swim_api::agent::{HttpLaneRequest, HttpLaneResponse};
use swim_model::http::{Header, HttpRequest, StatusCode, SupportedMethod, Version};
use tokio::sync::oneshot;
use tracing::debug;

use crate::{
    event_handler::{ActionContext, HandlerAction, StepResult},
    meta::AgentMetadata,
    AgentItem,
};

use self::{
    content_type::recon,
    headers::Headers,
    model::{MethodAndPayload, Request},
};

mod codec;
mod content_type;
mod headers;
pub mod lifecycle;
mod model;

pub use codec::{CodecError, DefaultCodec, HttpLaneCodec, HttpLaneCodecSupport};
pub use model::{Response, UnitResponse};

pub struct HttpLane<Get, Post, Put = Post, Codec = DefaultCodec> {
    id: u64,
    _type: PhantomData<fn(Post, Put) -> Get>,
    request: RefCell<Option<RequestAndChannel<Post, Put>>>,
    codec: Codec,
}

impl<Get, Post, Put, Codec> HttpLane<Get, Post, Put, Codec>
where
    Codec: Default,
{
    pub fn new(id: u64) -> Self {
        HttpLane {
            id,
            _type: Default::default(),
            request: Default::default(),
            codec: Default::default(),
        }
    }
}

impl<Get, Post, Put, Codec> HttpLane<Get, Post, Put, Codec> {
    pub(crate) fn take_request(&self) -> Option<RequestAndChannel<Post, Put>> {
        self.request.borrow_mut().take()
    }
}

impl<Get, Post, Put, Codec> HttpLane<Get, Post, Put, Codec>
where
    Codec: Clone,
{
    pub fn codec(&self) -> Codec {
        self.codec.clone()
    }
}

impl<Get, Post, Put, Codec> AgentItem for HttpLane<Get, Post, Put, Codec> {
    fn id(&self) -> u64 {
        self.id
    }
}

pub struct RequestAndChannel<Post, Put> {
    request: Request<Post, Put>,
    response_tx: oneshot::Sender<HttpLaneResponse>,
}

pub struct HttpLaneAccept<Context, Get, Post, Put, Codec> {
    projection: fn(&Context) -> &HttpLane<Get, Post, Put, Codec>,
    request: Option<HttpLaneRequest>,
}

impl<Context, Get, Post, Put, Codec> HttpLaneAccept<Context, Get, Post, Put, Codec> {
    pub fn new(
        projection: fn(&Context) -> &HttpLane<Get, Post, Put, Codec>,
        request: HttpLaneRequest,
    ) -> Self {
        HttpLaneAccept {
            projection,
            request: Some(request),
        }
    }
}

const REQ_DROPPED: &str = "HTTP request was dropped before it was fulfilled.";

impl<Context, Get, Post, Put, Codec> HandlerAction<Context>
    for HttpLaneAccept<Context, Get, Post, Put, Codec>
where
    Codec: HttpLaneCodec<Post> + HttpLaneCodec<Put>,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let HttpLaneAccept {
            projection,
            request,
        } = self;
        if let Some(HttpLaneRequest {
            request,
            response_tx,
        }) = request.take()
        {
            let lane = projection(context);
            let HttpRequest {
                method,
                uri,
                headers,
                payload,
                ..
            } = request;
            let method_and_payload = match method.supported_method() {
                Some(SupportedMethod::Get) => MethodAndPayload::Get,
                Some(SupportedMethod::Head) => MethodAndPayload::Head,
                Some(SupportedMethod::Post) => {
                    match try_decode_payload(&headers, &lane.codec, payload.as_ref()) {
                        Ok(body) => MethodAndPayload::Post(body),
                        Err(response) => {
                            if response_tx.send(response).is_err() {
                                debug!(REQ_DROPPED);
                            }
                            return StepResult::done(());
                        }
                    }
                }
                Some(SupportedMethod::Put) => {
                    match try_decode_payload(&headers, &lane.codec, payload.as_ref()) {
                        Ok(body) => MethodAndPayload::Put(body),
                        Err(response) => {
                            if response_tx.send(response).is_err() {
                                debug!(REQ_DROPPED);
                            }
                            return StepResult::done(());
                        }
                    }
                }
                Some(SupportedMethod::Delete) => MethodAndPayload::Delete,
                None => {
                    let response = bad_request(StatusCode::METHOD_NOT_ALLOWED, None);
                    if response_tx.send(response).is_err() {
                        debug!(REQ_DROPPED);
                    }
                    return StepResult::done(());
                }
            };
            let request_and_chan = RequestAndChannel {
                request: Request {
                    method_and_payload,
                    uri,
                    headers,
                },
                response_tx,
            };
            lane.request.replace(Some(request_and_chan));
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

fn bad_request(status_code: StatusCode, message: Option<String>) -> HttpLaneResponse {
    let payload = message.map(Bytes::from).unwrap_or_default();
    HttpLaneResponse {
        status_code,
        version: Version::HTTP_1_1,
        headers: vec![],
        payload,
    }
}

fn try_decode_payload<T, Codec>(
    headers: &[Header],
    codec: &Codec,
    payload: &[u8],
) -> Result<T, HttpLaneResponse>
where
    Codec: HttpLaneCodec<T>,
{
    let headers = Headers::new(headers);
    let content_type = match headers.content_type() {
        Ok(Some(mime)) => Cow::Owned(mime),
        Ok(_) => Cow::Borrowed(recon()),
        Err(_) => {
            return Err(bad_request(
                StatusCode::BAD_REQUEST,
                Some("Invalid content type header.".into()),
            ))
        }
    };
    match codec.decode(content_type.as_ref(), payload) {
        Ok(body) => Ok(body),
        Err(CodecError::UnsupportedContentType(ct)) => Err(bad_request(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Some(format!("Unsupported content type: {}", ct)),
        )),
        _ => Err(bad_request(StatusCode::INTERNAL_SERVER_ERROR, None)),
    }
}
