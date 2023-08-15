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

use std::{cell::RefCell, marker::PhantomData};

use bytes::Bytes;
use swim_api::agent::{HttpLaneRequest, HttpLaneResponse};
use swim_model::http::{HttpRequest, StatusCode, SupportedMethod, Version};
use tokio::sync::oneshot;
use tracing::debug;

use crate::{
    event_handler::{ActionContext, HandlerAction, StepResult},
    meta::AgentMetadata,
    AgentItem,
};

use self::{
    codec::HttpLaneCodec,
    content_type::recon,
    model::{MethodAndPayload, Request},
};

mod codec;
mod content_type;
pub mod lifecycle;
mod model;

pub use model::{Response, UnitResponse};

pub struct HttpLane<Get, Post, Put = Post> {
    id: u64,
    _type: PhantomData<fn(Post, Put) -> Get>,
    request: RefCell<Option<RequestAndChannel<Post, Put>>>,
}

impl<Get, Post, Put> HttpLane<Get, Post, Put> {
    pub fn new(id: u64) -> Self {
        HttpLane {
            id,
            _type: Default::default(),
            request: Default::default(),
        }
    }

    pub(crate) fn take_request(&self) -> Option<RequestAndChannel<Post, Put>> {
        self.request.borrow_mut().take()
    }
}

impl<Get, Post, Put> AgentItem for HttpLane<Get, Post, Put> {
    fn id(&self) -> u64 {
        self.id
    }
}

pub struct RequestAndChannel<Post, Put> {
    request: Request<Post, Put>,
    response_tx: oneshot::Sender<HttpLaneResponse>,
}

pub struct HttpLaneAccept<Context, Get, Post, Put, Codec> {
    projection: fn(&Context) -> &HttpLane<Get, Post, Put>,
    request: Option<HttpLaneRequest>,
    codec: Codec,
}

impl<Context, Get, Post, Put, Codec> HttpLaneAccept<Context, Get, Post, Put, Codec> {
    pub fn new(
        projection: fn(&Context) -> &HttpLane<Get, Post, Put>,
        request: HttpLaneRequest,
        codec: Codec,
    ) -> Self {
        HttpLaneAccept {
            projection,
            request: Some(request),
            codec,
        }
    }
}

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
            codec,
        } = self;
        if let Some(HttpLaneRequest {
            request,
            response_tx,
        }) = request.take()
        {
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
                Some(SupportedMethod::Post) => match codec.decode(recon(), payload.as_ref()) {
                    Ok(body) => MethodAndPayload::Post(body),
                    _ => {
                        bad_request(response_tx, StatusCode::BAD_REQUEST);
                        return StepResult::done(());
                    }
                },
                Some(SupportedMethod::Put) => match codec.decode(recon(), payload.as_ref()) {
                    Ok(body) => MethodAndPayload::Put(body),
                    _ => {
                        bad_request(response_tx, StatusCode::BAD_REQUEST);
                        return StepResult::done(());
                    }
                },
                Some(SupportedMethod::Delete) => MethodAndPayload::Delete,
                None => {
                    bad_request(response_tx, StatusCode::METHOD_NOT_ALLOWED);
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
            let lane = projection(context);
            lane.request.replace(Some(request_and_chan));
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

fn bad_request(tx: oneshot::Sender<HttpLaneResponse>, status_code: StatusCode) {
    let response = HttpLaneResponse {
        status_code,
        version: Version::HTTP_1_1,
        headers: vec![],
        payload: Bytes::new(),
    };
    if tx.send(response).is_err() {
        debug!("HTTP request was dropped before it was fulfilled.");
    }
}
