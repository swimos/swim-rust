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

use bytes::{Bytes, BytesMut, BufMut};
use swim_api::agent::{HttpLaneRequest, HttpLaneResponse};
use swim_form::structural::read::StructuralReadable;
use swim_model::http::{HttpRequest, SupportedMethod};
use swim_recon::parser::RecognizerDecoder;
use tokio::sync::oneshot;
use tokio_util::codec::Decoder;

use crate::{
    event_handler::{ActionContext, HandlerAction, Modification, StepResult, EventHandlerError},
    meta::AgentMetadata,
    AgentItem,
};

use self::model::{MethodAndPayload, Request};

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

pub struct HttpLaneAccept<Context, Get, Post, Put> {
    projection: fn(&Context) -> &HttpLane<Get, Post, Put>,
    request: Option<RequestAndChannel<Post, Put>>,
}

impl<Context, Get, Post, Put> HttpLaneAccept<Context, Get, Post, Put> {
    pub fn new(
        projection: fn(&Context) -> &HttpLane<Get, Post, Put>,
        request: Request<Post, Put>,
        response_tx: oneshot::Sender<HttpLaneResponse>,
    ) -> Self {
        HttpLaneAccept {
            projection,
            request: Some(RequestAndChannel {
                request,
                response_tx,
            }),
        }
    }
}

impl<Context, Get, Post, Put> HandlerAction<Context> for HttpLaneAccept<Context, Get, Post, Put> {
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
        if let Some(request) = request.take() {
            let lane = projection(context);
            lane.request.replace(Some(request));
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

pub struct DecodeRequest<'a, Post, Put> {
    _target_type: PhantomData<fn() -> MethodAndPayload<Post, Put>>,
    request: Option<HttpLaneRequest>,
    buffer: &'a mut BytesMut,
}

impl<'a, Context, Post, Put> HandlerAction<Context> for DecodeRequest<'a, Post, Put>
where
    Post: StructuralReadable,
    Put: StructuralReadable,
{
    type Completion = RequestAndChannel<Post, Put>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let DecodeRequest { request, buffer, .. } = self;
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
                Some(SupportedMethod::Post) => {
                    match decode_payload::<Post>(buffer, payload) {
                        Ok(body) => MethodAndPayload::Post(body),
                        Err(err) => return StepResult::Fail(err),
                    }
                },
                Some(SupportedMethod::Put) => {
                    match decode_payload::<Put>(buffer, payload) {
                        Ok(body) => MethodAndPayload::Put(body),
                        Err(err) => return StepResult::Fail(err),
                    }
                },
                Some(SupportedMethod::Delete) => MethodAndPayload::Delete,
                None => todo!(),
            };
            StepResult::done(RequestAndChannel {
                request: Request {
                    method_and_payload,
                    uri,
                    headers,
                },
                response_tx,
            })
        } else {
            StepResult::after_done()
        }
    }
}

fn decode_payload<T: StructuralReadable>(
    buffer: &mut BytesMut,
    payload: Bytes,
) -> Result<T, EventHandlerError>
where
    T: StructuralReadable,
{
    buffer.clear();
    buffer.put(payload);
    let mut decoder = RecognizerDecoder::new(T::make_recognizer());
    
    match decoder.decode_eof(buffer) {
        Ok(Some(value)) => Ok(value),
        Ok(_) => todo!(),
        Err(e) => Err(EventHandlerError::InvalidHttpRequest(Box::new(e))),
    }
}
