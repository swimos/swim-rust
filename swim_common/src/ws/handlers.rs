// Copyright 2015-2020 SWIM.AI inc.
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

use crate::ws::error::ConnectionError;
use crate::ws::{WebSocketHandler, WsMessage};
use http::header::CONTENT_ENCODING;
use http::{HeaderValue, Request, Response};
// use tracing::trace;

#[derive(Clone)]
struct LoggingHandler<I>
where
    I: WebSocketHandler,
{
    inner: I,
}

// todo: complete logging
impl<I> WebSocketHandler for LoggingHandler<I>
where
    I: WebSocketHandler,
{
    fn on_request(&mut self, request: &mut Request<()>) {
        self.inner.on_request(request);
    }

    fn on_response(&mut self, response: &mut Response<()>) {
        self.inner.on_response(response);
    }

    fn on_send(&mut self, message: &mut WsMessage) -> Result<(), ConnectionError> {
        self.inner.on_send(message)
    }

    fn on_receive(&mut self, message: &mut WsMessage) -> Result<(), ConnectionError> {
        self.inner.on_receive(message)
    }

    fn on_close(&mut self) {
        self.inner.on_close();
    }

    fn on_open(&mut self) {
        self.inner.on_open();
    }
}

/// Per-message compression strategies
#[derive(Clone)]
pub enum CompressionHandler {
    Deflate(DeflateHandler),
    Bzip(GzipHandler),
    Brotli(BrotliHandler),
}

impl WebSocketHandler for CompressionHandler {
    fn on_request(&mut self, request: &mut Request<()>) {
        match self {
            CompressionHandler::Deflate(h) => h.on_request(request),
            CompressionHandler::Bzip(h) => h.on_request(request),
            CompressionHandler::Brotli(h) => h.on_request(request),
        }
    }

    fn on_response(&mut self, response: &mut Response<()>) {
        match self {
            CompressionHandler::Deflate(h) => h.on_response(response),
            CompressionHandler::Bzip(h) => h.on_response(response),
            CompressionHandler::Brotli(h) => h.on_response(response),
        }
    }

    fn on_send(&mut self, message: &mut WsMessage) -> Result<(), ConnectionError> {
        match self {
            CompressionHandler::Deflate(h) => h.on_send(message),
            CompressionHandler::Bzip(h) => h.on_send(message),
            CompressionHandler::Brotli(h) => h.on_send(message),
        }
    }

    fn on_receive(&mut self, message: &mut WsMessage) -> Result<(), ConnectionError> {
        match self {
            CompressionHandler::Deflate(h) => h.on_receive(message),
            CompressionHandler::Bzip(h) => h.on_receive(message),
            CompressionHandler::Brotli(h) => h.on_receive(message),
        }
    }

    fn on_close(&mut self) {
        match self {
            CompressionHandler::Deflate(h) => h.on_close(),
            CompressionHandler::Bzip(h) => h.on_close(),
            CompressionHandler::Brotli(h) => h.on_close(),
        }
    }

    fn on_open(&mut self) {
        match self {
            CompressionHandler::Deflate(h) => h.on_open(),
            CompressionHandler::Bzip(h) => h.on_open(),
            CompressionHandler::Brotli(h) => h.on_open(),
        }
    }
}
#[derive(Clone)]
pub struct GzipHandler {
    enabled: bool,
}

impl WebSocketHandler for GzipHandler {
    fn on_request(&mut self, request: &mut Request<()>) {
        request
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("deflate"));
    }

    fn on_response(&mut self, _response: &mut Response<()>) {
        unimplemented!()
    }

    fn on_send(&mut self, _message: &mut WsMessage) -> Result<(), ConnectionError> {
        unimplemented!()
    }

    fn on_receive(&mut self, _message: &mut WsMessage) -> Result<(), ConnectionError> {
        unimplemented!()
    }
}
#[derive(Clone)]
pub struct BrotliHandler {
    enabled: bool,
}

impl WebSocketHandler for BrotliHandler {
    fn on_request(&mut self, request: &mut Request<()>) {
        request
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("deflate"));
    }

    fn on_response(&mut self, _response: &mut Response<()>) {
        unimplemented!()
    }

    fn on_send(&mut self, _message: &mut WsMessage) -> Result<(), ConnectionError> {
        unimplemented!()
    }

    fn on_receive(&mut self, _message: &mut WsMessage) -> Result<(), ConnectionError> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct DeflateHandler {
    enabled: bool,
}

impl WebSocketHandler for DeflateHandler {
    fn on_request(&mut self, request: &mut Request<()>) {
        request
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("deflate"));
    }

    fn on_response(&mut self, _response: &mut Response<()>) {
        unimplemented!()
    }

    fn on_send(&mut self, _message: &mut WsMessage) -> Result<(), ConnectionError> {
        unimplemented!()
    }

    fn on_receive(&mut self, _message: &mut WsMessage) -> Result<(), ConnectionError> {
        unimplemented!()
    }
}
