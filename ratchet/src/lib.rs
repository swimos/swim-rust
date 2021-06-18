// Copyright 2015-2021 SWIM.AI inc.
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

#![allow(warnings)]

mod builder;

use futures::future::BoxFuture;
use http::uri::InvalidUri;
use http::{Request, Response, Uri};
use std::error::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::TlsConnector;
use url::Url;

pub enum ConnectionError {
    BadRequest(RequestError),
}

impl From<RequestError> for ConnectionError {
    fn from(e: RequestError) -> Self {
        ConnectionError::BadRequest(e)
    }
}

pub struct DeflateConfig;

pub enum CompressionConfig {
    None,
    Deflate(DeflateConfig),
}

pub struct WebSocketConfig {
    // options..
    pub compression: CompressionConfig,
}

pub trait Interceptor {
    fn intercept(
        self,
        request: Request<()>,
        response: Response<()>,
    ) -> BoxFuture<'static, Response<()>>;
}

enum Role {
    Client,
    Server,
}

pub struct WebSocket<S> {
    stream: S,
    config: WebSocketConfig,
}

impl<S> WebSocket<S>
where
    S: WebSocketStream,
{
    pub async fn client(
        config: WebSocketConfig,
        stream: S,
        connector: TlsConnector,
        request: Request<()>,
    ) -> Result<WebSocket<S>, ConnectionError> {
        // perform handshake...
        unimplemented!()
    }

    pub async fn server<I>(
        config: WebSocketConfig,
        stream: S,
        interceptor: I,
    ) -> Result<WebSocket<S>, ConnectionError>
    where
        I: Interceptor,
    {
        // perform handshake...
        unimplemented!()
    }
}

pub trait WebSocketStream: AsyncRead + AsyncWrite + Unpin {}

pub struct RequestError(String);

impl From<InvalidUri> for RequestError {
    fn from(e: InvalidUri) -> Self {
        RequestError(e.to_string())
    }
}

impl From<http::Error> for RequestError {
    fn from(e: http::Error) -> Self {
        RequestError(e.to_string())
    }
}

pub trait TryIntoRequest {
    fn try_into_request(self) -> Result<Request<()>, RequestError>;
}

impl<'a> TryIntoRequest for &'a str {
    fn try_into_request(self) -> Result<Request<()>, RequestError> {
        self.parse::<Uri>()?.try_into_request()
    }
}

impl<'a> TryIntoRequest for &'a String {
    fn try_into_request(self) -> Result<Request<()>, RequestError> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for String {
    fn try_into_request(self) -> Result<Request<()>, RequestError> {
        self.as_str().try_into_request()
    }
}

impl<'a> TryIntoRequest for &'a Uri {
    fn try_into_request(self) -> Result<Request<()>, RequestError> {
        self.clone().try_into_request()
    }
}

impl TryIntoRequest for Uri {
    fn try_into_request(self) -> Result<Request<()>, RequestError> {
        Ok(Request::get(self).body(())?)
    }
}

impl<'a> TryIntoRequest for &'a Url {
    fn try_into_request(self) -> Result<Request<()>, RequestError> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for Url {
    fn try_into_request(self) -> Result<Request<()>, RequestError> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for Request<()> {
    fn try_into_request(self) -> Result<Request<()>, RequestError> {
        Ok(self)
    }
}
