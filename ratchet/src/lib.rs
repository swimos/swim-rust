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

pub mod builder;
mod errors;
mod extensions;
#[cfg(test)]
mod fixture;
mod handshake;
mod http_ext;
mod protocol;

use crate::errors::{Error, HttpError};
use crate::extensions::ExtensionHandshake;
use crate::handshake::{exec_client_handshake, RequestError};
use futures::future::BoxFuture;
use http::uri::InvalidUri;
use http::Uri;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::TlsConnector;
use url::Url;

pub(crate) type Request = http::Request<()>;
pub(crate) type Response = http::Response<()>;

pub struct DeflateConfig;

pub enum CompressionConfig {
    None,
    Deflate(DeflateConfig),
}

impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig::None
    }
}

#[derive(Default)]
pub struct WebSocketConfig {
    // options..
    pub compression: CompressionConfig,
}

pub trait Interceptor {
    fn intercept(self, request: Request, response: Response) -> BoxFuture<'static, Response>;
}

#[derive(Copy, Clone, PartialEq)]
pub enum Role {
    Client,
    Server,
}

pub struct WebSocket<S> {
    stream: S,
    role: Role,
    config: WebSocketConfig,
}

impl<S> WebSocket<S>
where
    S: WebSocketStream,
{
    pub fn role(&self) -> Role {
        self.role
    }

    pub fn config(&self) -> &WebSocketConfig {
        &self.config
    }

    pub async fn client<E>(
        config: WebSocketConfig,
        mut stream: S,
        connector: Option<TlsConnector>,
        request: Request,
        extension: E,
    ) -> Result<WebSocket<S>, Error>
    where
        E: ExtensionHandshake,
    {
        let result =
            exec_client_handshake(&config, &mut stream, connector, request, extension).await?;
        Ok(WebSocket {
            stream,
            role: Role::Client,
            config,
        })
    }

    pub async fn server<I>(
        _config: WebSocketConfig,
        _stream: S,
        _interceptor: I,
    ) -> Result<WebSocket<S>, Error>
    where
        I: Interceptor,
    {
        // perform handshake...
        unimplemented!()
    }
}

pub trait WebSocketStream: AsyncRead + AsyncWrite + Unpin {}
impl<S> WebSocketStream for S where S: AsyncRead + AsyncWrite + Unpin {}

impl From<InvalidUri> for RequestError {
    fn from(e: InvalidUri) -> Self {
        RequestError(Box::new(e))
    }
}

impl From<http::Error> for RequestError {
    fn from(e: http::Error) -> Self {
        RequestError(Box::new(e))
    }
}

pub trait TryIntoRequest {
    fn try_into_request(self) -> Result<Request, RequestError>;
}

impl<'a> TryIntoRequest for &'a str {
    fn try_into_request(self) -> Result<Request, RequestError> {
        self.parse::<Uri>()?.try_into_request()
    }
}

impl<'a> TryIntoRequest for &'a String {
    fn try_into_request(self) -> Result<Request, RequestError> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for String {
    fn try_into_request(self) -> Result<Request, RequestError> {
        self.as_str().try_into_request()
    }
}

impl<'a> TryIntoRequest for &'a Uri {
    fn try_into_request(self) -> Result<Request, RequestError> {
        self.clone().try_into_request()
    }
}

impl TryIntoRequest for Uri {
    fn try_into_request(self) -> Result<Request, RequestError> {
        Ok(Request::get(self).body(())?)
    }
}

impl<'a> TryIntoRequest for &'a Url {
    fn try_into_request(self) -> Result<Request, RequestError> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for Url {
    fn try_into_request(self) -> Result<Request, RequestError> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for Request {
    fn try_into_request(self) -> Result<Request, RequestError> {
        Ok(self)
    }
}
