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
mod extensions;

use futures::future::BoxFuture;
use http::uri::InvalidUri;
use http::{Uri};
use std::error::Error as StdError;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::TlsConnector;
use url::Url;
use crate::extensions::{Extension, ExtensionHandshake};

pub(crate) type Request = http::Request<()>;
pub(crate) type Response = http::Response<()>;

pub enum Error {
    BadRequest(RequestError),
}

impl From<RequestError> for Error {
    fn from(e: RequestError) -> Self {
        Error::BadRequest(e)
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
        request: Request,
        response: Response,
    ) -> BoxFuture<'static, Response>;
}

enum Role {
    Client,
    Server,
}

pub struct WebSocket<S, E> {
    stream: S,
    extension:E,
    role: Role,
    config: WebSocketConfig,
}

pub async fn client<S, E>(
    _config: WebSocketConfig,
    _stream: S,
    _request: Request,
    _extension: E,
    _subprotocols: Option<Vec<&'static str>>,
) -> Result<(WebSocket<S, E::Extension>, Option<String>), Error>
    where
        S: WebSocketStream,
        E: ExtensionHandshake,
{
    unimplemented!()
}

pub trait WebSocketStream: AsyncRead + AsyncWrite + Unpin {}

pub struct RequestError(Box<dyn StdError>);

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
