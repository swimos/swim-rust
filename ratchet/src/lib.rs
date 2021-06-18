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

use derive_more::Display;
use std::convert::TryFrom;
use thiserror::Error;

#[derive(Display)]
enum OpCode {
    #[display(fmt = "{}", _0)]
    DataCode(DataCode),
    #[display(fmt = "{}", _0)]
    ControlCode(ControlCode),
}

#[derive(Display)]
enum DataCode {
    #[display(fmt = "Continuation")]
    Continuation = 0,
    #[display(fmt = "Text")]
    Text = 1,
    #[display(fmt = "Binary")]
    Binary = 2,
}

#[derive(Display)]
enum ControlCode {
    #[display(fmt = "Close")]
    Close = 8,
    #[display(fmt = "Ping")]
    Ping = 9,
    #[display(fmt = "Pong")]
    Pong = 10,
}

#[derive(Debug, Error)]
enum OpCodeParseErr {
    #[error("Reserved OpCode: `{0}`")]
    Reserved(u8),
    #[error("Invalid OpCode: `{0}`")]
    Invalid(u8),
}

impl TryFrom<u8> for OpCode {
    type Error = OpCodeParseErr;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OpCode::DataCode(DataCode::Continuation)),
            1 => Ok(OpCode::DataCode(DataCode::Text)),
            2 => Ok(OpCode::DataCode(DataCode::Binary)),
            r @ 3..=7 => Err(OpCodeParseErr::Reserved(r)),
            8 => Ok(OpCode::ControlCode(ControlCode::Close)),
            9 => Ok(OpCode::ControlCode(ControlCode::Ping)),
            10 => Ok(OpCode::ControlCode(ControlCode::Pong)),
            r @ 11..=15 => Err(OpCodeParseErr::Reserved(r)),
            e => Err(OpCodeParseErr::Invalid(e)),
        }
    }
}

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
    role: Role,
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

pub struct RequestError(Box<dyn Error>);

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
