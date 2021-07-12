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

pub mod builder;
mod errors;
mod extensions;
#[cfg(test)]
mod fixture;
mod handshake;
mod http_ext;
#[allow(warnings)]
mod protocol;

use crate::errors::Error;
use crate::extensions::{Extension, ExtensionHandshake, NegotiatedExtension};
use crate::handshake::{exec_client_handshake, HandshakeResult, ProtocolRegistry};
use crate::protocol::frame::Frame;
use crate::protocol::Message;
use futures::future::BoxFuture;
use futures::{Sink, Stream};
pub use http_ext::TryIntoRequest;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

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

pub struct WebSocket<S, E> {
    stream: S,
    role: Role,
    extension: NegotiatedExtension<E>,
    config: WebSocketConfig,
}

impl<S, E> WebSocket<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    pub fn role(&self) -> Role {
        self.role
    }

    pub async fn config(&self) -> &WebSocketConfig {
        &self.config
    }

    pub async fn send_owned<M>(&self, _message: M) -> Result<(), Error>
    where
        M: Into<Message>,
    {
        unimplemented!()
    }

    pub async fn send_borrowed(&mut self, _message: impl AsRef<[u8]>) -> Result<(), Error> {
        unimplemented!()
    }

    pub async fn send_frame(&mut self, frame: Frame, data: &[u8]) -> Result<(), Error> {
        unimplemented!()
    }
}

pub async fn client<S, E>(
    config: WebSocketConfig,
    mut stream: S,
    request: Request,
    extension: E,
    subprotocols: ProtocolRegistry,
) -> Result<(WebSocket<S, E::Extension>, Option<String>), Error>
where
    S: WebSocketStream,
    E: ExtensionHandshake,
{
    let HandshakeResult {
        protocol,
        extension,
    } = exec_client_handshake(&mut stream, request, extension, subprotocols).await?;
    let socket = WebSocket {
        stream,
        role: Role::Client,
        extension,
        config,
    };
    Ok((socket, protocol))
}

pub trait WebSocketStream: AsyncRead + AsyncWrite + Unpin {}
impl<S> WebSocketStream for S where S: AsyncRead + AsyncWrite + Unpin {}

impl<S, E> Stream for WebSocket<S, E> {
    type Item = Message;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

impl<S, E> Sink<Message> for WebSocket<S, E> {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }
}
