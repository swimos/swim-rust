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

#[cfg(test)]
mod fixture;

mod builder;
mod errors;
mod extensions;
mod framed;
mod handshake;
mod protocol;
mod ws;

pub use crate::extensions::{deflate::*, ext::*, Extension, ExtensionProvider};
pub use builder::WebSocketClientBuilder;
pub use errors::*;
pub use handshake::{ProtocolRegistry, TryIntoRequest};
pub use protocol::{Message, PayloadType, WebSocketConfig};
pub use ws::{client, Upgraded, WebSocket};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite};

pub(crate) type Request = http::Request<()>;
pub(crate) type Response = http::Response<()>;

pub trait WebSocketStream: AsyncRead + AsyncWrite + Unpin {}
impl<S> WebSocketStream for S where S: AsyncRead + AsyncWrite + Unpin {}

pub trait Interceptor {
    fn intercept(self, request: Request, response: Response) -> BoxFuture<'static, Response>;
}