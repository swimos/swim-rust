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

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::errors::Error;
use crate::extensions::deflate::Deflate;
pub use crate::extensions::{deflate::*, ext::*, ExtHandshakeErr, Extension, ExtensionHandshake};
pub use crate::http_ext::TryIntoRequest;

pub mod codec;
mod errors;
mod extensions;
#[cfg(test)]
mod fixture;
mod handshake;
mod http_ext;
pub mod owned;
#[allow(warnings)]
mod protocol;
pub mod split;

pub(crate) type Request = http::Request<()>;
pub(crate) type Response = http::Response<()>;

pub trait WebSocketStream: AsyncRead + AsyncWrite + Unpin {}
impl<S> WebSocketStream for S where S: AsyncRead + AsyncWrite + Unpin {}

#[derive(Clone, Default)]
pub struct WebSocketConfig {
    // options..
}

pub trait Interceptor {
    fn intercept(self, request: Request, response: Response) -> BoxFuture<'static, Response>;
}

impl<F> Interceptor for F
where
    F: Fn(Request, Response) -> BoxFuture<'static, Response>,
{
    fn intercept(self, request: Request, response: Response) -> BoxFuture<'static, Response> {
        (self)(request, response)
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum Role {
    Client,
    Server,
}
