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

#![deny(
    // missing_docs,
    missing_copy_implementations,
    missing_debug_implementations,
    trivial_numeric_casts,
    unstable_features,
    unused_must_use,
    unused_mut,
    unused_imports,
    unused_import_braces
)]

#[cfg(test)]
mod fixture;

mod builder;
mod errors;
mod ext;
mod framed;
mod handshake;
mod protocol;
mod ws;

#[cfg(feature = "split")]
mod split;

pub use builder::{WebSocketClientBuilder, WebSocketServerBuilder};
pub use errors::*;
pub use ext::{NoExt, NoExtProvider};
pub use handshake::{
    accept, accept_with, ProtocolRegistry, TryIntoRequest, WebSocketResponse, WebSocketUpgrader,
};
pub use protocol::{Message, PayloadType, Role, WebSocketConfig};
pub use ws::{client, Upgraded, WebSocket};

use tokio::io::{AsyncRead, AsyncWrite};

pub(crate) type Request = http::Request<()>;

pub trait WebSocketStream: AsyncRead + AsyncWrite + Unpin + 'static {}
impl<S> WebSocketStream for S where S: AsyncRead + AsyncWrite + Unpin + 'static {}
