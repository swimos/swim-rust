// Copyright 2015-2021 Swim Inc.
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

use std::fmt::{Debug, Formatter};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use ratchet::{SplittableExtension, WebSocket};
pub use swim_ratchet::*;
pub use switcher::StreamSwitcher;

#[cfg(feature = "tls")]
use {
    crate::error::TlsError, crate::ws::tls::build_x509_certificate, std::path::Path,
    tokio_native_tls::native_tls::Certificate, tokio_native_tls::TlsStream,
};

use crate::error::ConnectionError;

pub mod ext;
mod swim_ratchet;
pub mod utils;

mod switcher;

pub type WebSocketDef<E> = WebSocket<TcpStream, E>;
pub type StreamDef = TcpStream;

pub type WsOpenFuture<'l, Sock, Ext, Error> = BoxFuture<'l, Result<WebSocket<Sock, Ext>, Error>>;

pub trait WsConnections<Socket>
where
    Socket: Send + Sync + Unpin,
{
    type Ext: SplittableExtension + Send + Sync + 'static;
    type Error: Into<ConnectionError>;

    /// Negotiate a new client connection.
    fn open_connection(
        &self,
        socket: Socket,
        addr: String,
    ) -> WsOpenFuture<Socket, Self::Ext, Self::Error>;

    /// Negotiate a new server connection.
    fn accept_connection(&self, socket: Socket) -> WsOpenFuture<Socket, Self::Ext, Self::Error>;
}

/// Trait for factories that asynchronously create web socket connections. This exists primarily
/// to allow for alternative implementations to be provided during testing.
pub trait WebsocketFactory: Send + Sync {
    type Sock: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static;
    type Ext: SplittableExtension + Send + 'static;

    /// Open a connection to the provided remote URL.
    fn connect(&mut self, url: url::Url) -> WsOpenFuture<Self::Sock, Self::Ext, ConnectionError>;
}

#[derive(Clone)]
pub enum Protocol {
    PlainText,
}

impl PartialEq for Protocol {
    fn eq(&self, other: &Self) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match (self, other) {
            (Protocol::PlainText, Protocol::PlainText) => true,
        }
    }
}

impl Debug for Protocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlainText => write!(f, "PlainText"),
        }
    }
}
