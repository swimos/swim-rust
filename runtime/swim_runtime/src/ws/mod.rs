// Copyright 2015-2023 Swim Inc.
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

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;

use futures::future::BoxFuture;
use futures::Stream;
use swim_utilities::errors::Recoverable;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use ratchet::{
    Extension, ExtensionProvider, ProtocolRegistry, WebSocket, WebSocketConfig, WebSocketStream,
};
pub use swim_ratchet::*;
pub use switcher::StreamSwitcher;
use thiserror::Error;

use crate::net::{Listener, ListenerError};
use lazy_static::lazy_static;

#[cfg(feature = "tls")]
use {
    crate::error::tls::TlsError, crate::ws::tls::build_x509_certificate, std::path::Path,
    tokio_native_tls::native_tls::Certificate, tokio_native_tls::TlsStream,
};

mod swim_ratchet;

mod switcher;

#[cfg(feature = "tls")]
pub mod tls;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct RatchetError(#[from] ratchet::Error);
impl Recoverable for RatchetError {
    fn is_fatal(&self) -> bool {
        true
    }
}

#[cfg(feature = "tls")]
pub type WebSocketDef<E> = WebSocket<StreamSwitcher<TcpStream, TlsStream<TcpStream>>, E>;
#[cfg(not(feature = "tls"))]
pub type WebSocketDef<E> = WebSocket<TcpStream, E>;

#[cfg(feature = "tls")]
pub type StreamDef = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;
#[cfg(not(feature = "tls"))]
pub type StreamDef = TcpStream;

pub type WsOpenFuture<'l, Sock, Ext, Error> = BoxFuture<'l, Result<WebSocket<Sock, Ext>, Error>>;

#[derive(Clone)]
pub enum Protocol {
    PlainText,
    #[cfg(feature = "tls")]
    Tls(Certificate),
}

impl PartialEq for Protocol {
    fn eq(&self, other: &Self) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match (self, other) {
            (Protocol::PlainText, Protocol::PlainText) => true,
            #[cfg(feature = "tls")]
            (Protocol::Tls(_), Protocol::Tls(_)) => true,
            #[cfg(feature = "tls")]
            _ => false,
        }
    }
}

impl Protocol {
    #[cfg(feature = "tls")]
    pub fn tls(path: impl AsRef<Path>) -> Result<Protocol, TlsError> {
        let cert = build_x509_certificate(path)?;
        Ok(Protocol::Tls(cert))
    }
}

impl Debug for Protocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlainText => write!(f, "PlainText"),
            #[cfg(feature = "tls")]
            Self::Tls(_) => write!(f, "Tls"),
        }
    }
}

/// A ratchet extension provider that can be freely shared between tasks.
pub trait ShareableExtensionProvider: Send + Sync + 'static {
    type Extension: Extension + Send + Sync + 'static;
    type Provider: ExtensionProvider<Extension = Self::Extension> + Send + Sync + 'static;

    fn as_provider(&self) -> &Self::Provider;
}

impl<P> ShareableExtensionProvider for P
where
    P: ExtensionProvider + Send + Sync + 'static,
    P::Extension: Extension + Send + Sync + 'static,
{
    type Extension = P::Extension;

    type Provider = P;

    fn as_provider(&self) -> &Self::Provider {
        self
    }
}

/// Trait for adapters that will negotiate a client websocket connection over an duplex connection.
pub trait WebsocketClient {
    /// Negotiate a new client connection.
    ///
    /// # Arguments
    /// * `socket` - The connection.
    /// * `provider` - Provider for websocket extensions.
    /// * `addr` - The remote host.
    fn open_connection<'a, Sock, Provider>(
        &self,
        socket: Sock,
        provider: &'a Provider,
        addr: String,
    ) -> WsOpenFuture<'a, Sock, Provider::Extension, RatchetError>
    where
        Sock: WebSocketStream + Send,
        Provider: ShareableExtensionProvider;
}

/// Trait for adapters that can negotiate websocket connections for incoming TCP connections.
pub trait WebsocketServer: Send + Sync {
    type WsStream<Sock, Ext>: Stream<Item = Result<(WebSocket<Sock, Ext>, SocketAddr), ListenerError>>
        + Send
        + Unpin;

    /// Create a stream that will negotiate websocket connections on a stream of incoming duplex connections.
    /// This will typically be a TCP listener.
    ///
    /// #Arguments
    /// * `listener` - The stream of incoming connections.
    /// * `provider` - Provider of websocket extensions.
    fn wrap_listener<Sock, L, Provider>(
        &self,
        listener: L,
        provider: Provider,
    ) -> Self::WsStream<Sock, Provider::Extension>
    where
        Sock: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
        L: Listener<Sock> + Send + 'static,
        Provider: ExtensionProvider + Send + Sync + Unpin + 'static,
        Provider::Extension: Send + Sync + Unpin + 'static;
}

/// Combination trait for managing both server and client websocket connections.
pub trait Websockets: WebsocketClient + WebsocketServer {}

impl<W> Websockets for W where W: WebsocketClient + WebsocketServer {}

/// Standard websocket client implementation.
pub struct RatchetClient(WebSocketConfig);
impl From<WebSocketConfig> for RatchetClient {
    fn from(config: WebSocketConfig) -> Self {
        RatchetClient(config)
    }
}

lazy_static! {
    pub static ref PROTOCOLS: HashSet<&'static str> = {
        let mut s = HashSet::new();
        s.insert("warp0");
        s
    };
}

impl WebsocketClient for RatchetClient {
    fn open_connection<'a, Sock, Provider>(
        &self,
        socket: Sock,
        provider: &'a Provider,
        addr: String,
    ) -> WsOpenFuture<'a, Sock, Provider::Extension, RatchetError>
    where
        Sock: WebSocketStream + Send,
        Provider: ShareableExtensionProvider,
    {
        let config = self.0;
        Box::pin(async move {
            let subprotocols = ProtocolRegistry::new(PROTOCOLS.iter().copied())?;
            let socket =
                ratchet::subscribe_with(config, socket, addr, provider.as_provider(), subprotocols)
                    .await?
                    .into_websocket();
            Ok(socket)
        })
    }
}
