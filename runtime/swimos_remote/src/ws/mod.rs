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
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::OnceLock;

use futures::future::BoxFuture;
use futures::Stream;
use swimos_utilities::errors::Recoverable;
use tokio::net::TcpStream;

use ratchet::{ExtensionProvider, ProtocolRegistry, WebSocket, WebSocketConfig, WebSocketStream};
pub use swimos_ratchet::*;
pub use switcher::StreamSwitcher;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::net::{Listener, ListenerError};
use crate::FindNode;

mod swimos_ratchet;

mod switcher;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct RatchetError(#[from] ratchet::Error);
impl Recoverable for RatchetError {
    fn is_fatal(&self) -> bool {
        true
    }
}

pub type WebSocketDef<E> = WebSocket<TcpStream, E>;

pub type StreamDef = TcpStream;

pub type WsOpenFuture<'l, Sock, Ext, Error> = BoxFuture<'l, Result<WebSocket<Sock, Ext>, Error>>;

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
        Provider: ExtensionProvider + Send + Sync + 'static,
        Provider::Extension: Send + Sync + 'static;
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
    /// * `find_nodes` - Channel used to find running agents.
    fn wrap_listener<Sock, L, Provider>(
        &self,
        listener: L,
        provider: Provider,
        find_nodes: mpsc::Sender<FindNode>,
    ) -> Self::WsStream<Sock, Provider::Extension>
    where
        Sock: WebSocketStream + Send + Sync,
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

static PROTOCOLS: OnceLock<HashSet<&'static str>> = OnceLock::new();

pub fn protocols() -> &'static HashSet<&'static str> {
    PROTOCOLS.get_or_init(|| {
        let mut s = HashSet::new();
        s.insert("warp0");
        s
    })
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
        Provider: ExtensionProvider + Send + Sync + 'static,
        Provider::Extension: Send + Sync + 'static,
    {
        let config = self.0;
        Box::pin(async move {
            let subprotocols = ProtocolRegistry::new(protocols().iter().copied())?;
            let socket = ratchet::subscribe_with(config, socket, addr, provider, subprotocols)
                .await?
                .into_websocket();
            Ok(socket)
        })
    }
}
