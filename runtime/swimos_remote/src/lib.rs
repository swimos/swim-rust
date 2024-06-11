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

//! # SwimOS Transport Layer
//!
//! This crate provides the transport layer for SwimOS (bidirectional web-socket connections over
//! TCP sockets). It consists of:
//!
//! - An abstraction over DNS to allow for the resolution of remote hosts of Swim agents.
//! - A networking abstraction with basic implementation for unencrypted traffic over TCP sockets.
//! - An optional implementation of the networking abstraction using TLS encryption.
//! - Bindings to use the [`ratchet`] web-socket library on top of the networking abstraction.
//! - A Tokio task to manage a bidirectional web-socket and handle communication with the core SwimOS runtime.

/// DNS support for resolving remote hosts.
pub mod dns;
mod net;

/// Basic networking support, without TLS support.
pub mod plain;
mod task;
/// Networking support with TLS provided by the [`rustls`] crate.
#[cfg(feature = "tls")]
pub mod tls;
mod ws;

pub use task::RemoteTask;

pub use net::{
    BadUrl, ClientConnections, ConnResult, ConnectionError, ExternalConnections, Listener,
    ListenerError, ListenerResult, Scheme, SchemeHostPort, ServerConnections,
};

/// Bindings to use the [`ratchet`] web-sockets crate with the networking abstraction in this crate.
pub mod websocket {

    pub use super::ws::{
        RatchetClient, RatchetError, WebsocketClient, WebsocketServer, Websockets, WsOpenFuture,
    };

    /// The name of the Warp protocol for negotiation web-socket connections.
    pub const WARP: &str = "warp0";
}
