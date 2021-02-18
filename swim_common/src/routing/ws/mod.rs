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

use futures::{Future, Sink, Stream};

use futures::future::BoxFuture;

mod protocol;
mod stream;
#[cfg(feature = "tungstenite")]
pub mod tungstenite;
pub mod utils;

use crate::routing::ConnectionError;
pub use protocol::*;
pub use stream::*;
pub use utils::*;

#[cfg(feature = "tls")]
pub mod tls;

pub type ConnResult<Snk, Str> = Result<(Snk, Str), ConnectionError>;
pub type ConnFuture<'a, Snk, Str> = BoxFuture<'a, ConnResult<Snk, Str>>;

/// Trait for factories that asynchronously create web socket connections. This exists primarily
/// to allow for alternative implementations to be provided during testing.
pub trait WebsocketFactory: Send + Sync {
    /// Type of the stream of incoming messages.
    type WsStream: Stream<Item = Result<WsMessage, ConnectionError>> + Unpin + Send + 'static;

    /// Type of the sink for outgoing messages.
    type WsSink: Sink<WsMessage> + Unpin + Send + 'static;

    /// Open a connection to the provided remote URL.
    fn connect(&mut self, url: url::Url) -> ConnFuture<Self::WsSink, Self::WsStream>;
}

/// Trait to provide a service to negotiate a web socket connection on top of a socket.
pub trait WsConnections<Sock: Send + Sync + Unpin> {
    type StreamSink: JoinedStreamSink<WsMessage, ConnectionError> + Send + Unpin + 'static;
    type Fut: Future<Output = Result<Self::StreamSink, ConnectionError>> + Send + 'static;

    /// Negotiate a new client connection.
    fn open_connection(&self, socket: Sock, addr: String) -> Self::Fut;

    /// Negotiate a new server connection.
    fn accept_connection(&self, socket: Sock) -> Self::Fut;
}
