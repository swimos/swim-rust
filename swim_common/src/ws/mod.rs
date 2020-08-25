// Copyright 2015-2020 SWIM.AI inc.
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

use std::path::PathBuf;
use std::str::FromStr;

use futures::{Future, Sink, Stream};
use http::header::{HeaderName, CONTENT_ENCODING, SEC_WEBSOCKET_EXTENSIONS};
use http::uri::{InvalidUri, Scheme};
use http::{HeaderValue, Request, Response};
use url::Url;

use crate::ws::error::{ConnectionError, WebSocketError};

mod compression;
pub mod error;

pub const UNSUPPORTED_SCHEME: &str = "Unsupported URL scheme";

/// An enumeration representing a WebSocket message. Variants are based on IETF RFC-6455
/// (The WebSocket Protocol) and may be Text (0x1) or Binary (0x2).
#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Clone)]
pub enum WsMessage {
    /// The payload data is text encoded as UTF-8.
    Text(String),
    /// The payload data is arbitrary binary data.
    Binary(Vec<u8>),
}

impl From<String> for WsMessage {
    fn from(s: String) -> Self {
        WsMessage::Text(s)
    }
}

impl From<&str> for WsMessage {
    fn from(s: &str) -> Self {
        WsMessage::Text(s.to_string())
    }
}

impl From<Vec<u8>> for WsMessage {
    fn from(v: Vec<u8>) -> Self {
        WsMessage::Binary(v)
    }
}

/// Trait for factories that asynchronously create web socket connections. This exists primarily
/// to allow for alternative implementations to be provided during testing.
pub trait WebsocketFactory: Send + Sync {
    /// Type of the stream of incoming messages.
    type WsStream: Stream<Item = Result<WsMessage, ConnectionError>> + Unpin + Send + 'static;

    /// Type of the sink for outgoing messages.
    type WsSink: Sink<WsMessage> + Unpin + Send + 'static;

    type ConnectFut: Future<Output = Result<(Self::WsSink, Self::WsStream), ConnectionError>>
        + Send
        + 'static;

    /// Open a connection to the provided remote URL.
    fn connect(&mut self, url: url::Url) -> Self::ConnectFut;
}

pub trait WebSocketHandler {
    fn on_request(&mut self, _request: &mut Request<()>) {}

    fn on_response(&mut self, _response: &mut Response<()>) {}
}

#[derive(Clone)]
pub enum StreamType {
    Plain,
    Tls(Option<PathBuf>),
}

#[derive(Clone)]
pub struct WebSocketConfig {
    pub stream_type: StreamType,
    // pub extensions: Vec<Box<dyn WebSocketHandler>>,
}

/// If the request scheme is `warp` or `warps` then it is replaced with a supported format.
pub fn maybe_normalise_url<T>(request: &Request<T>) -> Result<(), ConnectionError> {
    let uri = request.uri().clone();
    let new_scheme = match uri.scheme_str() {
        Some("warp") => Some("ws"),
        Some("warps") => Some("wss"),
        Some(s) => Some(s),
        None => None,
    }
    .ok_or_else(|| WebSocketError::Url(String::from(UNSUPPORTED_SCHEME)))?;

    request
        .uri()
        .scheme()
        .replace(&Scheme::from_str(new_scheme)?);

    Ok(())
}
