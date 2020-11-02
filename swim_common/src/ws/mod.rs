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

#[cfg(feature = "tls")]
use {
    crate::ws::error::CertificateError,
    native_tls::Certificate,
    std::fs::File,
    std::io::{BufReader, Read},
    std::path::Path,
};

use std::str::FromStr;

use futures::{Future, Sink, Stream};
use http::uri::Scheme;
use http::{Request, Uri};

use crate::ws::error::{ConnectionError, WebSocketError};
use std::fmt;
use std::fmt::{Debug, Formatter};

pub mod error;

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

#[derive(Clone)]
pub enum Protocol {
    PlainText,
    #[cfg(feature = "tls")]
    Tls(Certificate),
}

impl PartialEq for Protocol {
    fn eq(&self, other: &Self) -> bool {
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
    pub fn tls(path: impl AsRef<Path>) -> Result<Protocol, CertificateError> {
        let cert = build_x509_certificate(path)?;
        Ok(Protocol::Tls(cert))
    }
}

impl Debug for Protocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlainText => write!(f, "PlainText"),
            #[cfg(feature = "tls")]
            Self::Tls(_) => write!(f, "Tls"),
        }
    }
}

/// If the request scheme is `warp`, `swim`, `swims` or `warps` then it is replaced with a
/// supported format. If the scheme is invalid then an error is returned.
pub fn maybe_resolve_scheme<T>(request: Request<T>) -> Result<Request<T>, WebSocketError> {
    let uri = request.uri().clone();
    let new_scheme = match uri.scheme_str() {
        Some("swim") | Some("warp") | Some("ws") => Ok("ws"),
        Some("swims") | Some("warps") | Some("wss") => Ok("wss"),
        Some(s) => Err(WebSocketError::unsupported_scheme(s)),
        None => Err(WebSocketError::missing_scheme()),
    }?;

    let (mut request_parts, request_t) = request.into_parts();
    let mut uri_parts = uri.into_parts();
    uri_parts.scheme = Some(Scheme::from_str(new_scheme)?);

    // infallible as `ws` and `wss` are valid schemes and the previous scheme already parsed.
    let uri = Uri::from_parts(uri_parts).unwrap();
    request_parts.uri = uri;

    Ok(Request::from_parts(request_parts, request_t))
}

#[cfg(feature = "tls")]
pub fn build_x509_certificate(path: impl AsRef<Path>) -> Result<Certificate, CertificateError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut buf = vec![];
    reader.read_to_end(&mut buf)?;

    match Certificate::from_pem(&buf) {
        Ok(cert) => Ok(cert),
        Err(e) => Err(CertificateError::SSL(e.to_string())),
    }
}

#[derive(Copy, Clone, PartialOrd, PartialEq)]
pub enum CompressionKind {
    None,
    Deflate,
    Bzip,
    Brotli,
}

impl CompressionKind {
    pub fn is_compressed(&self) -> bool {
        *self != CompressionKind::None
    }
}
