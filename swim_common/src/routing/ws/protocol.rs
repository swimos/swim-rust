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

use std::fmt;
use std::fmt::{Debug, Formatter};

#[cfg(feature = "tls")]
use {
    crate::routing::error::TlsError, crate::routing::ws::tls::build_x509_certificate,
    std::path::Path, tokio_native_tls::native_tls::Certificate,
};
use {
    tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode as TungCloseCode,
    tokio_tungstenite::tungstenite::protocol::CloseFrame, tokio_tungstenite::tungstenite::Message,
};

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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlainText => write!(f, "PlainText"),
            #[cfg(feature = "tls")]
            Self::Tls(_) => write!(f, "Tls"),
        }
    }
}

impl From<Message> for WsMessage {
    fn from(message: Message) -> Self {
        match message {
            Message::Text(msg) => WsMessage::Text(msg),
            Message::Binary(data) => WsMessage::Binary(data),
            Message::Ping(data) => WsMessage::Ping(data),
            Message::Pong(data) => WsMessage::Pong(data),
            Message::Close(reason) => WsMessage::Closed(reason.map(Into::into)),
        }
    }
}

impl<'t> From<CloseFrame<'t>> for CloseReason {
    fn from(frame: CloseFrame<'t>) -> Self {
        let CloseFrame { code, reason } = frame;

        let code = match code {
            TungCloseCode::Away => CloseCode::GoingAway,
            TungCloseCode::Normal => CloseCode::Normal,
            TungCloseCode::Protocol => CloseCode::ProtocolError,
            _ => CloseCode::Error,
        };

        CloseReason {
            code,
            reason: reason.to_string(),
        }
    }
}

impl<'t> From<CloseReason> for CloseFrame<'t> {
    fn from(reason: CloseReason) -> Self {
        let CloseReason { code, reason } = reason;
        let code = match code {
            CloseCode::GoingAway => TungCloseCode::Away,
            CloseCode::ProtocolError => TungCloseCode::Protocol,
            CloseCode::Error => TungCloseCode::Error,
            CloseCode::Normal => TungCloseCode::Normal,
        };

        CloseFrame {
            code,
            reason: reason.into(),
        }
    }
}

impl From<WsMessage> for Message {
    fn from(message: WsMessage) -> Self {
        match message {
            WsMessage::Text(msg) => Message::Text(msg),
            WsMessage::Binary(data) => Message::Binary(data),
            WsMessage::Ping(data) => Message::Ping(data),
            WsMessage::Pong(data) => Message::Pong(data),
            WsMessage::Closed(reason) => Message::Close(reason.map(Into::into)),
        }
    }
}

/// An enumeration representing a WebSocket message. Variants are based on IETF RFC-6455
/// (The WebSocket Protocol) and may be Text (0x1) or Binary (0x2).
#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Clone)]
pub enum WsMessage {
    /// The payload data is text encoded as UTF-8.
    Text(String),
    /// The payload data is arbitrary binary data.
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Closed(Option<CloseReason>),
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Clone)]
pub struct CloseReason {
    pub code: CloseCode,
    pub reason: String,
}

impl CloseReason {
    pub fn new(code: CloseCode, reason: String) -> CloseReason {
        CloseReason { code, reason }
    }
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Clone)]
pub enum CloseCode {
    GoingAway,
    ProtocolError,
    Error,
    Normal, //TODO Fill in others.
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
