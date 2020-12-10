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

use crate::routing::ws::{CertificateError, WebSocketError};
use crate::routing::RoutingError;
use crate::sink::SinkSendError;
use crate::warp::envelope::Envelope;
use http::uri::InvalidUri;
use http::StatusCode;
use std::borrow::Cow;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::{fmt, io};
use swim_runtime::task::TaskError;
use tokio::sync::{mpsc, oneshot};
use utilities::errors::Recoverable;
#[cfg(feature = "tungstenite")]
use {std::ops::Deref, tokio_tungstenite::tungstenite};

#[cfg(test)]
mod tests;

/// Error type for routers that will return the envelope in the event that routing it fails.
#[derive(Clone, Debug, PartialEq)]
pub struct SendError {
    error: RoutingError,
    envelope: Envelope,
}

impl SendError {
    pub fn new(error: RoutingError, envelope: Envelope) -> Self {
        SendError { error, envelope }
    }

    pub fn split(self) -> (RoutingError, Envelope) {
        let SendError { error, envelope } = self;
        (error, envelope)
    }
}

impl Display for SendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl Error for SendError {}

impl From<mpsc::error::SendError<Envelope>> for SendError {
    fn from(err: mpsc::error::SendError<Envelope>) -> Self {
        SendError {
            error: RoutingError::RouterDropped,
            envelope: err.0,
        }
    }
}

fn io_fatal(kind: &ErrorKind) -> bool {
    !matches!(
        kind,
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::TimedOut
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionError {
    pub kind: ConnectionErrorKind,
    pub cause: Option<Cow<'static, str>>,
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.cause {
            Some(cause) => {
                write!(f, "{}. Caused by: {}", self.kind, cause)
            }
            None => {
                write!(f, "{}", self.kind)
            }
        }
    }
}

impl ConnectionError {
    pub fn new(kind: ConnectionErrorKind) -> ConnectionError {
        ConnectionError { kind, cause: None }
    }

    pub fn with_cause(kind: ConnectionErrorKind, cause: Cow<'static, str>) -> ConnectionError {
        ConnectionError {
            kind,
            cause: Some(cause),
        }
    }
}

/// A connection to a remote endpoint failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionErrorKind {
    ///
    ConnectError,
    /// The host could not be resolved.
    Resolution,
    /// An error occurred at the socket level.
    Socket(io::ErrorKind),
    /// An error occurred at the web socket protocol level.
    Websocket(WebSocketError),
    /// The remote host closed the connection.
    ClosedRemotely,
    /// An error occurred at the WARP protocol level.
    Warp,
    /// The connection was closed locally.
    Closed,
    /// The connection failed with the following status code.
    Http(StatusCode),
}

impl From<ConnectionErrorKind> for ConnectionError {
    fn from(e: ConnectionErrorKind) -> Self {
        ConnectionError::new(e)
    }
}

impl Display for ConnectionErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // todo
        match self {
            ConnectionErrorKind::ConnectError => {
                write!(f, "An error was produced during a connection.")
            }
            ConnectionErrorKind::Resolution => {
                write!(f, "The specified host could not be resolved.")
            }
            ConnectionErrorKind::Socket(err) => write!(f, "IO error: '{:?}'", err),
            ConnectionErrorKind::Websocket(err) => write!(f, "Websocket error: \"{}\"", err),
            ConnectionErrorKind::ClosedRemotely => {
                write!(f, "The connection was closed remotely.")
            }
            ConnectionErrorKind::Warp => {
                write!(f, "WARP error")
            }
            ConnectionErrorKind::Closed => write!(f, "The connection has been closed."),
            ConnectionErrorKind::Http(code) => {
                write!(f, "The connection failed with: \"{}\"", code)
            }
        }
    }
}

impl Error for ConnectionError {}

impl Recoverable for ConnectionError {
    fn is_fatal(&self) -> bool {
        match &self.kind {
            ConnectionErrorKind::Socket(err) => io_fatal(err),
            ConnectionErrorKind::Warp => false,
            _ => true,
        }
    }
}

#[cfg(feature = "tls")]
impl From<CertificateError> for ConnectionError {
    fn from(e: CertificateError) -> Self {
        ConnectionError::new(ConnectionErrorKind::Websocket(
            WebSocketError::CertificateError(e),
        ))
    }
}

impl From<WebSocketError> for ConnectionError {
    fn from(e: WebSocketError) -> Self {
        ConnectionError::new(ConnectionErrorKind::Websocket(e))
    }
}

impl From<InvalidUri> for WebSocketError {
    fn from(e: InvalidUri) -> Self {
        WebSocketError::Url(e.to_string())
    }
}

impl From<TaskError> for ConnectionError {
    fn from(_: TaskError) -> Self {
        ConnectionError::new(ConnectionErrorKind::ConnectError)
    }
}

impl<T> From<mpsc::error::SendError<T>> for ConnectionError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        ConnectionError::new(ConnectionErrorKind::ConnectError)
    }
}

impl From<oneshot::error::RecvError> for ConnectionError {
    fn from(_: oneshot::error::RecvError) -> Self {
        ConnectionError::new(ConnectionErrorKind::Closed)
    }
}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::new(ConnectionErrorKind::Socket(err.kind()))
    }
}

impl<T> From<SinkSendError<T>> for ConnectionError {
    fn from(_: SinkSendError<T>) -> Self {
        ConnectionError::new(ConnectionErrorKind::ClosedRemotely)
    }
}

impl From<futures::channel::mpsc::SendError> for ConnectionError {
    fn from(_: futures::channel::mpsc::SendError) -> Self {
        ConnectionError::new(ConnectionErrorKind::ClosedRemotely)
    }
}

impl From<http::Error> for ConnectionError {
    fn from(e: http::Error) -> Self {
        ConnectionError::new(ConnectionErrorKind::Websocket(WebSocketError::Url(
            e.to_string(),
        )))
    }
}

#[cfg(feature = "tungstenite")]
pub type TError = tungstenite::error::Error;

#[cfg(feature = "tungstenite")]
#[derive(Debug)]
pub struct TungsteniteError(pub tungstenite::error::Error);

#[cfg(feature = "tungstenite")]
impl Deref for TungsteniteError {
    type Target = TError;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "tungstenite")]
impl From<TungsteniteError> for ConnectionError {
    fn from(e: TungsteniteError) -> Self {
        match e.deref() {
            TError::ConnectionClosed => ConnectionError::new(ConnectionErrorKind::Closed),
            TError::Url(url) => ConnectionError::new(ConnectionErrorKind::Websocket(
                WebSocketError::Url(url.to_string()),
            )),
            TError::HttpFormat(e) => ConnectionError::with_cause(
                ConnectionErrorKind::Websocket(WebSocketError::Protocol),
                e.to_string().into(),
            ),
            TError::Http(e) => ConnectionError::new(ConnectionErrorKind::Http(e.status())),
            TError::Io(e) => ConnectionError::new(ConnectionErrorKind::Socket(e.kind())),
            e => ConnectionError::with_cause(
                ConnectionErrorKind::Socket(ErrorKind::Other),
                e.to_string().into(),
            ),
        }
    }
}
