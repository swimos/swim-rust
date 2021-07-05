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

use futures::channel::mpsc::SendError as FutSendError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::time::Duration;
use tokio::sync::mpsc::error::SendError as MpscSendError;

pub use capacity::*;
pub use closed::*;
pub use encoding::*;
pub use io::*;
pub use protocol::*;
pub use resolution::*;
pub use send::*;
#[cfg(feature = "tls")]
pub use tls::*;
use utilities::errors::Recoverable;
use utilities::sync::circular_buffer;
use utilities::uri::RelativeUri;
use {std::ops::Deref, tokio_tungstenite::tungstenite};

use crate::request::request_future::RequestError;
use crate::routing::RoutingAddr;

pub use self::http::*;

mod capacity;
mod closed;
mod encoding;
mod http;
mod io;
mod protocol;
mod resolution;
mod send;
#[cfg(feature = "tls")]
mod tls;

#[cfg(test)]
mod tests;

pub type FmtResult = std::fmt::Result;

/// An error returned by the router
#[derive(Clone, Debug, PartialEq)]
pub enum RoutingError {
    /// The connection to the remote host has been lost.
    ConnectionError,
    /// The remote host is unreachable.
    HostUnreachable,
    /// The connection pool has encountered an error.
    PoolError(ConnectionError),
    /// The router has been stopped.
    RouterDropped,
    /// The router has encountered an error while stopping.
    CloseError,
}

impl Recoverable for RoutingError {
    fn is_fatal(&self) -> bool {
        match &self {
            RoutingError::ConnectionError => false,
            RoutingError::HostUnreachable => false,
            RoutingError::PoolError(e) => e.is_fatal(),
            _ => true,
        }
    }
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::ConnectionError => write!(f, "Connection error."),
            RoutingError::HostUnreachable => write!(f, "Host unreachable."),
            RoutingError::PoolError(e) => write!(f, "Connection pool error. {}", e),
            RoutingError::RouterDropped => write!(f, "Router was dropped."),
            RoutingError::CloseError => write!(f, "Closing error."),
        }
    }
}

impl Error for RoutingError {}

impl<T> From<MpscSendError<T>> for RoutingError {
    fn from(_: MpscSendError<T>) -> Self {
        RoutingError::RouterDropped
    }
}

impl From<RoutingError> for RequestError {
    fn from(_: RoutingError) -> Self {
        RequestError {}
    }
}

impl<T> From<circular_buffer::error::SendError<T>> for RoutingError {
    fn from(_: circular_buffer::error::SendError<T>) -> Self {
        RoutingError::RouterDropped
    }
}

/// An error denoting that a connection error has occurred.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ConnectionError {
    /// A HTTP detailing either a malformatted request/response or a peer error.
    Http(HttpError),
    /// A TLS error that may be produced when reading a certificate or through a connection.
    #[cfg(feature = "tls")]
    Tls(TlsError),
    /// An error detailing that there has been a read/write buffer overflow.
    Capacity(CapacityError),
    /// A connection protocol error.
    Protocol(ProtocolError),
    /// An error produced when closing a connection or a normal close code.
    Closed(CloseError),
    /// An IO error produced during a read/write operation.
    Io(IoError),
    /// An unsupported encoding error or an illegal type error.
    Encoding(EncodingError),
    /// An error produced when attempting to resolve a peer.
    Resolution(ResolutionError),
    /// A pending write did not complete within the specified duration.
    WriteTimeout(Duration),
}

impl Recoverable for ConnectionError {
    fn is_fatal(&self) -> bool {
        match self {
            ConnectionError::Http(e) => e.is_fatal(),
            #[cfg(feature = "tls")]
            ConnectionError::Tls(e) => e.is_fatal(),
            ConnectionError::Capacity(e) => e.is_fatal(),
            ConnectionError::Protocol(e) => e.is_fatal(),
            ConnectionError::Closed(e) => e.is_fatal(),
            ConnectionError::Io(e) => matches!(
                e.kind(),
                ErrorKind::Interrupted | ErrorKind::TimedOut | ErrorKind::ConnectionReset
            ),
            ConnectionError::Encoding(e) => e.is_fatal(),
            ConnectionError::Resolution(e) => e.is_fatal(),
            ConnectionError::WriteTimeout(_) => false,
        }
    }
}

impl Error for ConnectionError {}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ConnectionError::Http(e) => write!(f, "{}", e),
            #[cfg(feature = "tls")]
            ConnectionError::Tls(e) => write!(f, "{}", e),
            ConnectionError::Capacity(e) => write!(f, "{}", e),
            ConnectionError::Protocol(e) => write!(f, "{}", e),
            ConnectionError::Closed(e) => write!(f, "{}", e),
            ConnectionError::Io(e) => write!(f, "{}", e),
            ConnectionError::Encoding(e) => write!(f, "{}", e),
            ConnectionError::Resolution(e) => write!(f, "{}", e),
            ConnectionError::WriteTimeout(dur) => write!(
                f,
                "Writing to the connection failed to complete within {:?}.",
                dur
            ),
        }
    }
}

pub type TError = tungstenite::error::Error;

#[derive(Debug)]
pub struct TungsteniteError(pub tungstenite::error::Error);

impl Deref for TungsteniteError {
    type Target = TError;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<TungsteniteError> for ConnectionError {
    fn from(e: TungsteniteError) -> Self {
        match e.0 {
            TError::ConnectionClosed => {
                ConnectionError::Closed(CloseError::new(CloseErrorKind::Normal, None))
            }
            TError::AlreadyClosed => {
                ConnectionError::Closed(CloseError::new(CloseErrorKind::AlreadyClosed, None))
            }
            TError::Io(e) => {
                ConnectionError::Io(IoError::new(e.kind(), e.source().map(ToString::to_string)))
            }
            #[cfg(feature = "tls")]
            TError::Tls(e) => ConnectionError::Tls(e.into()),
            TError::Capacity(e) => ConnectionError::Capacity(CapacityError::new(
                CapacityErrorKind::Ambiguous,
                Some(e.to_string()),
            )),
            TError::Protocol(e) => ConnectionError::Protocol(ProtocolError::new(
                ProtocolErrorKind::WebSocket,
                Some(e.to_string()),
            )),
            TError::SendQueueFull(e) => {
                ConnectionError::Capacity(CapacityError::new(CapacityErrorKind::Full(e), None))
            }
            TError::Utf8 => {
                ConnectionError::Encoding(EncodingError::new(EncodingErrorKind::Invalid, None))
            }
            TError::Url(e) => ConnectionError::Http(HttpError::new(
                HttpErrorKind::InvalidUri(InvalidUriError::new(
                    InvalidUriErrorKind::Malformatted,
                    Some(e.to_string()),
                )),
                None,
            )),
            TError::Http(e) => ConnectionError::Http(HttpError::new(
                HttpErrorKind::StatusCode(Some(e.status())),
                None,
            )),
            TError::HttpFormat(e) => ConnectionError::Http(HttpError::new(
                HttpErrorKind::InvalidUri(InvalidUriError::new(
                    InvalidUriErrorKind::Malformatted,
                    Some(e.to_string()),
                )),
                None,
            )),
            TError::ExtensionError(_) => {
                // todo: remove once deflate PR has bene merged
                unreachable!()
            }
        }
    }
}

impl From<FutSendError> for ConnectionError {
    fn from(e: FutSendError) -> Self {
        if e.is_disconnected() {
            ConnectionError::Closed(CloseError::new(CloseErrorKind::Unexpected, None))
        } else if e.is_full() {
            ConnectionError::Capacity(CapacityError::new(CapacityErrorKind::WriteFull, None))
        } else {
            // There are only two variants and no kind function to pattern match on
            unreachable!()
        }
    }
}

pub(crate) fn format_cause(cause: &Option<String>) -> String {
    match cause {
        Some(c) => format!(" {}", c),
        None => String::new(),
    }
}

/// Ways in which the router can fail to provide a route.
#[derive(Debug, PartialEq, Eq)]
pub enum RouterError {
    /// For a local endpoint it can be determined that no agent exists.
    NoAgentAtRoute(RelativeUri),
    /// Connecting to a remote endpoint failed (the endpoint may or may not exist).
    ConnectionFailure(ConnectionError),
    /// The router was dropped (the application is likely stopping).
    RouterDropped,
}

impl Display for RouterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RouterError::NoAgentAtRoute(route) => write!(f, "No agent at: '{}'", route),
            RouterError::ConnectionFailure(err) => {
                write!(f, "Failed to route to requested endpoint: '{}'", err)
            }
            RouterError::RouterDropped => write!(f, "The router channel was dropped."),
        }
    }
}

impl Error for RouterError {}

impl Recoverable for RouterError {
    fn is_fatal(&self) -> bool {
        match self {
            RouterError::ConnectionFailure(err) => err.is_fatal(),
            _ => true,
        }
    }
}

/// Error indicating that a routing address is invalid. (Typically, this should not occur and
/// suggests a bug).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unresolvable(pub RoutingAddr);

impl Display for Unresolvable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Unresolvable(addr) = self;
        write!(f, "No active endpoint with ID: {}", addr)
    }
}

impl Error for Unresolvable {}

/// Error indicating that request to route to a plane-local agent failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoAgentAtRoute(pub RelativeUri);

impl Display for NoAgentAtRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let NoAgentAtRoute(route) = self;
        write!(f, "No agent at route: '{}'", route)
    }
}

impl Error for NoAgentAtRoute {}
