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

use futures::channel::mpsc::SendError as FutSendError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::SendError as MpscSendError;

use thiserror::Error;

use crate::routing::RoutingAddr;
pub use capacity::*;
pub use closed::*;
pub use encoding::*;
pub use io::*;
pub use protocol::*;
pub use resolution::*;
pub use routing::*;
use swim_utilities::errors::Recoverable;
use swim_utilities::future::item_sink::SendError;
use swim_utilities::future::request::request_future::RequestError;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::sync::circular_buffer;
use thiserror::Error as ThisError;
pub use tls::*;

pub use self::http::*;

mod capacity;
mod closed;
mod encoding;
mod http;
mod io;
mod protocol;
mod resolution;
mod routing;
mod tls;

#[cfg(test)]
mod tests;

pub type FmtResult = std::fmt::Result;

type BoxRecoverableError = Box<dyn RecoverableError>;

pub trait RecoverableError: std::error::Error + Send + Sync + Recoverable + 'static {}
impl<T> RecoverableError for T where T: std::error::Error + Send + Sync + Recoverable + 'static {}

#[derive(Error, Debug, PartialEq)]
pub enum RoutingError {
    #[error("Failed to resolve the address: `{0}`")]
    Unresolvable(String),
    #[error("`{0}`")]
    Connection(#[from] ConnectionError),
    #[error("Router dropped")]
    RouterDropped,
}

impl Recoverable for RoutingError {
    fn is_fatal(&self) -> bool {
        match self {
            RoutingError::Unresolvable(_) => true,
            RoutingError::Connection(e) => e.is_fatal(),
            RoutingError::RouterDropped => true,
        }
    }
}

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

impl<T> From<swim_utilities::future::item_sink::SendError<T>> for RoutingError {
    fn from(_: SendError<T>) -> Self {
        RoutingError::RouterDropped
    }
}

/// An error denoting that a connection error has occurred.
#[derive(Debug, Clone)]
pub enum ConnectionError {
    /// A HTTP detailing either a malformatted request/response or a peer error.
    Http(HttpError),
    /// A TLS error that may be produced when reading a certificate or through a connection.
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
    Unresolvable(String),
    /// A pending write did not complete within the specified duration.
    WriteTimeout(Duration),
    /// An error was produced at the transport layer.
    Transport(Arc<BoxRecoverableError>),
    /// The router was dropped
    RouterDropped,
}

impl From<RoutingError> for ConnectionError {
    fn from(e: RoutingError) -> Self {
        match e {
            RoutingError::Unresolvable(e) => ConnectionError::Unresolvable(e),
            RoutingError::Connection(e) => e,
            RoutingError::RouterDropped => ConnectionError::RouterDropped,
        }
    }
}

impl PartialEq for ConnectionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ConnectionError::Http(l), ConnectionError::Http(r)) => l.eq(r),
            #[cfg(feature = "tls")]
            (ConnectionError::Tls(l), ConnectionError::Tls(r)) => l.eq(r),
            (ConnectionError::Capacity(l), ConnectionError::Capacity(r)) => l.eq(r),
            (ConnectionError::Protocol(l), ConnectionError::Protocol(r)) => l.eq(r),
            (ConnectionError::Closed(l), ConnectionError::Closed(r)) => l.eq(r),
            (ConnectionError::Io(l), ConnectionError::Io(r)) => l.eq(r),
            (ConnectionError::Encoding(l), ConnectionError::Encoding(r)) => l.eq(r),
            (ConnectionError::Unresolvable(l), ConnectionError::Unresolvable(r)) => l.eq(r),
            (ConnectionError::WriteTimeout(l), ConnectionError::WriteTimeout(r)) => l.eq(r),
            (ConnectionError::Transport(l), ConnectionError::Transport(r)) => {
                l.to_string().eq(&r.to_string())
            }
            _ => false,
        }
    }
}

#[derive(Debug, ThisError)]
#[error("{0}")]
struct RatchetError(ratchet::Error);
impl Recoverable for RatchetError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<ratchet::Error> for ConnectionError {
    fn from(e: ratchet::Error) -> Self {
        ConnectionError::Transport(Arc::new(Box::new(RatchetError(e))))
    }
}

impl Recoverable for ConnectionError {
    fn is_fatal(&self) -> bool {
        match self {
            ConnectionError::Http(e) => e.is_fatal(),
            ConnectionError::Tls(e) => e.is_fatal(),
            ConnectionError::Capacity(e) => e.is_fatal(),
            ConnectionError::Protocol(e) => e.is_fatal(),
            ConnectionError::Closed(e) => e.is_fatal(),
            ConnectionError::Io(e) => matches!(
                e.kind(),
                ErrorKind::Interrupted | ErrorKind::TimedOut | ErrorKind::ConnectionReset
            ),
            ConnectionError::Encoding(e) => e.is_fatal(),
            ConnectionError::Unresolvable(_) => true,
            ConnectionError::WriteTimeout(_) => false,
            ConnectionError::Transport(e) => e.is_fatal(),
            ConnectionError::RouterDropped => true,
        }
    }
}

impl Error for ConnectionError {}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ConnectionError::Http(e) => write!(f, "{}", e),
            ConnectionError::Tls(e) => write!(f, "{}", e),
            ConnectionError::Capacity(e) => write!(f, "{}", e),
            ConnectionError::Protocol(e) => write!(f, "{}", e),
            ConnectionError::Closed(e) => write!(f, "{}", e),
            ConnectionError::Io(e) => write!(f, "{}", e),
            ConnectionError::Encoding(e) => write!(f, "{}", e),
            ConnectionError::Unresolvable(e) => write!(f, "Address {} could not be resolved.", e),
            ConnectionError::WriteTimeout(dur) => write!(
                f,
                "Writing to the connection failed to complete within {:?}.",
                dur
            ),
            ConnectionError::Transport(e) => {
                write!(f, "{}", e)
            }
            ConnectionError::RouterDropped => {
                write!(f, "Router dropped")
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

impl From<ConnectionDropped> for ConnectionError {
    fn from(_: ConnectionDropped) -> Self {
        ConnectionError::Closed(CloseError::closed())
    }
}

impl From<RouterError> for ConnectionError {
    fn from(err: RouterError) -> Self {
        match err {
            RouterError::NoAgentAtRoute(uri) => ConnectionError::Unresolvable(uri.to_string()),
            RouterError::ConnectionFailure(err) => err,
            RouterError::RouterDropped => ConnectionError::RouterDropped,
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
#[derive(Debug, PartialEq)]
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
