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

use crate::routing::RoutingAddr;
pub use capacity::*;
pub use closed::*;
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

pub use self::http::*;

mod capacity;
mod closed;
mod http;
mod io;
mod protocol;
mod resolution;
mod routing;

#[cfg(test)]
mod tests;

pub type FmtResult = std::fmt::Result;

type BoxRecoverableError = Box<dyn RecoverableError>;

pub trait RecoverableError: std::error::Error + Send + Sync + Recoverable + 'static {}
impl<T> RecoverableError for T where T: std::error::Error + Send + Sync + Recoverable + 'static {}

impl From<RoutingError> for RequestError {
    fn from(_: RoutingError) -> Self {
        RequestError {}
    }
}

impl<T> From<circular_buffer::error::SendError<T>> for RoutingError {
    fn from(_: circular_buffer::error::SendError<T>) -> Self {
        RoutingError::Dropped
    }
}

impl<T> From<swim_utilities::future::item_sink::SendError<T>> for RoutingError {
    fn from(_: SendError<T>) -> Self {
        RoutingError::Dropped
    }
}

/// An error denoting that a connection error has occurred.
#[derive(Debug, Clone)]
pub enum ConnectionError {
    /// A HTTP detailing either a malformatted request/response or a peer error.
    Http(HttpError),
    /// An error detailing that there has been a read/write buffer overflow.
    Capacity(CapacityError),
    /// A connection protocol error.
    Protocol(ProtocolError),
    /// An error produced when closing a connection or a normal close code.
    Closed(CloseError),
    /// An IO error produced during a read/write operation.
    Io(IoError),
    /// An error produced when attempting to resolve a peer.
    Resolution(ResolutionError),
    /// An error was produced at the transport layer.
    Transport(Arc<BoxRecoverableError>),
    /// The router was dropped
    RouterDropped,
}

impl From<ResolutionError> for ConnectionError {
    fn from(e: ResolutionError) -> Self {
        ConnectionError::Resolution(e)
    }
}

impl From<RoutingError> for ConnectionError {
    fn from(e: RoutingError) -> Self {
        match e {
            RoutingError::Connection(e) => e,
            RoutingError::Dropped => ConnectionError::RouterDropped,
            RoutingError::Resolution(e) => ConnectionError::Resolution(e),
        }
    }
}

impl PartialEq for ConnectionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ConnectionError::Http(l), ConnectionError::Http(r)) => l.eq(r),
            (ConnectionError::Capacity(l), ConnectionError::Capacity(r)) => l.eq(r),
            (ConnectionError::Protocol(l), ConnectionError::Protocol(r)) => l.eq(r),
            (ConnectionError::Closed(l), ConnectionError::Closed(r)) => l.eq(r),
            (ConnectionError::Io(l), ConnectionError::Io(r)) => l.eq(r),
            (ConnectionError::Resolution(l), ConnectionError::Resolution(r)) => l.eq(r),
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
            ConnectionError::Capacity(e) => e.is_fatal(),
            ConnectionError::Protocol(e) => e.is_fatal(),
            ConnectionError::Closed(e) => e.is_fatal(),
            ConnectionError::Io(e) => matches!(
                e.kind(),
                ErrorKind::Interrupted | ErrorKind::TimedOut | ErrorKind::ConnectionReset
            ),
            ConnectionError::Resolution(e) => e.is_fatal(),
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
            ConnectionError::Capacity(e) => write!(f, "{}", e),
            ConnectionError::Protocol(e) => write!(f, "{}", e),
            ConnectionError::Closed(e) => write!(f, "{}", e),
            ConnectionError::Io(e) => write!(f, "{}", e),
            ConnectionError::Resolution(e) => write!(f, "{}", e),
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

pub(crate) fn format_cause(cause: &Option<String>) -> String {
    match cause {
        Some(c) => format!(" {}", c),
        None => String::new(),
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
