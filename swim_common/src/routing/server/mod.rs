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

use crate::routing::ws::WebSocketError;
use crate::routing::RoutingError;
use crate::sink::SinkSendError;
use crate::warp::envelope::Envelope;
use http::StatusCode;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::io::ErrorKind;
use tokio::sync::mpsc;
use utilities::errors::Recoverable;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

/// Error type for the [`crate::routing::ServerRouter`] that will return the envelope in the event that
/// routing it fails.
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

/// Ways in which the router can fail to provide a route.
#[derive(Debug, PartialEq, Eq)]
pub enum RouterError {
    /// For a local endpoint it can be determined that no agent exists.
    NoAgentAtRoute(RelativeUri),
    /// Connecting to a remote endpoint failed (the endpoint may or may not exist).
    ConnectionFailure(ServerConnectionError),
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

fn io_fatal(kind: &ErrorKind) -> bool {
    !matches!(
        kind,
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::TimedOut
    )
}

/// A connection to a remote endpoint failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerConnectionError {
    /// The host could not be resolved.
    Resolution,
    /// An error occurred at the socket level.
    Socket(io::ErrorKind),
    /// An error occurred at the web socket protocol level.
    Websocket(WebSocketError),
    /// The remote host closed the connection.
    ClosedRemotely,
    /// An error occurred at the Warp protocol level.
    Warp(String),
    /// The connection was closed locally.
    Closed,
    /// The connection failed with the following status code.
    Http(StatusCode),
}

impl Display for ServerConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerConnectionError::Resolution => {
                write!(f, "The specified host could not be resolved.")
            }
            ServerConnectionError::Socket(err) => write!(f, "IO error: '{:?}'", err),
            ServerConnectionError::Websocket(err) => write!(f, "Web socket error: '{}'", err),
            ServerConnectionError::ClosedRemotely => {
                write!(f, "The connection was closed remotely.")
            }
            ServerConnectionError::Warp(err) => write!(f, "Warp protocol error: '{}'", err),
            ServerConnectionError::Closed => write!(f, "The connection has been closed."),
            ServerConnectionError::Http(code) => write!(f, "The connection failed with: {}", code),
        }
    }
}

impl Error for ServerConnectionError {}

impl Recoverable for ServerConnectionError {
    fn is_fatal(&self) -> bool {
        match self {
            ServerConnectionError::Socket(err) => io_fatal(err),
            ServerConnectionError::Warp(_) => false,
            _ => true,
        }
    }
}

impl From<io::Error> for ServerConnectionError {
    fn from(err: io::Error) -> Self {
        ServerConnectionError::Socket(err.kind())
    }
}

impl<T> From<SinkSendError<T>> for ServerConnectionError {
    fn from(_: SinkSendError<T>) -> Self {
        ServerConnectionError::ClosedRemotely
    }
}

impl From<futures::channel::mpsc::SendError> for ServerConnectionError {
    fn from(_: futures::channel::mpsc::SendError) -> Self {
        ServerConnectionError::ClosedRemotely
    }
}

/// General error type for a failed agent resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionError {
    Unresolvable(Unresolvable),
    RouterDropped,
}

impl Display for ResolutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolutionError::Unresolvable(Unresolvable(id)) => {
                write!(f, "Address {} could not be resolved.", id)
            }
            ResolutionError::RouterDropped => write!(f, "The router channel was dropped."),
        }
    }
}

impl Error for ResolutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ResolutionError::Unresolvable(err) => Some(err),
            ResolutionError::RouterDropped => None,
        }
    }
}

/// Error indicating that a routing address is invalid. (Typically, this should not occur and
/// suggests a bug).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unresolvable(pub String);

impl Display for Unresolvable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Unresolvable(addr) = self;
        write!(f, "No active endpoint with ID: {}", addr)
    }
}

impl Error for Unresolvable {}
