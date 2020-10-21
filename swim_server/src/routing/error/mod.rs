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

use crate::routing::{RoutingAddr, TaggedEnvelope};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use swim_common::routing::RoutingError;
use swim_common::warp::envelope::Envelope;
use swim_common::ws::error::WebSocketError;
use tokio::sync::mpsc;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

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

impl From<mpsc::error::SendError<TaggedEnvelope>> for SendError {
    fn from(err: mpsc::error::SendError<TaggedEnvelope>) -> Self {
        let mpsc::error::SendError(TaggedEnvelope(_, envelope)) = err;
        SendError {
            error: RoutingError::RouterDropped,
            envelope,
        }
    }
}

#[derive(Debug)]
pub enum RouterError {
    NoAgentAtRoute(RelativeUri),
    ConnectionFailure(ConnectionError),
    RouterDropped,
}

impl RouterError {
    pub fn is_fatal(&self) -> bool {
        //TODO Implement this.
        true
    }
}

#[derive(Debug, Clone)]
pub enum ConnectionError {
    Resolution,
    Socket(io::ErrorKind),
    Websocket(WebSocketError),
    Warp(String),
}

impl ConnectionError {
    pub fn is_transient(&self) -> bool {
        //TODO Implement this.
        false
    }
}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::Socket(err.kind())
    }
}

/// General error type for a failed agent resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionError {
    Unresolvable(Unresolvable),
    RouterDropped,
}
/*
impl Display for ResolutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolutionError::NoAgent(err) => err.fmt(f),
            ResolutionError::NoRoute(err) => err.fmt(f),
        }
    }
}

impl Error for ResolutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ResolutionError::NoAgent(err) => Some(err),
            ResolutionError::NoRoute(err) => Some(err),
        }
    }
}*/

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
