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

use crate::model::text::Text;
use crate::request::request_future::RequestError;
use crate::ws::error::ConnectionError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use tokio::sync::mpsc::error::SendError;

// An error returned by the router
#[derive(Clone, Debug, PartialEq)]
pub enum RoutingError {
    // The connection to the remote host has been lost.
    ConnectionError,
    // The remote host is unreachable.
    HostUnreachable,
    // The connection pool has encountered an error.
    PoolError(ConnectionError),
    // The router has been stopped.
    RouterDropped,
    // The router has encountered an error while stopping.
    CloseError,
}

impl RoutingError {
    /// Returns whether or not the router can recover from the error.
    /// Inverse of [`is_fatal`].
    pub fn is_transient(&self) -> bool {
        match &self {
            RoutingError::ConnectionError => true,
            RoutingError::HostUnreachable => true,
            RoutingError::PoolError(ConnectionError::ConnectionRefused) => true,
            _ => false,
        }
    }

    /// Returns whether or not the error is unrecoverable.
    /// Inverse of [`is_transient`].
    pub fn is_fatal(&self) -> bool {
        !self.is_transient()
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

impl<T> From<SendError<T>> for RoutingError {
    fn from(_: SendError<T>) -> Self {
        RoutingError::RouterDropped
    }
}

impl From<RoutingError> for RequestError {
    fn from(_: RoutingError) -> Self {
        RequestError {}
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum LaneIdentifier {
    Agent(String),
    Meta(String),
}

impl ToString for LaneIdentifier {
    fn to_string(&self) -> String {
        match self {
            LaneIdentifier::Agent(s) => s.clone(),
            LaneIdentifier::Meta(s) => s.clone(),
        }
    }
}

impl From<LaneIdentifier> for Text {
    fn from(identifier: LaneIdentifier) -> Self {
        let inner = match identifier {
            LaneIdentifier::Agent(s) => s,
            LaneIdentifier::Meta(s) => s,
        };

        From::from(inner)
    }
}
