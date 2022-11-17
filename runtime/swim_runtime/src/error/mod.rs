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

use bytes::Bytes;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::Utf8Error;
use std::sync::Arc;
use std::time::Duration;
use swim_utilities::routing::route_uri::RouteUri;
use thiserror::Error;

use swim_utilities::errors::Recoverable;
use thiserror::Error as ThisError;
pub use tls::*;

mod tls;

#[cfg(test)]
mod tests;

pub type FmtResult = std::fmt::Result;

type BoxRecoverableError = Box<dyn RecoverableError>;

pub trait RecoverableError: std::error::Error + Send + Sync + Recoverable + 'static {}
impl<T> RecoverableError for T where T: std::error::Error + Send + Sync + Recoverable + 'static {}

/// An error denoting that a connection error has occurred.
#[derive(Debug, Clone)]
pub enum ConnectionError {
    /// An error produced when attempting to resolve a peer.
    Resolution(String),
    /// A pending write did not complete within the specified duration.
    WriteTimeout(Duration),
    /// An error was produced at the transport layer.
    Transport(Arc<BoxRecoverableError>),
}

impl PartialEq for ConnectionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ConnectionError::Resolution(l), ConnectionError::Resolution(r)) => l.eq(r),
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
            ConnectionError::Resolution(_) => false,
            ConnectionError::WriteTimeout(_) => false,
            ConnectionError::Transport(e) => e.is_fatal(),
        }
    }
}

impl Error for ConnectionError {}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ConnectionError::Resolution(e) => write!(f, "Address {} could not be resolved.", e),
            ConnectionError::WriteTimeout(dur) => write!(
                f,
                "Writing to the connection failed to complete within {:?}.",
                dur
            ),
            ConnectionError::Transport(e) => {
                write!(f, "{}", e)
            }
        }
    }
}

pub(crate) fn format_cause(cause: &Option<String>) -> String {
    match cause {
        Some(c) => format!(" {}", c),
        None => String::new(),
    }
}

/// Error indicating that request to route to a plane-local agent failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoAgentAtRoute(pub RouteUri);

impl Display for NoAgentAtRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let NoAgentAtRoute(route) = self;
        write!(f, "No agent at route: '{}'", route)
    }
}

impl Error for NoAgentAtRoute {}

/// Error indicating that the key for a map message contained invalid UTF8.
#[derive(Debug, Error)]
pub struct InvalidKey {
    key_bytes: Bytes,
    source: Utf8Error,
}

impl Display for InvalidKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The key {:?}, contains invalid UTF8: {}.",
            self.key_bytes, self.source
        )
    }
}

impl InvalidKey {
    pub fn new(key_bytes: Bytes, source: Utf8Error) -> Self {
        InvalidKey { key_bytes, source }
    }
}
