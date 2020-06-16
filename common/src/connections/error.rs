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

use crate::request::request_future::RequestError;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use utilities::rt::task::TaskError;

/// Connection error types returned by the connection pool and the connections.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ConnectionErrorKind {
    /// Error that occurred when connecting to a remote host.
    ConnectError,
    /// A WebSocket error.  
    SocketError,
    /// Error that occurred when sending messages.
    SendMessageError,
    /// Error that occurred when receiving messages.
    ReceiveMessageError,
    /// Error that occurred when closing down connections.
    ClosedError,
}

impl Display for ConnectionErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            ConnectionErrorKind::ConnectError => {
                write!(f, "An error was produced during a connection.")
            }
            ConnectionErrorKind::SocketError => {
                write!(f, "An error was produced by the web socket.")
            }
            ConnectionErrorKind::SendMessageError => {
                write!(f, "An error occured when sending a message.")
            }
            ConnectionErrorKind::ReceiveMessageError => {
                write!(f, "An error occured when receiving a message.")
            }
            ConnectionErrorKind::ClosedError => {
                write!(f, "An error occured when closing down connections.")
            }
        }
    }
}

type Cause = Box<dyn Error + Send + Sync>;

#[derive(Debug)]
pub struct ConnectionError {
    kind: ConnectionErrorKind,
    cause: Option<Cause>,
}

impl Clone for ConnectionError {
    fn clone(&self) -> Self {
        ConnectionError {
            kind: self.kind.clone(),
            // todo. Tungstenite errors don't implement clone
            cause: None,
        }
    }
}

impl PartialEq for ConnectionError {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl From<RequestError> for ConnectionError {
    fn from(_: RequestError) -> Self {
        ConnectionError {
            kind: ConnectionErrorKind::ConnectError,
            cause: None,
        }
    }
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.cause {
            Some(e) => match self.kind.fmt(f) {
                Ok(_) => write!(f, " cause: {}", e),
                e => e,
            },
            None => self.kind.fmt(f),
        }
    }
}

impl ConnectionError {
    pub fn new(kind: ConnectionErrorKind) -> Self {
        ConnectionError { kind, cause: None }
    }

    pub fn with_cause(kind: ConnectionErrorKind, cause: Cause) -> Self {
        ConnectionError {
            kind,
            cause: Some(cause),
        }
    }

    pub fn kind(&self) -> ConnectionErrorKind {
        self.kind
    }

    pub fn into_kind(self) -> ConnectionErrorKind {
        self.kind
    }

    pub fn cause(&mut self) -> Option<Cause> {
        self.cause.take()
    }

    /// Returns whether or not the error kind is deemed to be transient.
    pub fn is_transient(&self) -> bool {
        match &self.kind() {
            ConnectionErrorKind::SocketError => false,
            _ => true,
        }
    }
}

impl From<ConnectionErrorKind> for ConnectionError {
    fn from(kind: ConnectionErrorKind) -> Self {
        ConnectionError { kind, cause: None }
    }
}

impl From<TaskError> for ConnectionError {
    fn from(_: TaskError) -> Self {
        ConnectionError {
            kind: ConnectionErrorKind::ConnectError,
            cause: None,
        }
    }
}
