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
use std::fmt;
use std::fmt::{Display, Formatter};
use swim_runtime::task::TaskError;

/// Connection error types returned by the connection pool and the connections.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ConnectionError {
    /// Error that occurred when connecting to a remote host.
    ConnectError,
    /// A WebSocket error.  
    SocketError(WebSocketError),
    /// Error that occurred when sending messages.
    SendMessageError,
    /// Error that occurred when receiving messages.
    ReceiveMessageError,
    /// Error that occurred when closing down connections.
    AlreadyClosedError,

    /// Not an error. Closed event by the WebSocket
    Closed,
}

/// An error that occurred within the underlying WebSocket.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WebSocketError {
    /// An invalid URL was supplied.
    Url(String),
    /// A protocol error occured.
    Protocol,
}

impl Display for WebSocketError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            WebSocketError::Url(url) => write!(f, "An invalid URL ({}) was supplied", url),
            WebSocketError::Protocol => write!(f, "A protocol error occured."),
        }
    }
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            ConnectionError::ConnectError => {
                write!(f, "An error was produced during a connection.")
            }
            ConnectionError::SocketError(wse) => {
                write!(f, "An error was produced by the web socket: {}", wse)
            }
            ConnectionError::SendMessageError => {
                write!(f, "An error occured when sending a message.")
            }
            ConnectionError::ReceiveMessageError => {
                write!(f, "An error occured when receiving a message.")
            }
            ConnectionError::AlreadyClosedError => {
                write!(f, "An error occured when closing down connections.")
            }
            ConnectionError::Closed => write!(f, "The WebSocket closed successfully."),
        }
    }
}

impl From<RequestError> for ConnectionError {
    fn from(_: RequestError) -> Self {
        ConnectionError::ConnectError
    }
}

impl ConnectionError {
    /// Returns whether or not the error kind is deemed to be transient.
    pub fn is_transient(&self) -> bool {
        match &self {
            ConnectionError::SocketError(_) => false,
            _ => true,
        }
    }
}

impl From<TaskError> for ConnectionError {
    fn from(_: TaskError) -> Self {
        ConnectionError::ConnectError
    }
}
