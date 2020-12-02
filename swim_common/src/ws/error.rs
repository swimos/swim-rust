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
use futures::io::Error;
use http::uri::InvalidUri;
use std::fmt;
use std::fmt::{Display, Formatter};
use swim_runtime::task::TaskError;
use tokio::sync::{mpsc, oneshot};

const UNSUPPORTED_SCHEME: &str = "Unsupported URL scheme";
const MISSING_SCHEME: &str = "Missing URL scheme";

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
    /// The connection was refused by the host.
    ConnectionRefused,
    /// Not an error. Closed event by the WebSocket
    Closed,
}

/// An error that occurred within the underlying WebSocket.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WebSocketError {
    /// The WebSocket was configured incorrectly. Detailed in the String field.
    BadConfiguration(String),
    /// An invalid URL was supplied.
    Url(String),
    /// A protocol error occurred.
    Protocol,
    /// A custom mage detailing the error.
    Message(String),
    /// A TLS error from the underlying implementation.
    #[cfg(feature = "tls")]
    Tls(String),
    /// An error from building or reading a certificate
    #[cfg(feature = "tls")]
    CertificateError(CertificateError),
}

#[cfg(feature = "tls")]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CertificateError {
    /// An IO error occurred while attempting to read the certificate.
    Io(String),
    /// An error occurred while trying to deserialize the certificate.
    SSL(String),
}

#[cfg(feature = "tls")]
impl Display for CertificateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CertificateError::Io(cause) => write!(f, "{}", cause),
            CertificateError::SSL(cause) => write!(f, "{}", cause),
        }
    }
}

#[cfg(feature = "tls")]
impl From<native_tls::Error> for WebSocketError {
    fn from(e: native_tls::Error) -> Self {
        WebSocketError::Tls(e.to_string())
    }
}

#[cfg(feature = "tls")]
impl From<std::io::Error> for CertificateError {
    fn from(e: std::io::Error) -> Self {
        CertificateError::Io(e.to_string())
    }
}

#[cfg(feature = "tls")]
impl From<CertificateError> for WebSocketError {
    fn from(e: CertificateError) -> Self {
        WebSocketError::CertificateError(e)
    }
}

impl WebSocketError {
    /// Creates a new `WebSocketError::Url` error detailing that the `found` scheme is unsupported.
    pub fn unsupported_scheme<I>(scheme: I) -> WebSocketError
    where
        I: Into<String>,
    {
        WebSocketError::Url(format!("{}: {}", UNSUPPORTED_SCHEME, scheme.into()))
    }

    /// Creates a new `WebSocketError::Url` error detailing that the URL scheme is missing.
    pub fn missing_scheme() -> WebSocketError {
        WebSocketError::Url(MISSING_SCHEME.into())
    }
}

impl Display for WebSocketError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            WebSocketError::Url(url) => write!(f, "An invalid URL ({}) was supplied", url),
            WebSocketError::Protocol => write!(f, "A protocol error occurred."),
            WebSocketError::Message(msg) => write!(f, "{}", msg),
            WebSocketError::BadConfiguration(reason) => {
                write!(f, "Incorrect WebSocket configuration: {}", reason)
            }
            #[cfg(feature = "tls")]
            WebSocketError::Tls(e) => write!(f, "{}", e),
            #[cfg(feature = "tls")]
            WebSocketError::CertificateError(e) => write!(
                f,
                "An error was produced while trying to build the certificate: {}",
                e
            ),
        }
    }
}

#[cfg(feature = "tls")]
impl From<CertificateError> for ConnectionError {
    fn from(e: CertificateError) -> Self {
        ConnectionError::SocketError(WebSocketError::CertificateError(e))
    }
}

impl From<std::io::Error> for ConnectionError {
    fn from(err: Error) -> Self {
        ConnectionError::SocketError(WebSocketError::Message(err.to_string()))
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
                write!(f, "An error occurred when sending a message.")
            }
            ConnectionError::ReceiveMessageError => {
                write!(f, "An error occurred when receiving a message.")
            }
            ConnectionError::AlreadyClosedError => {
                write!(f, "An error occurred when closing down connections.")
            }
            ConnectionError::Closed => write!(f, "The WebSocket closed successfully."),
            ConnectionError::ConnectionRefused => {
                write!(f, "The connection was refused by the host")
            }
        }
    }
}

impl From<WebSocketError> for ConnectionError {
    fn from(e: WebSocketError) -> Self {
        ConnectionError::SocketError(e)
    }
}

impl From<RequestError> for ConnectionError {
    fn from(_: RequestError) -> Self {
        ConnectionError::ConnectError
    }
}

impl<T> From<mpsc::error::SendError<T>> for ConnectionError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        ConnectionError::ConnectError
    }
}

impl From<oneshot::error::RecvError> for ConnectionError {
    fn from(_: oneshot::error::RecvError) -> Self {
        ConnectionError::ConnectError
    }
}

impl ConnectionError {
    /// Returns whether or not the error kind is deemed to be transient.
    pub fn is_transient(&self) -> bool {
        match &self {
            ConnectionError::SocketError(_) => false,
            ConnectionError::ConnectionRefused => true,
            _ => true,
        }
    }
}

impl From<TaskError> for ConnectionError {
    fn from(_: TaskError) -> Self {
        ConnectionError::ConnectError
    }
}

impl From<InvalidUri> for WebSocketError {
    fn from(e: InvalidUri) -> Self {
        WebSocketError::Url(e.to_string())
    }
}

impl From<http::Error> for ConnectionError {
    fn from(e: http::Error) -> Self {
        ConnectionError::SocketError(WebSocketError::Url(e.to_string()))
    }
}
