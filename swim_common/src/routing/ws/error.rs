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

use std::fmt;
use std::fmt::{Display, Formatter};

const UNSUPPORTED_SCHEME: &str = "Unsupported URL scheme";
const MISSING_SCHEME: &str = "Missing URL scheme";

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
    /// A UTF-8 encoding error
    Utf8,
    /// A capacity was exhausted.
    Capacity,
    /// A WebSocket extension error.
    Extension(String),
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
        match self {
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
            WebSocketError::Utf8 => write!(f, "UTF-8 encoding error"),
            WebSocketError::Capacity => write!(f, "WebSocket buffer capacity exhausted"),
            WebSocketError::Extension(e) => write!(f, "An extension error occured: {}", e),
        }
    }
}
