// Copyright 2015-2021 SWIM.AI inc.
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

mod capacity;
mod closed;
mod encoding;
mod http;
mod io;
mod protocol;
mod resolution;
mod routing;
#[cfg(feature = "tls")]
mod tls;

pub use self::http::*;
pub use capacity::*;
pub use closed::*;
pub use encoding::*;
pub use io::*;
pub use protocol::*;
pub use resolution::*;
pub use routing::*;
#[cfg(feature = "tls")]
pub use tls::*;

use futures::channel::mpsc::SendError as FutSendError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use swim_utilities::errors::Recoverable;

pub type FmtResult = std::fmt::Result;

use std::time::Duration;

#[cfg(test)]
mod tests;

/// An error denoting that a connection error has occurred.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ConnectionError {
    /// A HTTP detailing either a malformatted request/response or a peer error.
    Http(HttpError),
    /// A TLS error that may be produced when reading a certificate or through a connection.
    #[cfg(feature = "tls")]
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
    Resolution(ResolutionError),
    /// A pending write did not complete within the specified duration.
    WriteTimeout(Duration),
}

impl From<ratchet::Error> for ConnectionError {
    fn from(_: ratchet::Error) -> Self {
        todo!()
    }
}

impl Recoverable for ConnectionError {
    fn is_fatal(&self) -> bool {
        match self {
            ConnectionError::Http(e) => e.is_fatal(),
            #[cfg(feature = "tls")]
            ConnectionError::Tls(e) => e.is_fatal(),
            ConnectionError::Capacity(e) => e.is_fatal(),
            ConnectionError::Protocol(e) => e.is_fatal(),
            ConnectionError::Closed(e) => e.is_fatal(),
            ConnectionError::Io(e) => matches!(
                e.kind(),
                ErrorKind::Interrupted | ErrorKind::TimedOut | ErrorKind::ConnectionReset
            ),
            ConnectionError::Encoding(e) => e.is_fatal(),
            ConnectionError::Resolution(e) => e.is_fatal(),
            ConnectionError::WriteTimeout(_) => false,
        }
    }
}

impl Error for ConnectionError {}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ConnectionError::Http(e) => write!(f, "{}", e),
            #[cfg(feature = "tls")]
            ConnectionError::Tls(e) => write!(f, "{}", e),
            ConnectionError::Capacity(e) => write!(f, "{}", e),
            ConnectionError::Protocol(e) => write!(f, "{}", e),
            ConnectionError::Closed(e) => write!(f, "{}", e),
            ConnectionError::Io(e) => write!(f, "{}", e),
            ConnectionError::Encoding(e) => write!(f, "{}", e),
            ConnectionError::Resolution(e) => write!(f, "{}", e),
            ConnectionError::WriteTimeout(dur) => write!(
                f,
                "Writing to the connection failed to complete within {:?}.",
                dur
            ),
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

pub(crate) fn format_cause(cause: &Option<String>) -> String {
    match cause {
        Some(c) => format!(" {}", c),
        None => String::new(),
    }
}
