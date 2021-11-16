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

use crate::protocol::{CloseCodeParseErr, OpCodeParseErr};
use http::header::{HeaderName, InvalidHeaderValue};
use http::status::InvalidStatusCode;
use http::uri::InvalidUri;
use http::StatusCode;
use std::any::Any;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::io;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use thiserror::Error;

pub(crate) type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// The errors that may occur during a WebSocket connection.
#[derive(Debug)]
pub struct Error {
    inner: Inner,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner.source.as_deref().map(|e| e as &dyn StdError)
    }
}

impl Error {
    /// Construct a new error with the provided kind and no cause.
    pub fn new(kind: ErrorKind) -> Error {
        Error {
            inner: Inner { kind, source: None },
        }
    }

    /// Construct a new error with the provided kind and a cause.
    pub fn with_cause<E>(kind: ErrorKind, source: E) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            inner: Inner {
                kind,
                source: Some(source.into()),
            },
        }
    }

    /// Returns some reference to the boxed value if it is of type T, or None if it isn’t.
    pub fn downcast_ref<T: Any + StdError>(&self) -> Option<&T> {
        match &self.inner.source {
            Some(source) => source.downcast_ref(),
            None => None,
        }
    }

    /// Whether this error is related to an IO error.
    pub fn is_io(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::IO)
    }

    /// Whether this error is related to an HTTP error.
    pub fn is_http(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Http)
    }

    /// Whether this error is related to an extension error.
    pub fn is_extension(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Extension)
    }

    /// Whether this error is related to a protocol error.
    pub fn is_protocol(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Protocol)
    }

    /// Whether this error is related to an encoding error.
    pub fn is_encoding(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Encoding)
    }

    /// Whether this error is related to a close error.
    pub fn is_close(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Close)
    }
}

#[derive(Debug)]
struct Inner {
    kind: ErrorKind,
    source: Option<BoxError>,
}

/// A type of error represented.
#[derive(Copy, Clone, Debug)]
pub enum ErrorKind {
    /// An IO error.
    IO,
    /// An HTTP error.
    Http,
    /// An extension error.
    Extension,
    /// A protocol error.
    Protocol,
    /// An encoding error.
    Encoding,
    /// A close error.
    Close,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::with_cause(ErrorKind::IO, e)
    }
}

impl From<httparse::Error> for Error {
    fn from(e: httparse::Error) -> Self {
        Error::with_cause(ErrorKind::Http, e)
    }
}

impl From<InvalidStatusCode> for Error {
    fn from(e: InvalidStatusCode) -> Self {
        Error::with_cause(ErrorKind::Http, e)
    }
}

/// HTTP errors.
#[derive(Error, Debug, PartialEq)]
pub enum HttpError {
    /// An invalid HTTP method was received.
    #[error("Invalid HTTP method: `{0:?}`")]
    HttpMethod(Option<String>),
    /// The server responded with a redirect.
    #[error("Redirected: `{0}`")]
    Redirected(String),
    /// The peer returned with a status code other than 101.
    #[error("Status code: `{0}`")]
    Status(StatusCode),
    /// An invalid HTTP version was received in a request.
    #[error("Invalid HTTP version: `{0:?}`")]
    HttpVersion(Option<u8>),
    /// A request or response was missing an expected header.
    #[error("Missing header: `{0}`")]
    MissingHeader(HeaderName),
    /// A request or response contained an invalid header.
    #[error("Invalid header: `{0}`")]
    InvalidHeader(HeaderName),
    /// Sec-WebSocket-Key was invalid.
    #[error("Sec-WebSocket-Accept mismatch")]
    KeyMismatch,
    /// The provided URI was malformatted
    #[error("The provided URI was malformatted")]
    MalformattedUri(Option<String>),
    /// A provided header was malformatted
    #[error("A provided header was malformatted")]
    MalformattedHeader(String),
}

impl From<HttpError> for Error {
    fn from(e: HttpError) -> Self {
        Error::with_cause(ErrorKind::Http, e)
    }
}

/// An invalid header was received.
#[derive(Debug)]
pub struct InvalidHeader(pub String);

impl From<InvalidHeader> for HttpError {
    fn from(e: InvalidHeader) -> Self {
        HttpError::MalformattedHeader(e.0)
    }
}

impl From<InvalidHeader> for Error {
    fn from(e: InvalidHeader) -> Self {
        Error::with_cause::<HttpError>(ErrorKind::Http, e.into())
    }
}

impl From<InvalidUri> for Error {
    fn from(e: InvalidUri) -> Self {
        Error::with_cause(ErrorKind::Http, e)
    }
}

impl From<InvalidUri> for HttpError {
    fn from(e: InvalidUri) -> Self {
        HttpError::MalformattedUri(Some(format!("{:?}", e)))
    }
}

impl From<http::Error> for Error {
    fn from(e: http::Error) -> Self {
        Error::with_cause(ErrorKind::Http, e)
    }
}

impl From<ProtocolError> for Error {
    fn from(e: ProtocolError) -> Self {
        Error::with_cause(ErrorKind::Http, e)
    }
}

impl From<OpCodeParseErr> for ProtocolError {
    fn from(e: OpCodeParseErr) -> Self {
        ProtocolError::OpCode(e)
    }
}

impl From<OpCodeParseErr> for Error {
    fn from(e: OpCodeParseErr) -> Self {
        Error::with_cause(ErrorKind::Protocol, Box::new(ProtocolError::from(e)))
    }
}

impl From<Utf8Error> for Error {
    fn from(e: Utf8Error) -> Self {
        Error::with_cause(ErrorKind::Encoding, e)
    }
}

impl From<CloseCodeParseErr> for Error {
    fn from(e: CloseCodeParseErr) -> Self {
        Error::with_cause(ErrorKind::Protocol, e)
    }
}

impl From<InvalidHeaderValue> for Error {
    fn from(e: InvalidHeaderValue) -> Self {
        Error::with_cause(ErrorKind::Http, e)
    }
}

#[derive(Clone, Copy, Error, Debug, PartialEq)]
/// The channel is already closed
#[error("The channel is already closed")]
pub struct CloseError;

/// WebSocket protocol errors.
#[derive(Copy, Clone, Debug, PartialEq, Error)]
pub enum ProtocolError {
    /// Invalid encoding was received.
    #[error("Not valid UTF-8 encoding")]
    Encoding,
    /// A peer selected a protocol that was not sent.
    #[error("Received an unknown subprotocol")]
    UnknownProtocol,
    /// An invalid OpCode was received.
    #[error("Bad OpCode: `{0}`")]
    OpCode(OpCodeParseErr),
    /// The peer sent an unmasked frame when one was expected.
    #[error("Received an unexpected unmasked frame")]
    UnmaskedFrame,
    /// The peer sent an masked frame when one was not expected.
    #[error("Received an unexpected masked frame")]
    MaskedFrame,
    /// Received a fragmented control frame
    #[error("Received a fragmented control frame")]
    FragmentedControl,
    /// A received frame exceeded the maximum permitted size
    #[error("A frame exceeded the maximum permitted size")]
    FrameOverflow,
    /// A peer attempted to use an extension that has not been negotiated
    #[error("Attempted to use an extension that has not been negotiated")]
    UnknownExtension,
    /// Received a continuation frame before one has been started
    #[error("Received a continuation frame before one has been started")]
    ContinuationNotStarted,
    /// A peer attempted to start another continuation before the previous one has completed
    #[error("Attempted to start another continuation before the previous one has completed")]
    ContinuationAlreadyStarted,
    /// Received an illegal close code
    #[error("Received an illegal close code: `{0}`")]
    CloseCode(u16),
    /// Received unexpected control frame data
    #[error("Received unexpected control frame data")]
    ControlDataMismatch,
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        Error::with_cause(ErrorKind::Encoding, e)
    }
}
