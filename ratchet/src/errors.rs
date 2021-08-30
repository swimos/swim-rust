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
    pub(crate) fn new(kind: ErrorKind) -> Error {
        Error {
            inner: Inner { kind, source: None },
        }
    }

    pub(crate) fn with_cause<E>(kind: ErrorKind, source: E) -> Error
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

    pub fn downcast_ref<T: Any + StdError>(&self) -> Option<&T> {
        match &self.inner.source {
            Some(source) => source.downcast_ref(),
            None => None,
        }
    }

    pub fn is_io(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::IO)
    }

    pub fn is_http(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Http)
    }

    pub fn is_extension(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Extension)
    }

    pub fn is_protocol(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Protocol)
    }

    pub fn is_encoding(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Encoding)
    }

    pub fn is_close(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Close)
    }
}

#[derive(Debug)]
struct Inner {
    kind: ErrorKind,
    source: Option<BoxError>,
}

#[derive(Debug)]
pub(crate) enum ErrorKind {
    IO,
    Http,
    Extension,
    Protocol,
    Encoding,
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

#[derive(Error, Debug, PartialEq)]
pub enum HttpError {
    #[error("Redirected: `{0}`")]
    Redirected(String),
    #[error("Status code: `{0}`")]
    Status(StatusCode),
    #[error("Invalid HTTP version: `{0:?}`")]
    HttpVersion(Option<u8>),
    #[error("Missing header: `{0}`")]
    MissingHeader(HeaderName),
    #[error("Invalid header: `{0}`")]
    InvalidHeader(HeaderName),
    #[error("Sec-WebSocket-Accept mismatch")]
    KeyMismatch,
    #[error("Invalid HTTP method")]
    InvalidMethod,
    #[error("The provided URI was malformatted")]
    MalformattedUri,
    #[error("A provided header was malformatted")]
    MalformattedHeader,
}

impl From<InvalidUri> for Error {
    fn from(e: InvalidUri) -> Self {
        Error::with_cause(ErrorKind::Http, e)
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

#[derive(Error, Debug, PartialEq)]
pub enum CloseError {
    #[error("The channel is already closed")]
    Closed,
}

#[derive(Debug, PartialEq, Error)]
pub enum ProtocolError {
    #[error("Not valid UTF-8 encoding")]
    Encoding,
    #[error("Received an unknown subprotocol")]
    UnknownProtocol,
    #[error("Bad OpCode: `{0}`")]
    OpCode(OpCodeParseErr),
    #[error("Received an unexpected unmasked frame")]
    UnmaskedFrame,
    #[error("Received an unexpected masked frame")]
    MaskedFrame,
    #[error("Received a fragmented control frame")]
    FragmentedControl,
    #[error("A frame exceeded the maximum permitted size")]
    FrameOverflow,
    #[error("Attempted to use an extension that has not been negotiated")]
    UnknownExtension,
    #[error("Received a continuation frame before one has been started")]
    ContinuationNotStarted,
    #[error("Attempted to start another continuation before the previous one has completed")]
    ContinuationAlreadyStarted,
    #[error("Received an illegal close code: `{0}`")]
    CloseCode(u16),
    #[error("Received unexpected control frame data")]
    ControlDataMismatch,
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        Error::with_cause(ErrorKind::Encoding, e)
    }
}
