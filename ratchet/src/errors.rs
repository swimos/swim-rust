use crate::handshake::RequestError;
use http::header::HeaderName;
use http::status::InvalidStatusCode;
use http::StatusCode;
use std::any::Any;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

pub(crate) type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Debug)]
pub struct Error {
    inner: Box<Inner>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl StdError for Error {}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Error {
        Error {
            inner: Box::new(Inner { kind, source: None }),
        }
    }

    pub(crate) fn with_cause<E>(kind: ErrorKind, source: E) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            inner: Box::new(Inner {
                kind,
                source: Some(source.into()),
            }),
        }
    }

    pub fn downcast_ref<T: Any + StdError>(&self) -> Option<&T> {
        match &self.inner.source {
            Some(source) => source.downcast_ref(),
            None => None,
        }
    }

    pub fn is_http(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Http)
    }

    pub fn is_io(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::IO)
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

impl From<RequestError> for Error {
    fn from(e: RequestError) -> Self {
        Error::with_cause(ErrorKind::Http, e.0)
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
    #[error("Malformatted HTTP request")]
    BadRequest,
    #[error("Invalid HTTP method")]
    InvalidMethod,
    #[error("The provided URI was malformatted")]
    MalformattedUri,
}
