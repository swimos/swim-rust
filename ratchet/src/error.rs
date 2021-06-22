use crate::RequestError;
use std::error::Error;
use std::io;

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;

pub struct ConnectionError {
    inner: Box<Inner>,
}

impl ConnectionError {
    pub(crate) fn new(kind: ConnectionErrorKind) -> ConnectionError {
        ConnectionError {
            inner: Box::new(Inner { kind, source: None }),
        }
    }

    pub(crate) fn with_cause<E>(kind: ConnectionErrorKind, source: E) -> ConnectionError
    where
        E: Into<BoxError>,
    {
        ConnectionError {
            inner: Box::new(Inner {
                kind,
                source: Some(source.into()),
            }),
        }
    }
}

struct Inner {
    kind: ConnectionErrorKind,
    source: Option<BoxError>,
}

pub(crate) enum ConnectionErrorKind {
    Request,
    IO,
}

impl From<RequestError> for ConnectionError {
    fn from(e: RequestError) -> Self {
        ConnectionError::with_cause(ConnectionErrorKind::Request, e.0)
    }
}

impl From<io::Error> for ConnectionError {
    fn from(e: io::Error) -> Self {
        ConnectionError::with_cause(ConnectionErrorKind::IO, e)
    }
}
