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

use crate::routing::error::FmtResult;
pub use http::{self, StatusCode};

use crate::routing::{format_cause, ConnectionError};
use std::error::Error;
use std::fmt::{Display, Formatter};
use utilities::errors::Recoverable;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HttpError {
    kind: HttpErrorKind,
    cause: Option<String>,
}

impl HttpError {
    pub fn new(kind: HttpErrorKind, cause: Option<String>) -> HttpError {
        HttpError { kind, cause }
    }

    pub fn kind(&self) -> &HttpErrorKind {
        &self.kind
    }

    pub fn invalid_url(url: String, cause: Option<String>) -> HttpError {
        HttpError::new(
            HttpErrorKind::InvalidUri(InvalidUriError {
                kind: InvalidUriErrorKind::Malformatted,
                cause: Some(url),
            }),
            cause,
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum HttpErrorKind {
    StatusCode(Option<StatusCode>),
    InvalidHeaderName,
    InvalidHeaderValue,
    InvalidUri(InvalidUriError),
    InvalidMethod,
}

impl Error for HttpError {}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let HttpError { kind, cause } = self;
        let cause = format_cause(cause);

        match kind {
            HttpErrorKind::StatusCode(code) => match code {
                Some(code) => {
                    write!(f, "HTTP error. Status code: {}.{}", code, cause)
                }
                None => write!(f, "HTTP error.{}", cause),
            },
            HttpErrorKind::InvalidHeaderName => {
                write!(f, "Invalid HTTP header name.{}", cause)
            }
            HttpErrorKind::InvalidHeaderValue => {
                write!(f, "Invalid HTTP header value.{}", cause)
            }
            HttpErrorKind::InvalidUri(e) => {
                write!(f, "{}", e)
            }
            HttpErrorKind::InvalidMethod => {
                write!(f, "Invalid HTTP method.{}", cause)
            }
        }
    }
}

impl Recoverable for HttpError {
    fn is_fatal(&self) -> bool {
        match &self.kind {
            HttpErrorKind::StatusCode(Some(code)) if code.is_server_error() => true,
            HttpErrorKind::StatusCode(Some(code)) if code.is_client_error() => true,
            HttpErrorKind::InvalidHeaderName => true,
            HttpErrorKind::InvalidHeaderValue => true,
            HttpErrorKind::InvalidUri(e) => e.is_fatal(),
            HttpErrorKind::InvalidMethod => true,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InvalidUriError {
    kind: InvalidUriErrorKind,
    cause: Option<String>,
}

impl InvalidUriError {
    pub fn new(kind: InvalidUriErrorKind, cause: Option<String>) -> InvalidUriError {
        InvalidUriError { kind, cause }
    }

    pub fn kind(&self) -> &InvalidUriErrorKind {
        &self.kind
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum InvalidUriErrorKind {
    InvalidScheme,
    InvalidPort,
    MissingScheme,
    Malformatted,
    UnsupportedScheme,
}

impl Error for InvalidUriError {}

impl Display for InvalidUriError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let InvalidUriError { kind, cause } = self;
        let cause = format_cause(cause);

        match kind {
            InvalidUriErrorKind::InvalidScheme => write!(f, "Invalid scheme.{}", cause),
            InvalidUriErrorKind::InvalidPort => write!(f, "Invalid port.{}", cause),
            InvalidUriErrorKind::MissingScheme => write!(f, "Missing scheme.{}", cause),
            InvalidUriErrorKind::Malformatted => write!(f, "Malformatted URL.{}", cause),
            InvalidUriErrorKind::UnsupportedScheme => write!(f, "Unsupported scheme.{}", cause),
        }
    }
}

impl Recoverable for InvalidUriError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<http::uri::InvalidUri> for InvalidUriError {
    fn from(e: http::uri::InvalidUri) -> Self {
        InvalidUriError::new(InvalidUriErrorKind::Malformatted, Some(e.to_string()))
    }
}

impl From<HttpError> for ConnectionError {
    fn from(e: HttpError) -> Self {
        ConnectionError::Http(e)
    }
}

impl From<InvalidUriError> for ConnectionError {
    fn from(e: InvalidUriError) -> Self {
        ConnectionError::Http(HttpError::new(HttpErrorKind::InvalidUri(e), None))
    }
}

impl From<http::Error> for HttpError {
    fn from(e: http::Error) -> Self {
        use http::{header, method, status, uri};

        match e.get_ref() {
            e if e.is::<header::InvalidHeaderValue>() => {
                HttpError::new(HttpErrorKind::InvalidHeaderValue, None)
            }
            e if e.is::<status::InvalidStatusCode>() => {
                HttpError::new(HttpErrorKind::StatusCode(None), None)
            }
            e if e.is::<header::InvalidHeaderName>() => {
                HttpError::new(HttpErrorKind::InvalidHeaderName, None)
            }
            e if e.is::<method::InvalidMethod>() => {
                HttpError::new(HttpErrorKind::InvalidMethod, None)
            }
            e if e.is::<uri::InvalidUri>() => HttpError::new(
                HttpErrorKind::InvalidUri(InvalidUriError::new(
                    InvalidUriErrorKind::Malformatted,
                    None,
                )),
                None,
            ),
            e if e.is::<uri::InvalidUriParts>() => HttpError::new(
                HttpErrorKind::InvalidUri(InvalidUriError::new(
                    InvalidUriErrorKind::Malformatted,
                    None,
                )),
                None,
            ),

            _ => unreachable!(),
        }
    }
}
