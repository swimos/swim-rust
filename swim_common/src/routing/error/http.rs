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

/// A HTTP error produced when attempting to execute a request or read a response.
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
    /// The peer returned the following status code or a request was attempted using an illegal
    /// status code.
    StatusCode(Option<StatusCode>),
    /// An invalid header name was returned or requested.
    InvalidHeaderName,
    /// An invalid header value was returned or requested.
    InvalidHeaderValue,
    /// An invalid URI was read.
    InvalidUri(InvalidUriError),
    /// The peer does not support the requested method or a malformatted method was read.
    InvalidMethod,
}

impl Error for HttpError {}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let HttpError { kind, cause } = self;

        match kind {
            HttpErrorKind::StatusCode(code) => {
                let cause = format_cause(cause);
                match code {
                    Some(code) => {
                        write!(f, "HTTP error. Status code: {}.{}", code, cause)
                    }
                    None => write!(f, "HTTP error.{}", cause),
                }
            }
            HttpErrorKind::InvalidHeaderName => {
                let cause = format_cause(cause);
                write!(f, "Invalid HTTP header name.{}", cause)
            }
            HttpErrorKind::InvalidHeaderValue => {
                let cause = format_cause(cause);
                write!(f, "Invalid HTTP header value.{}", cause)
            }
            HttpErrorKind::InvalidUri(e) => match cause {
                Some(cause) => {
                    write!(f, "{}. Caused by: {}", e, cause)
                }
                None => {
                    write!(f, "{}", e)
                }
            },
            HttpErrorKind::InvalidMethod => {
                let cause = format_cause(cause);
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
    /// A malformatted scheme was provided.
    InvalidScheme,
    /// A malformatted port was provided.
    InvalidPort,
    /// The request is missing a scheme.
    MissingScheme,
    /// The URI was malformatted.
    Malformatted,
    /// The provided scheme is not supported.
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
            InvalidUriErrorKind::Malformatted => write!(f, "Malformatted URI.{}", cause),
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

#[test]
fn test_http_error() {
    assert_eq!(
        HttpError::new(HttpErrorKind::StatusCode(None), None).to_string(),
        "HTTP error."
    );
    assert_eq!(
        HttpError::new(
            HttpErrorKind::StatusCode(Some(StatusCode::IM_A_TEAPOT)),
            None
        )
        .to_string(),
        "HTTP error. Status code: 418 I'm a teapot."
    );
    assert_eq!(
        HttpError::new(HttpErrorKind::InvalidHeaderName, None).to_string(),
        "Invalid HTTP header name."
    );
    assert_eq!(
        HttpError::new(HttpErrorKind::InvalidHeaderValue, None).to_string(),
        "Invalid HTTP header value."
    );
    assert_eq!(
        HttpError::new(
            HttpErrorKind::InvalidUri(InvalidUriError::new(
                InvalidUriErrorKind::UnsupportedScheme,
                None
            )),
            None
        )
        .to_string(),
        "Unsupported scheme."
    );
    assert_eq!(
        HttpError::new(HttpErrorKind::InvalidMethod, None).to_string(),
        "Invalid HTTP method."
    );

    assert_eq!(
        HttpError::new(
            HttpErrorKind::StatusCode(None),
            Some("Unknown status code".to_string())
        )
        .to_string(),
        "HTTP error. Unknown status code"
    );
    assert_eq!(
        HttpError::new(
            HttpErrorKind::StatusCode(Some(StatusCode::IM_A_TEAPOT)),
            Some("Teapots don't understand HTTP".to_string())
        )
        .to_string(),
        "HTTP error. Status code: 418 I'm a teapot. Teapots don't understand HTTP"
    );
    assert_eq!(
        HttpError::new(
            HttpErrorKind::InvalidHeaderName,
            Some("permessage--deflate".to_string())
        )
        .to_string(),
        "Invalid HTTP header name. permessage--deflate"
    );
    assert_eq!(
        HttpError::new(
            HttpErrorKind::InvalidHeaderValue,
            Some("application//json".to_string())
        )
        .to_string(),
        "Invalid HTTP header value. application//json"
    );
    assert_eq!(
        HttpError::new(
            HttpErrorKind::InvalidUri(InvalidUriError::new(
                InvalidUriErrorKind::UnsupportedScheme,
                Some("xyz".to_string())
            )),
            None
        )
        .to_string(),
        "Unsupported scheme. xyz"
    );
    assert_eq!(
        HttpError::new(
            HttpErrorKind::InvalidUri(InvalidUriError::new(
                InvalidUriErrorKind::UnsupportedScheme,
                Some("xyz".to_string())
            )),
            Some("Bad URI".to_string())
        )
        .to_string(),
        "Unsupported scheme. xyz. Caused by: Bad URI"
    );
    assert_eq!(
        HttpError::new(HttpErrorKind::InvalidMethod, Some("GETT".to_string())).to_string(),
        "Invalid HTTP method. GETT"
    );

    assert!(HttpError::new(HttpErrorKind::InvalidMethod, None).is_fatal());
    assert!(HttpError::new(HttpErrorKind::InvalidHeaderName, None).is_fatal());
    assert!(HttpError::new(HttpErrorKind::InvalidHeaderValue, None).is_fatal());
    assert!(
        !HttpError::new(HttpErrorKind::StatusCode(Some(StatusCode::CONTINUE)), None).is_fatal()
    );
    assert!(HttpError::new(
        HttpErrorKind::StatusCode(Some(StatusCode::IM_A_TEAPOT)),
        None
    )
    .is_fatal());
    assert!(!HttpError::new(
        HttpErrorKind::StatusCode(Some(StatusCode::TEMPORARY_REDIRECT)),
        None
    )
    .is_fatal());
    assert!(HttpError::new(
        HttpErrorKind::InvalidUri(InvalidUriError::new(
            InvalidUriErrorKind::UnsupportedScheme,
            None
        )),
        None
    )
    .is_fatal());
}

#[test]
fn test_invalid_uri_error() {
    assert_eq!(
        InvalidUriError::new(InvalidUriErrorKind::InvalidScheme, None).to_string(),
        "Invalid scheme."
    );
    assert_eq!(
        InvalidUriError::new(InvalidUriErrorKind::InvalidPort, None).to_string(),
        "Invalid port."
    );
    assert_eq!(
        InvalidUriError::new(InvalidUriErrorKind::MissingScheme, None).to_string(),
        "Missing scheme."
    );
    assert_eq!(
        InvalidUriError::new(InvalidUriErrorKind::Malformatted, None).to_string(),
        "Malformatted URI."
    );
    assert_eq!(
        InvalidUriError::new(InvalidUriErrorKind::UnsupportedScheme, None).to_string(),
        "Unsupported scheme."
    );
    assert_eq!(
        InvalidUriError::new(
            InvalidUriErrorKind::InvalidScheme,
            Some("htt p".to_string())
        )
        .to_string(),
        "Invalid scheme. htt p"
    );
    assert_eq!(
        InvalidUriError::new(InvalidUriErrorKind::InvalidPort, Some("90O1".to_string()))
            .to_string(),
        "Invalid port. 90O1"
    );
    assert_eq!(
        InvalidUriError::new(
            InvalidUriErrorKind::MissingScheme,
            Some("Expected a valid scheme".to_string())
        )
        .to_string(),
        "Missing scheme. Expected a valid scheme"
    );
    assert_eq!(
        InvalidUriError::new(
            InvalidUriErrorKind::Malformatted,
            Some("http://localhost::9001".to_string())
        )
        .to_string(),
        "Malformatted URI. http://localhost::9001"
    );
    assert_eq!(
        InvalidUriError::new(
            InvalidUriErrorKind::UnsupportedScheme,
            Some("xyz".to_string())
        )
        .to_string(),
        "Unsupported scheme. xyz"
    );

    assert!(InvalidUriError::new(InvalidUriErrorKind::InvalidScheme, None).is_fatal());
    assert!(InvalidUriError::new(InvalidUriErrorKind::InvalidPort, None).is_fatal());
    assert!(InvalidUriError::new(InvalidUriErrorKind::MissingScheme, None).is_fatal());
    assert!(InvalidUriError::new(InvalidUriErrorKind::Malformatted, None).is_fatal());
    assert!(InvalidUriError::new(InvalidUriErrorKind::UnsupportedScheme, None).is_fatal());
}
