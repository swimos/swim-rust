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

use std::error::Error;
use std::fmt::{Display, Formatter};

use utilities::errors::Recoverable;

use crate::routing::error::FmtResult;
use crate::routing::{format_cause, ConnectionError};

type NativeTlsError = tokio_native_tls::native_tls::Error;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TlsError {
    kind: TlsErrorKind,
    cause: Option<String>,
}

impl TlsError {
    pub fn new(kind: TlsErrorKind, cause: Option<String>) -> TlsError {
        TlsError { kind, cause }
    }

    pub fn kind(&self) -> TlsErrorKind {
        self.kind
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TlsErrorKind {
    /// An IO error was produced when attempting to read a certificate or in the connection.
    Io,
    /// An OpenSSL error.
    Ssl,
    /// The provided certificate is invalid.
    InvalidCertificate,
}

impl Error for TlsError {}

impl Display for TlsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let TlsError { kind, cause } = self;
        let cause = format_cause(cause);

        match kind {
            TlsErrorKind::Io => {
                write!(
                    f,
                    "An IO error was produced during a TLS operation.{}",
                    cause
                )
            }
            TlsErrorKind::Ssl => {
                write!(f, "SSL error.{}", cause)
            }
            TlsErrorKind::InvalidCertificate => {
                write!(f, "Invalid certificate.{}", cause)
            }
        }
    }
}

impl Recoverable for TlsError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<NativeTlsError> for TlsError {
    fn from(e: NativeTlsError) -> Self {
        TlsError::new(TlsErrorKind::Ssl, Some(e.to_string()))
    }
}

impl From<std::io::Error> for TlsError {
    fn from(e: std::io::Error) -> Self {
        TlsError::new(TlsErrorKind::Io, Some(e.to_string()))
    }
}

impl From<TlsError> for ConnectionError {
    fn from(e: TlsError) -> Self {
        ConnectionError::Tls(e)
    }
}

#[test]
fn test_tls_error() {
    assert_eq!(
        TlsError::new(TlsErrorKind::Io, None).to_string(),
        "An IO error was produced during a TLS operation."
    );
    assert_eq!(
        TlsError::new(TlsErrorKind::Ssl, None).to_string(),
        "SSL error."
    );
    assert_eq!(
        TlsError::new(TlsErrorKind::InvalidCertificate, None).to_string(),
        "Invalid certificate."
    );

    assert_eq!(
        TlsError::new(TlsErrorKind::Io, Some("Broken pipe".to_string())).to_string(),
        "An IO error was produced during a TLS operation. Broken pipe"
    );
    assert_eq!(
        TlsError::new(TlsErrorKind::Ssl, Some("Handshake failure".to_string())).to_string(),
        "SSL error. Handshake failure"
    );
    assert_eq!(
        TlsError::new(
            TlsErrorKind::InvalidCertificate,
            Some("Certificate has expired".to_string())
        )
        .to_string(),
        "Invalid certificate. Certificate has expired"
    );

    assert!(TlsError::new(TlsErrorKind::Io, None).is_fatal());
    assert!(TlsError::new(TlsErrorKind::Ssl, None).is_fatal());
    assert!(TlsError::new(TlsErrorKind::InvalidCertificate, None).is_fatal());

    let e: TlsError = tokio_native_tls::native_tls::Certificate::from_der(&Vec::new())
        .err()
        .unwrap()
        .into();

    matches!(
        e,
        TlsError {
            kind: TlsErrorKind::Ssl,
            ..
        }
    );
}
