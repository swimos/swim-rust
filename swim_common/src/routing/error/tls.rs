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
use crate::routing::{format_cause, ConnectionError};
use std::error::Error;
use std::fmt::{Display, Formatter};
use utilities::errors::Recoverable;

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
    IO,
    SSL,
    InvalidCertificate,
}

impl Error for TlsError {}

impl Display for TlsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let TlsError { kind, cause } = self;
        let cause = format_cause(cause);

        match kind {
            TlsErrorKind::IO => {
                write!(
                    f,
                    "An IO error was produced during a TLS operation.{}",
                    cause
                )
            }
            TlsErrorKind::SSL => {
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
        TlsError::new(TlsErrorKind::SSL, Some(e.to_string()))
    }
}

impl From<std::io::Error> for TlsError {
    fn from(e: std::io::Error) -> Self {
        TlsError::new(TlsErrorKind::IO, Some(e.to_string()))
    }
}

impl From<TlsError> for ConnectionError {
    fn from(e: TlsError) -> Self {
        ConnectionError::Tls(e)
    }
}
