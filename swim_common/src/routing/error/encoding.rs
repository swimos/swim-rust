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

use crate::routing::error::{format_cause, ConnectionError, FmtResult};
use std::error::Error;
use std::fmt::{Display, Formatter};
use swim_utilities::errors::Recoverable;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodingError {
    kind: EncodingErrorKind,
    cause: Option<String>,
}

impl EncodingError {
    pub fn new(kind: EncodingErrorKind, cause: Option<String>) -> EncodingError {
        EncodingError { kind, cause }
    }

    pub fn kind(&self) -> EncodingErrorKind {
        self.kind
    }

    pub fn invalid() -> EncodingError {
        EncodingError::new(EncodingErrorKind::Invalid, None)
    }

    pub fn unsupported() -> EncodingError {
        EncodingError::new(EncodingErrorKind::Unsupported, None)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum EncodingErrorKind {
    /// An invalid encoding was read.
    Invalid,
    /// Unsupported encoding.
    Unsupported,
}

impl Error for EncodingError {}

impl Display for EncodingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let EncodingError { kind, cause } = self;
        let cause = format_cause(cause);

        match kind {
            EncodingErrorKind::Invalid => {
                write!(f, "Invalid encoding.{}", cause)
            }
            EncodingErrorKind::Unsupported => {
                write!(f, "Unsupported encoding.{}", cause)
            }
        }
    }
}

impl Recoverable for EncodingError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<EncodingError> for ConnectionError {
    fn from(e: EncodingError) -> Self {
        ConnectionError::Encoding(e)
    }
}

#[test]
fn tests() {
    assert_eq!(EncodingError::invalid().to_string(), "Invalid encoding.");
    assert_eq!(
        EncodingError::unsupported().to_string(),
        "Unsupported encoding."
    );

    assert!(EncodingError::invalid().is_fatal());
    assert!(EncodingError::unsupported().is_fatal());

    assert_eq!(
        EncodingError::new(
            EncodingErrorKind::Invalid,
            Some("Invalid byte sequence".to_string())
        )
        .to_string(),
        "Invalid encoding. Invalid byte sequence"
    );
    assert_eq!(
        EncodingError::new(EncodingErrorKind::Unsupported, Some("latin-1".to_string())).to_string(),
        "Unsupported encoding. latin-1"
    );
}
