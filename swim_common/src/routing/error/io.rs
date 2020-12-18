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
use std::io::ErrorKind;
use utilities::errors::Recoverable;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct IoError {
    kind: ErrorKind,
    cause: Option<String>,
}

impl IoError {
    pub fn new(kind: ErrorKind, cause: Option<String>) -> IoError {
        IoError { kind, cause }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl Error for IoError {}

impl Display for IoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let IoError { kind, cause } = self;
        let cause = format_cause(cause);

        write!(f, "IO error: {:?}.{}", kind, cause)
    }
}

impl Recoverable for IoError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<IoError> for ConnectionError {
    fn from(e: IoError) -> Self {
        ConnectionError::Io(e)
    }
}

impl From<std::io::Error> for IoError {
    fn from(e: std::io::Error) -> Self {
        IoError::new(e.kind(), Some(e.to_string()))
    }
}

impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::Io(e.into())
    }
}

#[test]
fn test_io_error() {
    assert_eq!(
        IoError::new(ErrorKind::ConnectionAborted, None).to_string(),
        "IO error: ConnectionAborted."
    );
    assert_eq!(
        IoError::new(
            ErrorKind::ConnectionAborted,
            Some("Aborted by peer".to_string())
        )
        .to_string(),
        "IO error: ConnectionAborted. Aborted by peer"
    );

    assert!(IoError::new(ErrorKind::ConnectionAborted, None).is_fatal());

    let error: IoError =
        std::io::Error::new(ErrorKind::ConnectionAborted, "Aborted by peer").into();
    assert_eq!(
        error,
        IoError::new(ErrorKind::ConnectionAborted, Some("Aborted by peer".into()))
    );
}
