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
        IoError::new(e.kind(), e.source().map(ToString::to_string))
    }
}

impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::Io(IoError::new(e.kind(), e.source().map(ToString::to_string)))
    }
}
