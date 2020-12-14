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

use crate::routing::{format_cause, ConnectionError, FmtResult};
use std::error::Error;
use std::fmt::{Display, Formatter};
use utilities::errors::Recoverable;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CapacityError {
    kind: CapacityErrorKind,
    cause: Option<String>,
}

impl CapacityError {
    pub fn new(kind: CapacityErrorKind, cause: Option<String>) -> CapacityError {
        CapacityError { kind, cause }
    }

    pub fn kind(&self) -> &CapacityErrorKind {
        &self.kind
    }

    pub fn read() -> CapacityError {
        CapacityError::new(CapacityErrorKind::ReadFull, None)
    }

    pub fn write() -> CapacityError {
        CapacityError::new(CapacityErrorKind::WriteFull, None)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CapacityErrorKind {
    ReadFull,
    WriteFull,
    Ambiguous,
    #[cfg(feature = "tungstenite")]
    Full(tokio_tungstenite::tungstenite::Message),
}

impl Error for CapacityError {}

impl Display for CapacityError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let CapacityError { kind, cause } = self;
        let cause = format_cause(cause);

        match kind {
            CapacityErrorKind::ReadFull => {
                write!(f, "Read buffer full.{}", cause)
            }
            CapacityErrorKind::WriteFull => {
                write!(f, "Write buffer full.{}", cause)
            }
            CapacityErrorKind::Ambiguous => {
                write!(f, "Buffer overflow.{}", cause)
            }
            #[cfg(feature = "tungstenite")]
            CapacityErrorKind::Full(_) => {
                write!(
                    f,
                    "Read buffer full or message greater than write buffer capacity.{}",
                    cause
                )
            }
        }
    }
}

impl Recoverable for CapacityError {
    fn is_fatal(&self) -> bool {
        !matches!(
            self.kind,
            CapacityErrorKind::ReadFull | CapacityErrorKind::WriteFull | CapacityErrorKind::Full(_)
        )
    }
}

impl From<CapacityError> for ConnectionError {
    fn from(e: CapacityError) -> Self {
        ConnectionError::Capacity(e)
    }
}
