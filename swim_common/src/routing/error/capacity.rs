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
    /// The read buffer is full.
    ReadFull,
    /// The write buffer is full.
    WriteFull,
    /// A buffer is either full or has overflowed.
    Ambiguous,
    #[cfg(feature = "tungstenite")]
    /// A Tokio Tungstenite buffer is full. Contains the message that was attempted to be written.
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
        #[cfg(not(feature = "tungstenite"))]
        {
            !matches!(
                self.kind,
                CapacityErrorKind::ReadFull | CapacityErrorKind::WriteFull
            )
        }
        #[cfg(feature = "tungstenite")]
        {
            !matches!(
                self.kind,
                CapacityErrorKind::ReadFull
                    | CapacityErrorKind::WriteFull
                    | CapacityErrorKind::Full(_)
            )
        }
    }
}

impl From<CapacityError> for ConnectionError {
    fn from(e: CapacityError) -> Self {
        ConnectionError::Capacity(e)
    }
}

#[test]
fn tests() {
    assert_eq!(CapacityError::read().to_string(), "Read buffer full.");
    assert_eq!(CapacityError::write().to_string(), "Write buffer full.");

    #[cfg(feature = "tungstenite")]
    {
        use tokio_tungstenite::tungstenite::Message;

        assert_eq!(
            CapacityError::new(CapacityErrorKind::Full(Message::Pong(vec![])), None).to_string(),
            "Read buffer full or message greater than write buffer capacity."
        );

        assert_eq!(
            CapacityError::new(
                CapacityErrorKind::Full(Message::Pong(vec![])),
                Some("Bad message.".to_string())
            )
            .to_string(),
            "Read buffer full or message greater than write buffer capacity. Bad message."
        );

        assert!(
            !CapacityError::new(CapacityErrorKind::Full(Message::Pong(vec![])), None).is_fatal()
        );
    }

    assert!(CapacityError::new(CapacityErrorKind::Ambiguous, None).is_fatal());
    assert!(!CapacityError::new(CapacityErrorKind::ReadFull, None).is_fatal());
    assert!(!CapacityError::new(CapacityErrorKind::WriteFull, None).is_fatal());

    assert_eq!(
        CapacityError::new(
            CapacityErrorKind::Ambiguous,
            Some("Unknown cause".to_string())
        )
        .to_string(),
        "Buffer overflow. Unknown cause"
    );

    assert_eq!(
        CapacityError::new(CapacityErrorKind::ReadFull, Some("Failed".to_string())).to_string(),
        "Read buffer full. Failed"
    );

    assert_eq!(
        CapacityError::new(CapacityErrorKind::WriteFull, Some("Failed".to_string())).to_string(),
        "Write buffer full. Failed"
    );
}
