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

use crate::routing::error::FmtResult;

use crate::routing::{format_cause, ConnectionError};
use std::error::Error;
use std::fmt::{Display, Formatter};
use swim_errors::Recoverable;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ProtocolError {
    kind: ProtocolErrorKind,
    cause: Option<String>,
}

impl ProtocolError {
    pub fn new(kind: ProtocolErrorKind, cause: Option<String>) -> ProtocolError {
        ProtocolError { kind, cause }
    }

    pub fn kind(&self) -> ProtocolErrorKind {
        self.kind
    }

    pub fn cause(&self) -> &Option<String> {
        &self.cause
    }

    pub fn websocket(cause: Option<String>) -> ProtocolError {
        ProtocolError::new(ProtocolErrorKind::WebSocket, cause)
    }

    pub fn warp(cause: Option<String>) -> ProtocolError {
        ProtocolError::new(ProtocolErrorKind::Warp, cause)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ProtocolErrorKind {
    /// A WebSocket violation occurred.
    WebSocket,
    /// A WARP violation occurred.
    Warp,
}

impl Error for ProtocolError {}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let ProtocolError { kind, cause } = self;
        let cause = format_cause(cause);

        match kind {
            ProtocolErrorKind::WebSocket => write!(f, "WebSocket protocol violation.{}", cause),
            ProtocolErrorKind::Warp => write!(f, "WARP violation.{}", cause),
        }
    }
}

impl Recoverable for ProtocolError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<ProtocolError> for ConnectionError {
    fn from(e: ProtocolError) -> Self {
        ConnectionError::Protocol(e)
    }
}

#[test]
fn test_protocol_error() {
    assert_eq!(ProtocolError::warp(None).to_string(), "WARP violation.");
    assert_eq!(
        ProtocolError::websocket(None).to_string(),
        "WebSocket protocol violation."
    );
    assert_eq!(
        ProtocolError::warp(Some("Unexpected link message".to_string())).to_string(),
        "WARP violation. Unexpected link message"
    );
    assert_eq!(
        ProtocolError::websocket(Some("No extension negotiated".to_string())).to_string(),
        "WebSocket protocol violation. No extension negotiated"
    );

    assert!(ProtocolError::warp(None).is_fatal());
    assert!(ProtocolError::websocket(None).is_fatal());
}
