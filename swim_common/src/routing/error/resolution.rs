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
use swim_utilities::errors::Recoverable;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ResolutionError {
    kind: ResolutionErrorKind,
    cause: Option<String>,
}

impl ResolutionError {
    pub fn new(kind: ResolutionErrorKind, cause: Option<String>) -> ResolutionError {
        ResolutionError { kind, cause }
    }

    pub fn kind(&self) -> ResolutionErrorKind {
        self.kind
    }

    pub fn unresolvable(host: String) -> ResolutionError {
        ResolutionError {
            kind: ResolutionErrorKind::Unresolvable,
            cause: Some(host),
        }
    }

    pub fn router_dropped() -> ResolutionError {
        ResolutionError {
            kind: ResolutionErrorKind::RouterDropped,
            cause: None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ResolutionErrorKind {
    /// Unable to resolve the requested peer.
    Unresolvable,
    /// The router has been dropped. This typically indicates that the application is stopping.
    RouterDropped,
}

impl Error for ResolutionError {}

impl Display for ResolutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let ResolutionError { kind, cause } = self;

        match kind {
            ResolutionErrorKind::Unresolvable => match cause {
                Some(addr) => {
                    write!(f, "Address {} could not be resolved.", addr)
                }
                None => write!(f, "Address could not be resolved."),
            },
            ResolutionErrorKind::RouterDropped => {
                let cause = format_cause(cause);

                write!(f, "The router channel was dropped.{}", cause)
            }
        }
    }
}

impl Recoverable for ResolutionError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<ResolutionError> for ConnectionError {
    fn from(e: ResolutionError) -> Self {
        ConnectionError::Resolution(e)
    }
}

#[test]
fn test_resolution_error() {
    assert_eq!(
        ResolutionError::unresolvable("http://swim.ai".to_string()).to_string(),
        "Address http://swim.ai could not be resolved."
    );
    assert_eq!(
        ResolutionError::new(ResolutionErrorKind::Unresolvable, None).to_string(),
        "Address could not be resolved."
    );
    assert_eq!(
        ResolutionError::router_dropped().to_string(),
        "The router channel was dropped."
    );
    assert_eq!(
        ResolutionError::new(
            ResolutionErrorKind::RouterDropped,
            Some("Server stopping".to_string())
        )
        .to_string(),
        "The router channel was dropped. Server stopping"
    );

    assert!(ResolutionError::router_dropped().is_fatal());
    assert!(ResolutionError::unresolvable("swim.ai".to_string()).is_fatal());
}
