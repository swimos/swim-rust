// Copyright 2015-2021 Swim Inc.
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

use crate::error::{FmtResult, Unresolvable};

use crate::routing::RoutingAddr;
use std::error::Error;
use std::fmt::{Display, Formatter};
use swim_utilities::errors::Recoverable;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ResolutionError {
    RouterDropped,
    Unresolvable(RoutingAddr),
}

impl ResolutionError {
    pub fn kind(&self) -> ResolutionErrorKind {
        match self {
            ResolutionError::RouterDropped => ResolutionErrorKind::RouterDropped,
            ResolutionError::Unresolvable(_) => ResolutionErrorKind::Unresolvable,
        }
    }

    pub fn unresolvable(addr: RoutingAddr) -> ResolutionError {
        ResolutionError::Unresolvable(addr)
    }

    pub fn router_dropped() -> ResolutionError {
        ResolutionError::RouterDropped
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
        match self {
            ResolutionError::Unresolvable(addr) => {
                write!(f, "Address {} could not be resolved.", addr)
            }
            ResolutionError::RouterDropped => write!(f, "The router channel was dropped."),
        }
    }
}

impl Recoverable for ResolutionError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<Unresolvable> for ResolutionError {
    fn from(err: Unresolvable) -> Self {
        ResolutionError::Unresolvable(err.0)
    }
}

#[test]
fn test_resolution_error() {
    let id = RoutingAddr::remote(1);
    let id_str = id.to_string();
    assert_eq!(
        ResolutionError::unresolvable(id).to_string(),
        format!("Address {} could not be resolved.", id_str)
    );

    assert_eq!(
        ResolutionError::router_dropped().to_string(),
        "The router channel was dropped."
    );

    assert!(ResolutionError::router_dropped().is_fatal());
    assert!(ResolutionError::unresolvable(id).is_fatal());
}
