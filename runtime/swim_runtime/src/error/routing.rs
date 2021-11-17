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

use crate::error::ConnectionError;

use std::fmt::{Display, Formatter};

use std::time::Duration;
use swim_utilities::errors::Recoverable;

/// Reasons for a router connection to be dropped.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionDropped {
    /// The connection was explicitly closed.
    Closed,
    /// No data passed through the connection, in either direction, within the specified duration.
    TimedOut(Duration),
    /// A remote connection failed with an error.
    Failed(ConnectionError),
    /// A local agent failed.
    AgentFailed,
    /// The promise indicating the reason was dropped (this is likely a bug).
    Unknown,
}

impl Display for ConnectionDropped {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionDropped::Closed => write!(f, "The connection was explicitly closed."),
            ConnectionDropped::TimedOut(t) => write!(f, "The connection timed out after {:?}.", t),
            ConnectionDropped::Failed(err) => write!(f, "The connection failed: '{}'", err),
            ConnectionDropped::AgentFailed => write!(f, "The agent failed."),
            ConnectionDropped::Unknown => write!(f, "The reason could not be determined."),
        }
    }
}

impl ConnectionDropped {
    //The Recoverable trait cannot be implemented as ConnectionDropped is not an Error.
    pub fn is_recoverable(&self) -> bool {
        match self {
            ConnectionDropped::TimedOut(_) => true,
            ConnectionDropped::Failed(err) => err.is_transient(),
            ConnectionDropped::AgentFailed => true,
            _ => false,
        }
    }
}
