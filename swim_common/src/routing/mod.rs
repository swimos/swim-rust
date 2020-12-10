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

mod error;
pub use error::*;
pub mod ws;

use crate::request::request_future::RequestError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use tokio::sync::mpsc::error::SendError as MpscSendError;
use utilities::errors::Recoverable;

// An error returned by the router
#[derive(Clone, Debug, PartialEq)]
pub enum RoutingError {
    // The connection to the remote host has been lost.
    ConnectionError,
    // The remote host is unreachable.
    HostUnreachable,
    // The connection pool has encountered an error.
    PoolError(ConnectionError),
    // The router has been stopped.
    RouterDropped,
    // The router has encountered an error while stopping.
    CloseError,
}

impl Recoverable for RoutingError {
    fn is_fatal(&self) -> bool {
        match &self {
            RoutingError::ConnectionError => false,
            RoutingError::HostUnreachable => false,
            RoutingError::PoolError(e)
                if e.kind == ConnectionErrorKind::Socket(ErrorKind::ConnectionRefused) =>
            {
                false
            }
            _ => true,
        }
    }
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::ConnectionError => write!(f, "Connection error."),
            RoutingError::HostUnreachable => write!(f, "Host unreachable."),
            RoutingError::PoolError(e) => write!(f, "Connection pool error. {}", e),
            RoutingError::RouterDropped => write!(f, "Router was dropped."),
            RoutingError::CloseError => write!(f, "Closing error."),
        }
    }
}

impl Error for RoutingError {}

impl<T> From<MpscSendError<T>> for RoutingError {
    fn from(_: MpscSendError<T>) -> Self {
        RoutingError::RouterDropped
    }
}

impl From<RoutingError> for RequestError {
    fn from(_: RoutingError) -> Self {
        RequestError {}
    }
}
