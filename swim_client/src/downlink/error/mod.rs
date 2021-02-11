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

use std::error::Error;
use std::fmt::{Display, Formatter};
use swim_common::model::schema::StandardSchema;
use swim_common::model::Value;
use swim_common::routing::{ConnectionError, RoutingError};
use swim_common::sink::item;
use tokio::sync::mpsc;
use utilities::errors::Recoverable;

#[cfg(test)]
mod tests;

/// Indicates that an error ocurred within a downlink.
#[derive(Clone, PartialEq, Debug)]
pub enum DownlinkError {
    DroppedChannel,
    TaskPanic(&'static str),
    TransitionError,
    MalformedMessage,
    InvalidAction,
    SchemaViolation(Value, StandardSchema),
    ConnectionFailure(String),
    ConnectionPoolFailure(ConnectionError),
    ClosingFailure,
}

impl From<RoutingError> for DownlinkError {
    fn from(e: RoutingError) -> Self {
        match e {
            RoutingError::RouterDropped => DownlinkError::DroppedChannel,
            RoutingError::ConnectionError => {
                DownlinkError::ConnectionFailure("The connection has been lost".to_string())
            }
            RoutingError::PoolError(e) => DownlinkError::ConnectionPoolFailure(e),
            RoutingError::CloseError => DownlinkError::ClosingFailure,
            RoutingError::HostUnreachable => {
                DownlinkError::ConnectionFailure("The host is unreachable".to_string())
            }
        }
    }
}

impl Display for DownlinkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DownlinkError::DroppedChannel => write!(
                f,
                "An internal channel was dropped and the downlink is now closed."
            ),
            DownlinkError::TaskPanic(m) => {
                write!(f, "The downlink task panicked with: \"{}\"", m)
            }
            DownlinkError::TransitionError => {
                write!(f, "The downlink state machine produced and error.")
            }
            DownlinkError::SchemaViolation(value, schema) => write!(
                f,
                "Received {} but expected a value matching {}.",
                value, schema
            ),
            DownlinkError::MalformedMessage => {
                write!(f, "A message did not have the expected shape.")
            }
            DownlinkError::InvalidAction => {
                write!(f, "An action could not be applied to the internal state.")
            }
            DownlinkError::ConnectionFailure(error) => write!(f, "Connection failure: {}", error),

            DownlinkError::ConnectionPoolFailure(connection_error) => write!(
                f,
                "The connection pool has encountered a failure: {}",
                connection_error
            ),
            DownlinkError::ClosingFailure => write!(f, "An error occurred while closing down."),
        }
    }
}

impl std::error::Error for DownlinkError {}

impl<T> From<mpsc::error::SendError<T>> for DownlinkError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for DownlinkError {
    fn from(_: mpsc::error::TrySendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl From<item::SendError> for DownlinkError {
    fn from(_: item::SendError) -> Self {
        DownlinkError::DroppedChannel
    }
}

/// Error indicating that a state change was attempted in the downlink and failed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TransitionError {
    ReceiverDropped,
    SideEffectFailed,
    IllegalTransition(String),
}

impl Display for TransitionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransitionError::ReceiverDropped => write!(f, "Observer of the update was dropped."),
            TransitionError::SideEffectFailed => write!(f, "A side effect failed to complete."),
            TransitionError::IllegalTransition(err) => {
                write!(f, "An illegal transition was attempted: '{}'", err)
            }
        }
    }
}

/// Indicates that an error ocurred attempting to update the state of a downlink.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UpdateFailure(pub String);

impl Error for TransitionError {}

impl Recoverable for TransitionError {
    fn is_fatal(&self) -> bool {
        matches!(self, TransitionError::IllegalTransition(_))
    }
}

/// Merges a number of different channel send error types.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DroppedError;

impl Display for DroppedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Channel dropped.")
    }
}

impl std::error::Error for DroppedError {}

impl<T> From<mpsc::error::SendError<T>> for DroppedError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        DroppedError
    }
}

impl From<swim_common::sink::item::SendError> for DroppedError {
    fn from(_: swim_common::sink::item::SendError) -> Self {
        DroppedError
    }
}
