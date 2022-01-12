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

use crate::downlink::DownlinkKind;
use std::error::Error;
use std::fmt::{Display, Formatter};
use swim_model::path::Addressable;
use swim_model::Value;
use swim_runtime::error::{ConnectionError, RoutingError};
use swim_schema::schema::StandardSchema;
use swim_utilities::errors::Recoverable;
use swim_utilities::future::item_sink;
use swim_utilities::future::request::request_future::RequestError;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

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

impl DownlinkError {
    pub fn is_bad_message(&self) -> bool {
        matches!(
            self,
            DownlinkError::MalformedMessage | DownlinkError::SchemaViolation(_, _)
        )
    }
}

impl From<RoutingError> for DownlinkError {
    fn from(e: RoutingError) -> Self {
        match e {
            RoutingError::Dropped => DownlinkError::DroppedChannel,
            RoutingError::Connection(e) => match e {
                ConnectionError::Closed(_) => DownlinkError::ClosingFailure,
                ConnectionError::RouterDropped => DownlinkError::DroppedChannel,
                e => DownlinkError::ConnectionFailure(format!(
                    "The connection has been lost. Cause: {}",
                    e
                )),
            },
            RoutingError::Resolution(e) => {
                DownlinkError::ConnectionFailure(format!("Resolution error: `{0}`", e))
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

impl<T> From<item_sink::SendError<T>> for DownlinkError {
    fn from(_: item_sink::SendError<T>) -> Self {
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

impl<T> From<swim_utilities::future::item_sink::SendError<T>> for DroppedError {
    fn from(_: swim_utilities::future::item_sink::SendError<T>) -> Self {
        DroppedError
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SubscriptionError<Path> {
    BadKind {
        expected: DownlinkKind,
        actual: DownlinkKind,
    },
    DownlinkTaskStopped,
    IncompatibleValueSchema {
        path: Path,
        existing: Box<StandardSchema>,
        requested: Box<StandardSchema>,
    },
    IncompatibleMapSchema {
        is_key: bool,
        path: Path,
        existing: Box<StandardSchema>,
        requested: Box<StandardSchema>,
    },
    ConnectionError,
}

impl<Path: Addressable> SubscriptionError<Path> {
    pub fn incompatible_value(
        path: Path,
        existing: StandardSchema,
        requested: StandardSchema,
    ) -> Self {
        SubscriptionError::IncompatibleValueSchema {
            path,
            existing: Box::new(existing),
            requested: Box::new(requested),
        }
    }

    pub fn incompatible_map_key(
        path: Path,
        existing: StandardSchema,
        requested: StandardSchema,
    ) -> Self {
        SubscriptionError::IncompatibleMapSchema {
            is_key: true,
            path,
            existing: Box::new(existing),
            requested: Box::new(requested),
        }
    }

    pub fn incompatible_map_value(
        path: Path,
        existing: StandardSchema,
        requested: StandardSchema,
    ) -> Self {
        SubscriptionError::IncompatibleMapSchema {
            is_key: false,
            path,
            existing: Box::new(existing),
            requested: Box::new(requested),
        }
    }
}

impl<Path: Addressable> From<oneshot::error::RecvError> for SubscriptionError<Path> {
    fn from(_: oneshot::error::RecvError) -> Self {
        SubscriptionError::DownlinkTaskStopped
    }
}

impl<Path: Addressable> From<RequestError> for SubscriptionError<Path> {
    fn from(_: RequestError) -> Self {
        SubscriptionError::ConnectionError
    }
}

impl<Path: Addressable> From<ConnectionError> for SubscriptionError<Path> {
    fn from(_: ConnectionError) -> Self {
        SubscriptionError::ConnectionError
    }
}

impl<Path: Addressable> Display for SubscriptionError<Path> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionError::BadKind { expected, actual } => write!(
                f,
                "Requested {} downlink but a {} downlink was already open for that lane.",
                expected, actual
            ),
            SubscriptionError::DownlinkTaskStopped => {
                write!(f, "The downlink task has already stopped.")
            }
            SubscriptionError::IncompatibleValueSchema {
                path,
                existing,
                requested,
            } => {
                write!(f, "A downlink was requested to {} with schema {} but one is already running with schema {}.",
                       path, existing, requested)
            }
            SubscriptionError::IncompatibleMapSchema {
                is_key,
                path,
                existing,
                requested,
            } => {
                let key_or_val = if *is_key { "key" } else { "value" };
                write!(f, "A map downlink was requested to {} with {} schema {} but one is already running with schema {}.",
                       path, key_or_val, existing, requested)
            }
            SubscriptionError::ConnectionError => {
                write!(f, "The downlink could not establish a connection.")
            }
        }
    }
}

impl<Path: Addressable> std::error::Error for SubscriptionError<Path> {}

impl<Path: Addressable> SubscriptionError<Path> {
    pub fn bad_kind(expected: DownlinkKind, actual: DownlinkKind) -> SubscriptionError<Path> {
        SubscriptionError::BadKind { expected, actual }
    }
}
