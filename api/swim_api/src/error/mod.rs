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

use std::{fmt::Display, io};

use swim_form::structural::read::ReadError;
use swim_model::Text;
use swim_recon::parser::AsyncParseError;
use swim_utilities::trigger::promise;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

use crate::store::StoreKind;

/// Indicates that an agent or downlink failed to read a frame from a byte stream.
#[derive(Error, Debug)]
pub enum FrameIoError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    BadFrame(#[from] InvalidFrame),
    #[error("The stream terminated when a frame was expected.")]
    InvalidTermination,
}

impl From<AsyncParseError> for FrameIoError {
    fn from(e: AsyncParseError) -> Self {
        FrameIoError::BadFrame(InvalidFrame::InvalidMessageBody(e))
    }
}

/// Indicates that the content of a frame was invalid.
#[derive(Error, Debug)]
pub enum InvalidFrame {
    #[error("An incoming frame was incomplete.")]
    Incomplete,
    #[error("Invalid frame header: {problem}")]
    InvalidHeader { problem: Text },
    #[error("Invalid frame body: {0}")]
    InvalidMessageBody(#[from] AsyncParseError),
}

/// Possible failure modes for a downlink consumer.
#[derive(Error, Debug)]
pub enum DownlinkTaskError {
    #[error("The downlink failed to start.")]
    FailedToStart,
    #[error("A synced envelope was received with no data provided.")]
    SyncedWithNoValue,
    #[error("{0}")]
    BadFrame(#[from] FrameIoError),
    #[error("Failed to deserialize frame body: {0}")]
    DeserializationFailed(#[from] ReadError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownlinkFailureReason {
    Unresolvable,
    ConnectionFailed,
    WebsocketNegotiationFailed,
    RemoteStopped,
    DownlinkStopped,
}

impl Display for DownlinkFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownlinkFailureReason::Unresolvable => write!(f, "The lane was unresolvable."),
            DownlinkFailureReason::ConnectionFailed => {
                write!(f, "Connection to the remote host failed.")
            }
            DownlinkFailureReason::WebsocketNegotiationFailed => {
                write!(f, "Could not negotiate a websocket connection.")
            }
            DownlinkFailureReason::RemoteStopped => {
                write!(
                    f,
                    "The remote client stopped while the downlink was starting."
                )
            }
            DownlinkFailureReason::DownlinkStopped => {
                write!(f, "The downlink runtime task stopped during attachment.")
            }
        }
    }
}

/// Error type for operations that communicate with the agent runtime.
#[derive(Error, Debug, Clone, Copy)]
pub enum AgentRuntimeError {
    #[error("The agent runtime is stopping.")]
    Stopping,
    #[error("The agent runtime has terminated.")]
    Terminated,
}

#[derive(Error, Debug, Clone, Copy)]
pub enum DownlinkRuntimeError {
    #[error(transparent)]
    RuntimeError(#[from] AgentRuntimeError),
    #[error("Opening a new downlink failed: {0}")]
    DownlinkConnectionFailed(DownlinkFailureReason),
}

#[derive(Error, Debug, Clone, Copy)]
pub enum OpenStoreError {
    #[error(transparent)]
    RuntimeError(#[from] AgentRuntimeError),
    #[error("The runtime does not support stores.")]
    StoresNotSupported,
    #[error(
        "A store of kind {requested} was requested but the prexisting store is of kind {actual}."
    )]
    IncorrectStoreKind {
        requested: StoreKind,
        actual: StoreKind,
    },
}

impl<T> From<mpsc::error::SendError<T>> for AgentRuntimeError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        AgentRuntimeError::Terminated
    }
}

impl<T> From<watch::error::SendError<T>> for AgentRuntimeError {
    fn from(_: watch::error::SendError<T>) -> Self {
        AgentRuntimeError::Terminated
    }
}

impl From<promise::PromiseError> for AgentRuntimeError {
    fn from(_: promise::PromiseError) -> Self {
        AgentRuntimeError::Terminated
    }
}

impl From<oneshot::error::RecvError> for AgentRuntimeError {
    fn from(_: oneshot::error::RecvError) -> Self {
        AgentRuntimeError::Terminated
    }
}

impl<T> From<mpsc::error::SendError<T>> for DownlinkRuntimeError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        DownlinkRuntimeError::RuntimeError(AgentRuntimeError::Terminated)
    }
}

impl<T> From<watch::error::SendError<T>> for DownlinkRuntimeError {
    fn from(_: watch::error::SendError<T>) -> Self {
        DownlinkRuntimeError::RuntimeError(AgentRuntimeError::Terminated)
    }
}

impl From<promise::PromiseError> for DownlinkRuntimeError {
    fn from(_: promise::PromiseError) -> Self {
        DownlinkRuntimeError::RuntimeError(AgentRuntimeError::Terminated)
    }
}

impl From<oneshot::error::RecvError> for DownlinkRuntimeError {
    fn from(_: oneshot::error::RecvError) -> Self {
        DownlinkRuntimeError::RuntimeError(AgentRuntimeError::Terminated)
    }
}

/// Error type is returned by implementations of the agent interface trait.
#[derive(Error, Debug)]
pub enum AgentTaskError {
    #[error("Bad frame for lane '{lane}': {error}")]
    BadFrame { lane: Text, error: FrameIoError },
    #[error("Failed to deserialize frame body: {0}")]
    DeserializationFailed(#[from] ReadError),
    #[error("Error in use code (likely an event handler): {0}")]
    UserCodeError(Box<dyn std::error::Error + Send>),
}

/// Error type that is returned by implementations of the agent interface trait during the initialization phase.
#[derive(Error, Debug)]
pub enum AgentInitError {
    #[error("The agent failed to start.")]
    FailedToStart,
    #[error("Multiple lanes with the same name: {0}")]
    DuplicateLane(Text),
    #[error("Error in use code (likely an event handler): {0}")]
    UserCodeError(Box<dyn std::error::Error + Send>),
    #[error("Initializing the state of an anget lane failed: {0}")]
    LaneInitializationFailure(FrameIoError),
}

//TODO Make this more sophisticated.
impl From<AgentRuntimeError> for AgentInitError {
    fn from(_: AgentRuntimeError) -> Self {
        AgentInitError::FailedToStart
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    /// The provided key was not found in the store.
    #[error("The specified key was not found")]
    KeyNotFound,
    /// The delegate byte engine failed to initialised.
    #[error("The delegate store engine failed to initialise: {0}")]
    InitialisationFailure(String),
    /// An IO error produced by the delegate byte engine.
    #[error("IO error: {0}")]
    Io(io::Error),
    /// An error produced when attempting to encode a value.
    #[error("Encoding error: {0}")]
    Encoding(String),
    /// An error produced when attempting to decode a value.
    #[error("Decoding error: {0}")]
    Decoding(String),
    /// A raw error produced by the delegate byte engine.
    #[error("Delegate store error: {0}")]
    Delegate(Box<dyn std::error::Error + Send + Sync>),
    /// A raw error produced by the delegate byte engine that isnt send or sync
    #[error("Delegate store error: {0}")]
    DelegateMessage(String),
    /// An operation was attempted on the byte engine when it was closing.
    #[error("An operation was attempted on the delegate store engine when it was closing")]
    Closing,
    /// The requested keyspace was not found.
    #[error("The requested keyspace was not found")]
    KeyspaceNotFound,
}

impl StoreError {
    pub fn downcast_ref<E: std::error::Error + 'static>(&self) -> Option<&E> {
        match self {
            StoreError::Delegate(d) => {
                if let Some(downcasted) = d.downcast_ref() {
                    return Some(downcasted);
                }
                None
            }
            _ => None,
        }
    }
}

impl PartialEq for StoreError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StoreError::KeyNotFound, StoreError::KeyNotFound) => true,
            (StoreError::InitialisationFailure(l), StoreError::InitialisationFailure(r)) => l.eq(r),
            (StoreError::Io(l), StoreError::Io(r)) => l.kind().eq(&r.kind()),
            (StoreError::Encoding(l), StoreError::Encoding(r)) => l.eq(r),
            (StoreError::Decoding(l), StoreError::Decoding(r)) => l.eq(r),
            (StoreError::Delegate(l), StoreError::Delegate(r)) => l.to_string().eq(&r.to_string()),
            (StoreError::DelegateMessage(l), StoreError::DelegateMessage(r)) => l.eq(r),
            (StoreError::Closing, StoreError::Closing) => true,
            (StoreError::KeyspaceNotFound, StoreError::KeyspaceNotFound) => true,
            _ => false,
        }
    }
}
