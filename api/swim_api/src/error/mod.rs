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

use std::fmt::Display;

use swim_form::structural::read::ReadError;
use swim_model::Text;
use swim_recon::parser::AsyncParseError;
use swim_utilities::trigger::promise;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// Indicates that an agent or downlink failed to read a frame from a byte stream.
#[derive(Error, Debug)]
pub enum FrameIoError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    BadFrame(#[from] InvalidFrame),
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
                write!(f, "The remote client stopped while the downlink was starting.")
            }
        }
    }
}

/// Error type for operations that communicate with the agent runtime.
#[derive(Error, Debug, Clone, Copy)]
pub enum AgentRuntimeError {
    #[error("Opening a new downlink failed: {0}")]
    DownlinkConnectionFailed(DownlinkFailureReason),
    #[error("The agent runtime is stopping.")]
    Stopping,
    #[error("The agent runtime has terminated.")]
    Terminated,
}

impl<T> From<mpsc::error::SendError<T>> for AgentRuntimeError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
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
}

//TODO Make this more sophisticated.
impl From<AgentRuntimeError> for AgentInitError {
    fn from(_: AgentRuntimeError) -> Self {
        AgentInitError::FailedToStart
    }
}
