// Copyright 2015-2024 Swim Inc.
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

//! # SwimOS API error types

use std::io;
use std::{error::Error, sync::Arc};

use swimos_form::read::ReadError;
use swimos_model::Text;
use swimos_recon::parser::AsyncParseError;
use swimos_utilities::trigger::TriggerError;
use swimos_utilities::{errors::Recoverable, routing::UnapplyError, trigger::promise};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

use crate::agent::WarpLaneKind;
use crate::{address::RelativeAddress, agent::StoreKind};

mod introspection;

pub use introspection::{IntrospectionStopped, LaneIntrospectionError, NodeIntrospectionError};

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
    #[error("{0:?}")]
    Custom(Box<dyn Error + Send + Sync + 'static>),
}

#[derive(Error, Debug, Clone)]
pub enum DownlinkFailureReason {
    #[error("The specified remote URL was not valid.")]
    InvalidUrl,
    #[error("The lane was unresolvable: {0}")]
    UnresolvableRemote(Arc<std::io::Error>),
    #[error("A local lane was not resolvable: {0}")]
    UnresolvableLocal(RelativeAddress<Text>),
    #[error("Connection to the remote host failed: {0}")]
    ConnectionFailed(Arc<std::io::Error>),
    #[error("Failed to negotiate a TLS connection: {message}")]
    TlsConnectionFailed { message: String, recoverable: bool },
    #[error("Could not negotiate a websocket connection: {0}")]
    WebsocketNegotiationFailed(String),
    #[error("The remote client stopped while the downlink was starting.")]
    RemoteStopped,
    #[error("The downlink runtime task stopped during attachment.")]
    DownlinkStopped,
}

/// Error type for operations that communicate with the agent runtime.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRuntimeError {
    #[error("The agent runtime is stopping.")]
    Stopping,
    #[error("The agent runtime has terminated.")]
    Terminated,
}

/// Error type for registering point to point command channels.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommanderRegistrationError {
    /// The registration could not be completed because the agent is stopping.
    #[error(transparent)]
    RuntimeError(#[from] AgentRuntimeError),
    /// The limit on the number of registrations for the agent was exceeded.
    #[error("Too many commander IDs were requested for the agent.")]
    CommanderIdOverflow,
}

impl<T> From<mpsc::error::SendError<T>> for CommanderRegistrationError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        CommanderRegistrationError::RuntimeError(AgentRuntimeError::Terminated)
    }
}

impl From<TriggerError> for CommanderRegistrationError {
    fn from(_: TriggerError) -> Self {
        CommanderRegistrationError::RuntimeError(AgentRuntimeError::Terminated)
    }
}

/// Error type for the operation of spawning a new downlink on the runtime.
#[derive(Error, Debug, Clone)]
pub enum DownlinkRuntimeError {
    #[error(transparent)]
    RuntimeError(#[from] AgentRuntimeError),
    #[error("Opening a new downlink failed: {0}")]
    DownlinkConnectionFailed(DownlinkFailureReason),
}

/// Error type for requests so the runtime for creating/opening a state for an agent.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenStoreError {
    #[error(transparent)]
    RuntimeError(#[from] AgentRuntimeError),
    #[error("The runtime does not support stores.")]
    StoresNotSupported,
    #[error(
        "A store of kind {requested} was requested but the pre-existing store is of kind {actual}."
    )]
    IncorrectStoreKind {
        requested: StoreKind,
        actual: StoreKind,
    },
}

/// Error type for requests to a running agent to register a new lane.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DynamicRegistrationError {
    #[error("This agent does not support dynamically registered items.")]
    DynamicRegistrationsNotSupported,
    #[error("This agent only supports dynamic registration during initialization.")]
    AfterInitialization,
    #[error("The requested item name '{0}' is already in use.")]
    DuplicateName(String),
    #[error("This agent does not support dynamically adding lanes of type: {0}")]
    LaneKindUnsupported(WarpLaneKind),
    #[error("This agent does not support dynamically adding stores of type: {0}")]
    StoreKindUnsupported(StoreKind),
    #[error("This agent does not support dynamically adding HTTP lanes.")]
    HttpLanesUnsupported,
}

/// Error type for an operation to add a new lane to a running agent.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum LaneSpawnError {
    /// The agent runtime stopped before the request could be completed,
    #[error(transparent)]
    Runtime(#[from] AgentRuntimeError),
    /// The agent refused to register the lane.
    #[error(transparent)]
    Registration(#[from] DynamicRegistrationError),
}

impl<T> From<mpsc::error::SendError<T>> for AgentRuntimeError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        AgentRuntimeError::Terminated
    }
}

impl<T> From<mpsc::error::SendError<T>> for OpenStoreError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        OpenStoreError::RuntimeError(AgentRuntimeError::Terminated)
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

impl From<oneshot::error::RecvError> for OpenStoreError {
    fn from(_: oneshot::error::RecvError) -> Self {
        OpenStoreError::RuntimeError(AgentRuntimeError::Terminated)
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

impl Recoverable for DownlinkFailureReason {
    fn is_fatal(&self) -> bool {
        match self {
            DownlinkFailureReason::InvalidUrl => true,
            DownlinkFailureReason::UnresolvableRemote(_) => false,
            DownlinkFailureReason::ConnectionFailed(_) => false,
            DownlinkFailureReason::WebsocketNegotiationFailed(_) => false,
            DownlinkFailureReason::RemoteStopped => false,
            DownlinkFailureReason::DownlinkStopped => false,
            DownlinkFailureReason::UnresolvableLocal(_) => true,
            DownlinkFailureReason::TlsConnectionFailed { recoverable, .. } => !recoverable,
        }
    }
}

impl Recoverable for DownlinkRuntimeError {
    fn is_fatal(&self) -> bool {
        match self {
            DownlinkRuntimeError::RuntimeError(_) => true,
            DownlinkRuntimeError::DownlinkConnectionFailed(err) => err.is_fatal(),
        }
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
    #[error("The agent failed to generate a required output: {0}")]
    OutputFailed(std::io::Error),
    #[error("Attempting to register a commander failed.")]
    CommanderRegistrationFailed,
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
    #[error("Initializing the state of an agent lane failed: {0}")]
    LaneInitializationFailure(FrameIoError),
    #[error("Attempting to dynamically register a lane failed: {0}")]
    RegistrationFailure(#[from] DynamicRegistrationError),
    #[error(
        "Requested a store of kind {requested} for item {name} but store was of kind {actual}."
    )]
    IncorrectStoreKind {
        name: Text,
        requested: StoreKind,
        actual: StoreKind,
    },
    #[error("The parameters passed to the agent were invalid: {0}")]
    InvalidParameters(#[from] UnapplyError),
    #[error("Failed to initialize introspection for an agent: {0}")]
    NodeIntrospection(#[from] NodeIntrospectionError),
    #[error("Failed to initialize introspection for a labe: {0}")]
    LaneIntrospection(#[from] LaneIntrospectionError),
}

//TODO Make this more sophisticated.
impl From<AgentRuntimeError> for AgentInitError {
    fn from(_: AgentRuntimeError) -> Self {
        AgentInitError::FailedToStart
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    /// This implementation does not provide stores.
    #[error("No store available.")]
    NoStoreAvailable,
    /// The provided key was not found in the store.
    #[error("The specified key was not found")]
    KeyNotFound,
    /// A key returned by the store did not have the correct format.
    #[error("The store returned an invalid key")]
    InvalidKey,
    /// Invalid operation attempted.
    #[error("An invalid operation was attempted (e.g. Updating a map entry on a value entry)")]
    InvalidOperation,
    /// The delegate byte engine failed to initialise.
    #[error("The delegate store engine failed to initialise: {0}")]
    InitialisationFailure(String),
    /// An IO error produced by the delegate byte engine.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// An error produced when attempting to encode a value.
    #[error("Encoding error: {0}")]
    Encoding(String),
    /// An error produced when attempting to decode a value.
    #[error("Decoding error: {0}")]
    Decoding(String),
    /// A raw error produced by the delegate byte engine.
    #[error("Delegate store error: {0}")]
    Delegate(Box<dyn std::error::Error + Send + Sync>),
    /// A raw error produced by the delegate byte engine that isn't send or sync
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
