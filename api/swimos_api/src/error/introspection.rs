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

use swimos_model::Text;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// Error type for the introspection resolver to indicate that the introspection system has stopped
/// when attempting to register an agent.
#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
#[error("The plane introspection task has stopped.")]
pub struct IntrospectionStopped;

/// Error type for the introspection resolver for when a node cannot be introspected.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum NodeIntrospectionError {
    #[error("No running agent at {node_uri}.")]
    NoSuchAgent { node_uri: Text },
    #[error("The plane introspection task has stopped.")]
    IntrospectionStopped,
}

impl<T> From<mpsc::error::SendError<T>> for NodeIntrospectionError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        NodeIntrospectionError::IntrospectionStopped
    }
}

impl From<oneshot::error::RecvError> for NodeIntrospectionError {
    fn from(_: oneshot::error::RecvError) -> Self {
        NodeIntrospectionError::IntrospectionStopped
    }
}

/// Error type for the introspection resolver for when a lane cannot be introspeced.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum LaneIntrospectionError {
    #[error("No running agent at {node_uri}.")]
    NoSuchAgent { node_uri: Text },
    #[error("No lane named {lane_name} running at {node_uri}.")]
    NoSuchLane { node_uri: Text, lane_name: Text },
    #[error("The plane introspection task has stopped.")]
    IntrospectionStopped,
}

impl<T> From<mpsc::error::SendError<T>> for LaneIntrospectionError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        LaneIntrospectionError::IntrospectionStopped
    }
}

impl From<oneshot::error::RecvError> for LaneIntrospectionError {
    fn from(_: oneshot::error::RecvError) -> Self {
        LaneIntrospectionError::IntrospectionStopped
    }
}
