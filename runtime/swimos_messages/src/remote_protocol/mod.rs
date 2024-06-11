// Copyright 2015-2023 Swim Inc.
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

use swimos_api::{address::RelativeAddress, agent::HttpLaneRequest, error::DownlinkFailureReason};
use swimos_model::Text;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum LinkError {
    #[error("No endpoint with address: {0}")]
    NoEndpoint(RelativeAddress<Text>),
}

impl From<LinkError> for DownlinkFailureReason {
    fn from(err: LinkError) -> Self {
        match err {
            LinkError::NoEndpoint(addr) => DownlinkFailureReason::UnresolvableLocal(addr),
        }
    }
}

/// Message to attach a new client to a socket.
#[derive(Debug)]
pub enum AttachClient {
    /// Attach a send only client.
    OneWay {
        agent_id: Uuid,
        path: Option<RelativeAddress<Text>>,
        receiver: ByteReader,
        done: oneshot::Sender<Result<(), LinkError>>,
    },
    /// Attach a two way (downlink) client.
    AttachDownlink {
        downlink_id: Uuid,
        path: RelativeAddress<Text>,
        sender: ByteWriter,
        receiver: ByteReader,
        done: oneshot::Sender<Result<(), LinkError>>,
    },
}

/// Message type sent by the socket management task to find an agent node.
pub struct FindNode {
    pub node: Text,
    pub lane: Option<Text>,
    pub request: NodeConnectionRequest,
}

pub enum NodeConnectionRequest {
    Warp {
        source: Uuid,
        promise: oneshot::Sender<Result<(ByteWriter, ByteReader), AgentResolutionError>>,
    },
    Http {
        promise: oneshot::Sender<Result<mpsc::Sender<HttpLaneRequest>, AgentResolutionError>>,
    },
}

impl NodeConnectionRequest {
    pub fn fail(self, err: AgentResolutionError) -> Result<(), AgentResolutionError> {
        match self {
            NodeConnectionRequest::Warp { promise, .. } => match promise.send(Err(err)) {
                Err(Err(e)) => Err(e),
                _ => Ok(()),
            },
            NodeConnectionRequest::Http { promise } => match promise.send(Err(err)) {
                Err(Err(e)) => Err(e),
                _ => Ok(()),
            },
        }
    }
}

/// Error type produced when attempting to resolve a lane on an agent that does
/// not exist (the lane name is kept for producing the response envelope).
#[derive(Debug)]
pub struct NoSuchAgent {
    pub node: Text,
    pub lane: Option<Text>,
}

impl Display for NoSuchAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let NoSuchAgent { node, lane } = self;
        write!(f, "Agent '{}' does not exist.", node)?;
        if let Some(lane_name) = lane {
            write!(f, " Requested lane was '{}'.", lane_name)
        } else {
            Ok(())
        }
    }
}

impl std::error::Error for NoSuchAgent {}

/// Error type produced when the resolution of an agent fails.
#[derive(Debug, Error)]
pub enum AgentResolutionError {
    #[error(transparent)]
    NotFound(#[from] NoSuchAgent),
    #[error("The plane is stopping.")]
    PlaneStopping,
}
