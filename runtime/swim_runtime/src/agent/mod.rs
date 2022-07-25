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

use futures::{
    future::{join, BoxFuture},
    FutureExt,
};
use swim_api::{
    agent::{Agent, AgentConfig, AgentContext, LaneConfig, UplinkKind},
    downlink::{Downlink, DownlinkConfig},
    error::{AgentInitError, AgentRuntimeError, AgentTaskError},
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::uri::RelativeUri,
    trigger::{self, promise},
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use std::{
    fmt::{Debug, Display},
    num::NonZeroUsize,
    time::Duration,
};

use crate::routing::RoutingAddr;

use self::task::{AgentInitTask, NoLanes};

mod task;

use task::AgentRuntimeRequest;

/// Implementaton of [`AgentContext`] that communicates with with another task over a channel
/// to perform the supported operations.
struct AgentRuntimeContext {
    tx: mpsc::Sender<AgentRuntimeRequest>,
}

impl AgentRuntimeContext {
    fn new(tx: mpsc::Sender<AgentRuntimeRequest>) -> Self {
        AgentRuntimeContext { tx }
    }
}

impl AgentContext for AgentRuntimeContext {
    fn add_lane<'a>(
        &'a self,
        name: &str,
        uplink_kind: UplinkKind,
        config: Option<LaneConfig>,
    ) -> BoxFuture<'a, Result<Io, AgentRuntimeError>> {
        let name = Text::new(name);
        async move {
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(AgentRuntimeRequest::AddLane {
                    name,
                    kind: uplink_kind,
                    config,
                    promise: tx,
                })
                .await?;
            rx.await?
        }
        .boxed()
    }

    fn open_downlink(
        &self,
        config: DownlinkConfig,
        downlink: Box<dyn Downlink + Send>,
    ) -> BoxFuture<'_, Result<(), AgentRuntimeError>> {
        async move {
            let (tx, rx) = oneshot::channel();
            self.tx
                .send(AgentRuntimeRequest::OpenDownlink {
                    config,
                    downlink,
                    promise: tx,
                })
                .await?;
            rx.await?
        }
        .boxed()
    }
}

/// Ends of two independent channels (for example the input and output channels of an agent).
type Io = (ByteWriter, ByteReader);

/// Reasons that a remote connected to an agent runtime task could be disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectionReason {
    /// The agent stopped as part of a clean shutdown.
    AgentStoppedExternally,
    /// The remote timed out after no longer having any active links.
    RemoteTimedOut,
    /// The agent terminated after a period of inactivity.
    AgentTimedOut,
    /// Another remote registered with the same ID.
    DuplicateRegistration(Uuid),
    /// The remote was dropped by the other party.
    ChannelClosed,
    /// Either the remote was not fully registered before the agent stopped or the agent stopped by
    /// some means other than a clean shutdown (for example, a panic).
    Failed,
}

impl Display for DisconnectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisconnectionReason::AgentStoppedExternally => write!(f, "Agent stopped externally."),
            DisconnectionReason::RemoteTimedOut => {
                write!(f, "The remote was pruned due to inactivity.")
            }
            DisconnectionReason::AgentTimedOut => {
                write!(f, "Agent stopped after a period of inactivity.")
            }
            DisconnectionReason::DuplicateRegistration(id) => {
                write!(f, "The remote registration for {} was replaced.", id)
            }
            DisconnectionReason::ChannelClosed => write!(f, "The remote stopped listening."),
            DisconnectionReason::Failed => write!(
                f,
                "The agent task was dropped or the connection was never established."
            ),
        }
    }
}

/// A request to attach a new remote connection to an agent runtime task.
#[derive(Debug)]
pub struct AgentAttachmentRequest {
    /// The unique ID of the remote endpoint.
    id: Uuid,
    /// Channels over which the agent runtime task should communicate with the endpoint.
    io: Io,
    /// A promise that will be satisified when the agent runtime task closes the remote.
    completion: promise::Sender<DisconnectionReason>,
    /// If provided, this will be triggered when the remote has been fully registered with
    /// the agent runtime request. The completion promise will only receive a non-failed
    /// result after this occurs.
    on_attached: Option<trigger::Sender>,
}

impl AgentAttachmentRequest {
    pub fn new(id: Uuid, io: Io, completion: promise::Sender<DisconnectionReason>) -> Self {
        AgentAttachmentRequest {
            id,
            io,
            completion,
            on_attached: None,
        }
    }

    /// Constructs a request with a trigger that will be called when the registration completes.
    pub fn with_confirmation(
        id: Uuid,
        io: Io,
        completion: promise::Sender<DisconnectionReason>,
        on_attached: trigger::Sender,
    ) -> Self {
        AgentAttachmentRequest {
            id,
            io,
            completion,
            on_attached: Some(on_attached),
        }
    }
}

/// Configuration parameters for the aget runtime task.
#[derive(Debug, Clone, Copy)]
pub struct AgentRuntimeConfig {
    /// Default configuration parameters to use for lanes that do not specify their own.
    pub default_lane_config: LaneConfig,
    /// Size of the queue for hanlding requests to attach remotes to the task.
    pub attachment_queue_size: NonZeroUsize,
    /// If the task is idle for more than this length of time, the agent will stop.
    pub inactive_timeout: Duration,
    /// If a remote, with no links, is idle for more than this length of time, it will be
    /// deregistered.
    pub prune_remote_delay: Duration,
    /// If the clean-shutdown mechanism for the task takes longer than this, it will be
    /// terminated.
    pub shutdown_timeout: Duration,
}

/// Ways in which the agent runtime task can fail.
#[derive(Debug, Error)]
pub enum AgentExecError {
    /// Initializing the agent failed.
    #[error("Failed to initialize agent: {0}")]
    FailedInit(#[from] AgentInitError),
    /// Initialization completed but no lanes were registered.
    #[error("The agent did not register any lanes.")]
    NoInitialLanes,
    /// The runtime loop of the agent failed.
    #[error("The agent task failed: {0}")]
    FailedTask(#[from] AgentTaskError),
}

impl From<NoLanes> for AgentExecError {
    fn from(_: NoLanes) -> Self {
        AgentExecError::NoInitialLanes
    }
}

/// Run an agent.
///
/// #Arguments
/// * `agent` - The agent instance.
/// * `identity` - The routing ID that will be attached to outgoing envelopes.
/// * `route` - The node URI that will be attached to outgoing envelopes.
/// * `attachment_rx` - Channel for making requests to attach remotes to the agent task.
/// * `stopping` - Instructs the agent task to stop.
/// * `agent_config` - Configuration parameters for the user agent task.
/// * `runtime_config` - Configuration for the runtime part of the agent task.
pub async fn run_agent<A>(
    agent: A,
    identity: RoutingAddr,
    route: RelativeUri,
    attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
    stopping: trigger::Receiver,
    agent_config: AgentConfig,
    runtime_config: AgentRuntimeConfig,
) -> Result<(), AgentExecError>
where
    A: Agent + Send + 'static,
{
    let node_uri = route.to_string().into();
    let (runtime_tx, runtime_rx) = mpsc::channel(runtime_config.attachment_queue_size.get());
    let (init_tx, init_rx) = trigger::trigger();
    let runtime_init_task = AgentInitTask::new(runtime_rx, init_rx, runtime_config);
    let context = AgentRuntimeContext::new(runtime_tx);

    let agent_init = agent.run(route, agent_config, &context);

    let agent_init_task = async move {
        let agent_task_result = agent_init.await;
        drop(init_tx);
        agent_task_result
    };

    let (initial_state_result, agent_task_result) =
        join(runtime_init_task.run(), agent_init_task).await;
    let initial_state = initial_state_result?;
    let agent_task = agent_task_result?;

    let runtime_task = initial_state.make_runtime_task(
        identity,
        node_uri,
        attachment_rx,
        runtime_config,
        stopping,
    );

    let (_, agent_result) = join(runtime_task.run(), agent_task).await;
    agent_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::DisconnectionReason;

    #[test]
    fn disconnection_reason_display() {
        assert_eq!(
            DisconnectionReason::AgentStoppedExternally.to_string(),
            "Agent stopped externally."
        );
        assert_eq!(
            DisconnectionReason::AgentTimedOut.to_string(),
            "Agent stopped after a period of inactivity."
        );
        assert_eq!(
            DisconnectionReason::RemoteTimedOut.to_string(),
            "The remote was pruned due to inactivity."
        );
        assert_eq!(
            DisconnectionReason::ChannelClosed.to_string(),
            "The remote stopped listening."
        );
        assert_eq!(
            DisconnectionReason::Failed.to_string(),
            "The agent task was dropped or the connection was never established."
        );
        assert_eq!(
            DisconnectionReason::DuplicateRegistration(Uuid::from_u128(84772)).to_string(),
            "The remote registration for 00000000-0000-0000-0000-000000014b24 was replaced."
        );
    }
}
