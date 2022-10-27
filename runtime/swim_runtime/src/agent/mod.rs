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
    agent::{Agent, AgentConfig, AgentContext, LaneConfig},
    downlink::DownlinkKind,
    error::{
        AgentInitError, AgentRuntimeError, AgentTaskError, DownlinkRuntimeError, OpenStoreError,
        StoreError,
    },
    meta::lane::LaneKind,
    store::{NodePersistence, StoreKind},
};
use swim_model::{address::Address, Text};
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
    future::Future,
    num::NonZeroUsize,
    time::Duration,
};

use crate::downlink::DownlinkOptions;

use self::{
    reporting::{UplinkReportReader, UplinkReporter},
    store::{StoreInitError, StorePersistence},
    task::AgentInitTask,
};

mod reporting;
mod store;
mod task;

use task::AgentRuntimeRequest;

#[derive(Debug)]
pub struct DownlinkRequest {
    pub key: (Address<Text>, DownlinkKind),
    pub options: DownlinkOptions,
    pub promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
}

impl DownlinkRequest {
    pub fn new(
        path: Address<Text>,
        kind: DownlinkKind,
        options: DownlinkOptions,
        promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
    ) -> Self {
        DownlinkRequest {
            key: (path, kind),
            options,
            promise,
        }
    }
}

/// Implementaton of [`AgentContext`] that communicates with with another task over a channel
/// to perform the supported operations.
#[derive(Clone)]
struct AgentRuntimeContext {
    tx: mpsc::Sender<AgentRuntimeRequest>,
}

impl AgentRuntimeContext {
    fn new(tx: mpsc::Sender<AgentRuntimeRequest>) -> Self {
        AgentRuntimeContext { tx }
    }
}

impl AgentContext for AgentRuntimeContext {
    fn add_lane(
        &self,
        name: &str,
        lane_kind: LaneKind,
        config: LaneConfig,
    ) -> BoxFuture<'static, Result<Io, AgentRuntimeError>> {
        let name = Text::new(name);
        let sender = self.tx.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(AgentRuntimeRequest::AddLane {
                    name,
                    kind: lane_kind.uplink_kind(),
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
        host: Option<&str>,
        node: &str,
        lane: &str,
        kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        let host = host.map(Text::new);
        let node = Text::new(node);
        let lane = Text::new(lane);
        let sender = self.tx.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(AgentRuntimeRequest::OpenDownlink(DownlinkRequest::new(
                    Address::new(host, node, lane),
                    kind,
                    DownlinkOptions::empty(),
                    tx,
                )))
                .await?;
            rx.await?
        }
        .boxed()
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        todo!("Non-lane stores not yet implemented.")
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

pub struct UplinkReporterRegistration {
    agent_id: Uuid,
    lane_name: Text,
    reader: UplinkReportReader,
}

impl UplinkReporterRegistration {
    pub fn new(agent_id: Uuid, lane_name: Text, reader: UplinkReportReader) -> Self {
        UplinkReporterRegistration {
            agent_id,
            lane_name,
            reader,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeReporter {
    agent_id: Uuid,
    aggregate_reporter: UplinkReporter,
    lane_registrations: mpsc::Sender<UplinkReporterRegistration>,
}

impl NodeReporter {
    async fn register(&self, name: Text) -> Option<UplinkReporter> {
        let NodeReporter {
            agent_id,
            lane_registrations,
            ..
        } = self;
        let reporter = UplinkReporter::default();
        let reader = reporter.reader();
        let registration = UplinkReporterRegistration::new(*agent_id, name.clone(), reader);
        if lane_registrations.send(registration).await.is_err() {
            //TODO Log error.
            None
        } else {
            Some(reporter)
        }
    }

    fn aggregate(&self) -> UplinkReporter {
        self.aggregate_reporter.clone()
    }
}

impl NodeReporter {
    pub fn new(
        agent_id: Uuid,
        aggregate_reporter: UplinkReporter,
        lane_registrations: mpsc::Sender<UplinkReporterRegistration>,
    ) -> Self {
        NodeReporter {
            agent_id,
            aggregate_reporter,
            lane_registrations,
        }
    }
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
    /// If initializing a lane from the store takes longer than this, the agent will fail.
    pub lane_init_timeout: Duration,
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
    /// Sending a downlink request to the runtime failed.
    #[error("The runtime failed to handle a downlink request.")]
    FailedDownlinkRequest,
    #[error("Restoring the state of the lane `{lane_name}` failed: {error}")]
    FailedRestoration {
        lane_name: Text,
        #[source]
        error: StoreInitError,
    },
    #[error("Persisting a change to the state of a lan failed: {0}")]
    PersistenceFailure(#[from] StoreError),
}

pub struct AgentRoute {
    pub identity: Uuid,
    pub route: RelativeUri,
}

#[derive(Debug, Clone, Copy)]
pub struct CombinedAgentConfig {
    pub agent_config: AgentConfig,
    pub runtime_config: AgentRuntimeConfig,
}
pub struct AgentRouteTask<'a, A> {
    agent: &'a A,
    identity: Uuid,
    route: RelativeUri,
    attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
    downlink_tx: mpsc::Sender<DownlinkRequest>,
    stopping: trigger::Receiver,
    agent_config: AgentConfig,
    runtime_config: AgentRuntimeConfig,
    reporting: Option<NodeReporter>,
}

impl<'a, A: Agent + 'static> AgentRouteTask<'a, A> {
    /// Run an agent.
    ///
    /// #Arguments
    /// * `agent` - The agent instance.
    /// * `identity` - Routing identify of the agent instance..
    /// * `attachment_rx` - Channel for making requests to attach remotes to the agent task.
    /// * `stopping` - Instructs the agent task to stop.
    /// * `config` - Configuration parameters for the user agent task and agent runtime.
    /// * `reporting` - Uplink metrics reporters.
    pub fn new(
        agent: &'a A,
        identity: AgentRoute,
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        downlink_tx: mpsc::Sender<DownlinkRequest>,
        stopping: trigger::Receiver,
        config: CombinedAgentConfig,
        reporting: Option<NodeReporter>,
    ) -> Self {
        AgentRouteTask {
            agent,
            identity: identity.identity,
            route: identity.route,
            attachment_rx,
            downlink_tx,
            stopping,
            agent_config: config.agent_config,
            runtime_config: config.runtime_config,
            reporting,
        }
    }

    pub fn run_agent(self) -> impl Future<Output = Result<(), AgentExecError>> + Send + 'static {
        let AgentRouteTask {
            agent,
            identity,
            route,
            attachment_rx,
            downlink_tx,
            stopping,
            agent_config,
            runtime_config,
            reporting,
        } = self;
        let node_uri = route.to_string().into();
        let (runtime_tx, runtime_rx) = mpsc::channel(runtime_config.attachment_queue_size.get());
        let (init_tx, init_rx) = trigger::trigger();
        let runtime_init_task = AgentInitTask::new(
            runtime_rx,
            downlink_tx,
            init_rx,
            runtime_config.lane_init_timeout,
            reporting,
        );
        let context = Box::new(AgentRuntimeContext::new(runtime_tx));

        let agent_init = agent.run(route, agent_config, context);

        async move {
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

            let (runtime_result, agent_result) = join(runtime_task.run(), agent_task).await;
            runtime_result?;
            agent_result?;
            Ok(())
        }
    }

    pub fn run_agent_with_store<Store>(
        self,
        store: Store,
    ) -> impl Future<Output = Result<(), AgentExecError>> + Send + 'static
    where
        Store: NodePersistence + Clone + Send + Sync + 'static,
    {
        let AgentRouteTask {
            agent,
            identity,
            route,
            attachment_rx,
            downlink_tx,
            stopping,
            agent_config,
            runtime_config,
            reporting,
        } = self;
        let node_uri = route.to_string().into();
        let (runtime_tx, runtime_rx) = mpsc::channel(runtime_config.attachment_queue_size.get());
        let (init_tx, init_rx) = trigger::trigger();
        let runtime_init_task = AgentInitTask::with_store(
            runtime_rx,
            downlink_tx,
            init_rx,
            runtime_config.lane_init_timeout,
            reporting,
            StorePersistence(store.clone()),
        );
        let context = Box::new(AgentRuntimeContext::new(runtime_tx));

        let agent_init = agent.run(route, agent_config, context);

        async move {
            let agent_init_task = async move {
                let agent_task_result = agent_init.await;
                drop(init_tx);
                agent_task_result
            };

            let (initial_state_result, agent_task_result) =
                join(runtime_init_task.run(), agent_init_task).await;
            let initial_state = initial_state_result?;
            let agent_task = agent_task_result?;

            let runtime_task = initial_state.make_runtime_task_with_store(
                identity,
                node_uri,
                attachment_rx,
                runtime_config,
                stopping,
                StorePersistence(store),
            );

            let (runtime_result, agent_result) = join(runtime_task.run(), agent_task).await;
            runtime_result?;
            agent_result?;
            Ok(())
        }
    }
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
