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

use futures::{
    future::{join, BoxFuture},
    FutureExt,
};
use swim_api::{
    agent::{
        Agent, AgentConfig, AgentContext, HttpLaneRequest, HttpLaneRequestChannel, LaneConfig,
    },
    downlink::DownlinkKind,
    error::{
        AgentInitError, AgentRuntimeError, AgentTaskError, DownlinkRuntimeError, OpenStoreError,
        StoreError,
    },
    lane::WarpLaneKind,
    meta::lane::LaneKind,
    net::SchemeHostPort,
    store::{NodePersistence, StoreKind},
};
use swim_model::{address::RelativeAddress, Text};
use swim_utilities::{
    future::retryable::RetryStrategy,
    io::byte_channel::{ByteReader, ByteWriter},
    non_zero_usize,
    routing::route_uri::RouteUri,
    trigger::{self, promise},
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    future::Future,
    num::NonZeroUsize,
    time::Duration,
};

use crate::downlink::DownlinkOptions;

use self::{
    reporting::{UplinkReportReader, UplinkReporter},
    store::{StoreInitError, StorePersistence},
    task::{
        AdHocChannelRequest, AgentInitTask, AgentRuntimeTask, HttpLaneRuntimeSpec, InitTaskConfig,
        LaneRuntimeSpec, LinksTaskConfig, NodeDescriptor, StoreRuntimeSpec,
    },
};

pub mod reporting;
mod store;
mod task;
#[cfg(test)]
mod tests;

use task::AgentRuntimeRequest;
use tracing::{error, info_span, Instrument};

#[derive(Debug)]
pub enum LinkRequest {
    Downlink(DownlinkRequest),
    Commander(CommanderRequest),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum CommanderKey {
    Remote(SchemeHostPort),
    Local(RelativeAddress<Text>),
}

#[derive(Debug)]
pub struct CommanderRequest {
    pub agent_id: Uuid,
    pub key: CommanderKey,
    pub promise: oneshot::Sender<Result<ByteWriter, DownlinkRuntimeError>>,
}

impl CommanderRequest {
    pub fn new(
        agent_id: Uuid,
        key: CommanderKey,
        promise: oneshot::Sender<Result<ByteWriter, DownlinkRuntimeError>>,
    ) -> Self {
        CommanderRequest {
            agent_id,
            key,
            promise,
        }
    }
}

#[derive(Debug)]
pub struct DownlinkRequest {
    pub remote: Option<SchemeHostPort>,
    pub address: RelativeAddress<Text>,
    pub kind: DownlinkKind,
    pub options: DownlinkOptions,
    pub promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
}

impl DownlinkRequest {
    pub fn replace_promise(
        &self,
        replacement: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
    ) -> Self {
        DownlinkRequest {
            remote: self.remote.clone(),
            address: self.address.clone(),
            kind: self.kind,
            options: self.options,
            promise: replacement,
        }
    }
}

impl DownlinkRequest {
    pub fn new(
        remote: Option<SchemeHostPort>,
        address: RelativeAddress<Text>,
        kind: DownlinkKind,
        options: DownlinkOptions,
        promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
    ) -> Self {
        DownlinkRequest {
            remote,
            address,
            kind,
            options,
            promise,
        }
    }
}

/// Implementation of [`AgentContext`] that communicates with with another task over a channel
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
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        let sender = self.tx.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(AgentRuntimeRequest::AdHoc(AdHocChannelRequest::new(tx)))
                .await?;
            rx.await?
        }
        .boxed()
    }

    fn add_lane(
        &self,
        name: &str,
        lane_kind: WarpLaneKind,
        config: LaneConfig,
    ) -> BoxFuture<'static, Result<Io, AgentRuntimeError>> {
        let name = Text::new(name);
        let sender = self.tx.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                    name, lane_kind, config, tx,
                )))
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
        let remote_result = host.map(|h| h.parse::<SchemeHostPort>()).transpose();
        let node = Text::new(node);
        let lane = Text::new(lane);
        let sender = self.tx.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            let remote = match remote_result {
                Ok(r) => r,
                Err(_) => {
                    return Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                        swim_api::error::DownlinkFailureReason::InvalidUrl,
                    ))
                }
            };
            sender
                .send(AgentRuntimeRequest::OpenDownlink(DownlinkRequest::new(
                    remote,
                    RelativeAddress::new(node, lane),
                    kind,
                    DownlinkOptions::DEFAULT,
                    tx,
                )))
                .await?;
            rx.await?
        }
        .boxed()
    }

    fn add_store(
        &self,
        name: &str,
        kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        let name = Text::new(name);
        let sender = self.tx.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(AgentRuntimeRequest::AddStore(StoreRuntimeSpec::new(
                    name,
                    kind,
                    Default::default(),
                    tx,
                )))
                .await?;
            rx.await?
        }
        .boxed()
    }

    fn add_http_lane(
        &self,
        name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        let name = Text::new(name);
        let sender = self.tx.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(AgentRuntimeRequest::AddHttpLane(HttpLaneRuntimeSpec::new(
                    name, tx,
                )))
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
pub enum AgentAttachmentRequest {
    OneWay {
        /// The unique ID of the remote endpoint.
        id: Uuid,
        /// Channels over which the agent runtime task should communicate with the endpoint.
        io: ByteReader,
        /// If provided, this will be triggered when the remote has been fully registered with
        /// the agent runtime request. The completion promise will only receive a non-failed
        /// result after this occurs.
        on_attached: Option<trigger::Sender>,
    },
    TwoWay {
        /// The unique ID of the remote endpoint.
        id: Uuid,
        /// Channels over which the agent runtime task should communicate with the endpoint.
        io: Io,
        /// If provided, this will be triggered when the remote has been fully registered with
        /// the agent runtime. The completion promise will only receive a non-failed
        /// result after this occurs.
        on_attached: Option<trigger::Sender>,
        /// A promise that will be satisfied when the agent runtime task closes the remote.
        completion: promise::Sender<DisconnectionReason>,
    },
}

/// A request from an agent to register a new lane for metadata reporting.
pub struct UplinkReporterRegistration {
    pub agent_id: Uuid,
    pub lane_name: Text,
    pub kind: LaneKind,
    pub reader: UplinkReportReader,
}

impl UplinkReporterRegistration {
    pub fn new(
        agent_id: Uuid,
        lane_name: Text,
        kind: LaneKind,
        reader: UplinkReportReader,
    ) -> Self {
        UplinkReporterRegistration {
            agent_id,
            lane_name,
            kind,
            reader,
        }
    }
}

/// Context to be passed into an agent runtime to allow it to report the traffic over the uplinks
/// of its lanes.
#[derive(Debug, Clone)]
pub struct NodeReporting {
    agent_id: Uuid,
    aggregate_reporter: UplinkReporter,
    lane_registrations: mpsc::Sender<UplinkReporterRegistration>,
}

impl NodeReporting {
    /// #Arguments
    /// * `agent_id` - The unique ID of the agent that will hold this context.
    /// * `aggregate_reporter` - Used to report the aggregated values for all lanes.
    /// * `lane_registrations` - Used by the agent to register a new lane for reporting.
    pub fn new(
        agent_id: Uuid,
        aggregate_reporter: UplinkReporter,
        lane_registrations: mpsc::Sender<UplinkReporterRegistration>,
    ) -> Self {
        NodeReporting {
            agent_id,
            aggregate_reporter,
            lane_registrations,
        }
    }

    /// Register a new lane for reporting.
    async fn register(&self, name: Text, kind: WarpLaneKind) -> Option<UplinkReporter> {
        let NodeReporting {
            agent_id,
            lane_registrations,
            ..
        } = self;
        let reporter = UplinkReporter::default();
        let reader = reporter.reader();
        let registration =
            UplinkReporterRegistration::new(*agent_id, name.clone(), kind.into(), reader);
        if lane_registrations.send(registration).await.is_err() {
            error!(
                "Failed to register lane {} for agent {} for reporting.",
                name, agent_id
            );
            None
        } else {
            Some(reporter)
        }
    }

    /// Get an aggregate reporter for all lanes of the agent.
    fn aggregate(&self) -> UplinkReporter {
        self.aggregate_reporter.clone()
    }
}

impl AgentAttachmentRequest {
    pub fn downlink(id: Uuid, io: Io, completion: promise::Sender<DisconnectionReason>) -> Self {
        AgentAttachmentRequest::TwoWay {
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
        AgentAttachmentRequest::TwoWay {
            id,
            io,
            completion,
            on_attached: Some(on_attached),
        }
    }

    pub fn commander(id: Uuid, io: ByteReader, on_attached: trigger::Sender) -> Self {
        AgentAttachmentRequest::OneWay {
            id,
            io,
            on_attached: Some(on_attached),
        }
    }
}

/// Configuration parameters for the agent runtime task.
#[derive(Debug, Clone, Copy)]
pub struct AgentRuntimeConfig {
    /// Size of the queue for handling requests to attach remotes to the task.
    pub attachment_queue_size: NonZeroUsize,
    /// The size of the channel used by the server runtime to pass HTTP requests to an agent.
    pub agent_http_request_channel_size: NonZeroUsize,
    /// If the task is idle for more than this length of time, the agent will stop.
    pub inactive_timeout: Duration,
    /// If a remote, with no links, is idle for more than this length of time, it will be
    /// deregistered.
    pub prune_remote_delay: Duration,
    /// If the clean-shutdown mechanism for the task takes longer than this, it will be
    /// terminated.
    pub shutdown_timeout: Duration,
    /// If initializing an item from the store takes longer than this, the agent will fail.
    pub item_init_timeout: Duration,
    /// Timeout for outgoing channels to send ad hoc commands.
    pub ad_hoc_output_timeout: Duration,
    /// Retry strategy for opening outgoing channels for ad hoc commands.
    pub ad_hoc_output_retry: RetryStrategy,
    /// The size of the buffer used by the agent to send ad hoc commands to the runtime.
    pub ad_hoc_buffer_size: NonZeroUsize,
    /// The size of the channel used by the agent to pass requests to an HTTP lane.
    pub lane_http_request_channel_size: NonZeroUsize,
}

const DEFAULT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const DEFAULT_CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(16);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_INIT_TIMEOUT: Duration = Duration::from_secs(1);

impl Default for AgentRuntimeConfig {
    fn default() -> Self {
        Self {
            attachment_queue_size: DEFAULT_CHANNEL_SIZE,
            agent_http_request_channel_size: DEFAULT_CHANNEL_SIZE,
            inactive_timeout: DEFAULT_TIMEOUT,
            prune_remote_delay: DEFAULT_TIMEOUT,
            shutdown_timeout: DEFAULT_TIMEOUT,
            item_init_timeout: DEFAULT_INIT_TIMEOUT,
            ad_hoc_output_timeout: DEFAULT_TIMEOUT,
            ad_hoc_output_retry: RetryStrategy::none(),
            ad_hoc_buffer_size: DEFAULT_BUFFER_SIZE,
            lane_http_request_channel_size: DEFAULT_CHANNEL_SIZE,
        }
    }
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
    #[error("Restoring the state of the item `{item_name}` failed: {error}")]
    FailedRestoration {
        item_name: Text,
        #[source]
        error: StoreInitError,
    },
    #[error("Persisting a change to the state of a lane failed: {0}")]
    PersistenceFailure(#[from] StoreError),
}

pub struct AgentRoute {
    pub identity: Uuid,
    pub route: RouteUri,
    pub route_params: HashMap<String, String>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CombinedAgentConfig {
    pub agent_config: AgentConfig,
    pub runtime_config: AgentRuntimeConfig,
}

pub struct AgentRouteChannels {
    attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
    http_rx: mpsc::Receiver<HttpLaneRequest>,
    link_tx: mpsc::Sender<LinkRequest>,
}

impl AgentRouteChannels {
    /// #Arguments
    /// * `attachment_rx` - Channel for making requests to attach remotes to the agent task.
    /// * `http_rx` - Channel for routing HTTP requests to the agent.
    /// * `link_tx` - Channel to request external links from the runtime.
    pub fn new(
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        http_rx: mpsc::Receiver<HttpLaneRequest>,
        link_tx: mpsc::Sender<LinkRequest>,
    ) -> Self {
        AgentRouteChannels {
            attachment_rx,
            http_rx,
            link_tx,
        }
    }
}

pub struct AgentRouteTask<'a, A> {
    agent: &'a A,
    identity: Uuid,
    route: RouteUri,
    route_params: HashMap<String, String>,
    attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
    http_rx: mpsc::Receiver<HttpLaneRequest>,
    link_tx: mpsc::Sender<LinkRequest>,
    stopping: trigger::Receiver,
    agent_config: AgentConfig,
    runtime_config: AgentRuntimeConfig,
    reporting: Option<NodeReporting>,
}

impl<'a, A: Agent + 'static> AgentRouteTask<'a, A> {
    /// Run an agent.
    ///
    /// #Arguments
    /// * `agent` - The agent instance.
    /// * `identity` - Routing identify of the agent instance.
    /// * `channels` - Channels over which the runtime communicates with the agent.
    /// * `stopping` - Instructs the agent task to stop.
    /// * `config` - Configuration parameters for the user agent task and agent runtime.
    /// * `reporting` - Uplink metrics reporters.
    pub fn new(
        agent: &'a A,
        identity: AgentRoute,
        channels: AgentRouteChannels,
        stopping: trigger::Receiver,
        config: CombinedAgentConfig,
        reporting: Option<NodeReporting>,
    ) -> Self {
        AgentRouteTask {
            agent,
            identity: identity.identity,
            route: identity.route,
            route_params: identity.route_params,
            attachment_rx: channels.attachment_rx,
            http_rx: channels.http_rx,
            link_tx: channels.link_tx,
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
            route_params,
            attachment_rx,
            http_rx,
            link_tx,
            stopping,
            agent_config,
            runtime_config,
            reporting,
        } = self;
        let node_uri = route.to_string().into();
        let (runtime_tx, runtime_rx) = mpsc::channel(runtime_config.attachment_queue_size.get());
        let (init_tx, init_rx) = trigger::trigger();

        let ad_hoc_config = LinksTaskConfig {
            buffer_size: runtime_config.ad_hoc_buffer_size,
            retry_strategy: runtime_config.ad_hoc_output_retry,
            timeout_delay: runtime_config.ad_hoc_output_timeout,
        };

        let runtime_init_task = AgentInitTask::new(
            identity,
            runtime_rx,
            link_tx,
            init_rx,
            InitTaskConfig {
                ad_hoc_queue_size: runtime_config.attachment_queue_size,
                item_init_timeout: runtime_config.item_init_timeout,
                external_links: ad_hoc_config,
                http_lane_channel_size: runtime_config.lane_http_request_channel_size,
            },
            reporting,
        );
        let context = Box::new(AgentRuntimeContext::new(runtime_tx));

        let agent_init = agent.run(route, route_params, agent_config, context);

        async move {
            let agent_init_task = async move {
                let agent_task_result = agent_init.await;
                init_tx.trigger();
                agent_task_result
            };

            let (initial_state_result, agent_task_result) =
                join(runtime_init_task.run(), agent_init_task).await;

            let agent_task = agent_task_result?;
            let (initial_state, _) = initial_state_result?;

            let runtime_task = AgentRuntimeTask::new(
                NodeDescriptor::new(identity, node_uri),
                initial_state,
                attachment_rx,
                http_rx,
                stopping,
                runtime_config,
            );

            let (runtime_result, agent_result) = join(runtime_task.run(), agent_task).await;
            runtime_result?;
            agent_result?;
            Ok(())
        }
    }

    pub fn run_agent_with_store<Store, Fut>(
        self,
        store_fut: Fut,
    ) -> impl Future<Output = Result<(), AgentExecError>> + Send + 'static
    where
        Store: NodePersistence + Send + Sync + 'static,
        Fut: Future<Output = Result<Store, StoreError>> + Send + 'static,
    {
        let AgentRouteTask {
            agent,
            identity,
            route,
            route_params,
            attachment_rx,
            http_rx,
            link_tx,
            stopping,
            agent_config,
            runtime_config,
            reporting,
        } = self;
        let node_uri: Text = route.to_string().into();
        let (runtime_tx, runtime_rx) = mpsc::channel(runtime_config.attachment_queue_size.get());
        let (init_tx, init_rx) = trigger::trigger();

        let context = Box::new(AgentRuntimeContext::new(runtime_tx));

        let agent_init = agent
            .run(route, route_params, agent_config, context)
            .instrument(
                info_span!("Agent initialization task.", id = %identity, route = %node_uri),
            );

        let ad_hoc_config = LinksTaskConfig {
            buffer_size: runtime_config.ad_hoc_buffer_size,
            retry_strategy: runtime_config.ad_hoc_output_retry,
            timeout_delay: runtime_config.ad_hoc_output_timeout,
        };

        async move {
            let store = store_fut.await?;
            let runtime_init_task = AgentInitTask::with_store(
                identity,
                runtime_rx,
                link_tx.clone(),
                init_rx,
                InitTaskConfig {
                    ad_hoc_queue_size: runtime_config.attachment_queue_size,
                    item_init_timeout: runtime_config.item_init_timeout,
                    external_links: ad_hoc_config,
                    http_lane_channel_size: runtime_config.lane_http_request_channel_size,
                },
                reporting,
                StorePersistence(store),
            );

            let agent_init_task = async move {
                let agent_task_result = agent_init.await;
                drop(init_tx);
                agent_task_result
            };

            let (initial_state_result, agent_task_result) =
                join(runtime_init_task.run(), agent_init_task).await;

            let (initial_state, store_per) = initial_state_result?;
            let agent_task = agent_task_result?.instrument(
                info_span!("Agent implementation task.", id = %identity, route = %node_uri),
            );

            let runtime_task = AgentRuntimeTask::with_store(
                NodeDescriptor::new(identity, node_uri.clone()),
                initial_state,
                attachment_rx,
                http_rx,
                stopping,
                runtime_config,
                store_per,
            )
            .run()
            .instrument(info_span!("Agent runtime task.", id = %identity, route = %node_uri));

            let (runtime_result, agent_result) = join(runtime_task, agent_task).await;
            runtime_result?;
            agent_result?;
            Ok(())
        }
    }
}
