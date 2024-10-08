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

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::{pin, Pin};
use std::time::Duration;

use crate::agent::store::StoreInitError;
use crate::agent::task::links::TriggerUnlink;
use crate::agent::task::sender::LaneSendError;
use crate::agent::task::write_fut::SpecialAction;
use crate::backpressure::InvalidKey;
use crate::timeout_coord::{self, VoteResult};

use self::external_links::{LinksTaskState, NoReport};
use self::init::Initialization;
use self::links::Links;
use self::prune::PruneRemotes;
use self::receiver::{Failed, ItemResponse, LaneData, ResponseData, ResponseReceiver, StoreData};
use self::remotes::{RemoteSender, RemoteTracker, UplinkResponse};
use self::sender::LaneSender;
use self::write_fut::{WriteResult, WriteTask};

use super::reporting::UplinkReporter;
use super::store::{AgentItemInitError, AgentPersistence};
use super::{
    AgentAttachmentRequest, AgentRuntimeConfig, DisconnectionReason, DownlinkRequest, Io,
    NodeReporting,
};
use bytes::{Bytes, BytesMut};
use futures::future::{join4, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::{
    future::{join, select, Either},
    stream::SelectAll,
    Stream, StreamExt,
};
use swimos_api::address::RelativeAddress;
use swimos_api::agent::{
    HttpLaneRequest, HttpLaneRequestChannel, HttpResponseSender, LaneConfig, StoreConfig,
};
use swimos_api::error::{DownlinkRuntimeError, OpenStoreError, StoreError};
use swimos_api::persistence::StoreDisabled;
use swimos_api::{
    agent::{StoreKind, UplinkKind, WarpLaneKind},
    error::AgentRuntimeError,
    http::{Header, HttpResponse, StandardHeaderName, StatusCode, Version},
};
use swimos_messages::protocol::{Operation, RawRequestMessageDecoder, RequestMessage};
use swimos_model::Text;
use swimos_recon::parser::MessageExtractError;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use swimos_utilities::encoding::BytesStr;
use swimos_utilities::future::{immediate_or_join, StopAfterError};
use swimos_utilities::trigger::{self, promise};

mod external_links;
mod init;
mod links;
mod prune;
mod receiver;
mod remotes;
mod sender;
mod uri_params;
mod write_fut;

pub use external_links::LinksTaskConfig;
pub use init::{AgentInitTask, InitTaskConfig};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout, Instant, Sleep};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};
use uuid::Uuid;
#[cfg(test)]
mod fake_store;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct LaneRuntimeSpec {
    pub name: Text,
    pub kind: WarpLaneKind,
    pub config: LaneConfig,
    pub promise: oneshot::Sender<Result<Io, AgentRuntimeError>>,
}

#[derive(Debug)]
pub struct HttpLaneRuntimeSpec {
    pub name: Text,
    pub promise: oneshot::Sender<Result<HttpLaneRequestChannel, AgentRuntimeError>>,
}

impl LaneRuntimeSpec {
    pub fn new(
        name: Text,
        kind: WarpLaneKind,
        config: LaneConfig,
        promise: oneshot::Sender<Result<Io, AgentRuntimeError>>,
    ) -> Self {
        LaneRuntimeSpec {
            name,
            kind,
            config,
            promise,
        }
    }
}

impl HttpLaneRuntimeSpec {
    pub fn new(
        name: Text,
        promise: oneshot::Sender<Result<HttpLaneRequestChannel, AgentRuntimeError>>,
    ) -> Self {
        HttpLaneRuntimeSpec { name, promise }
    }
}

#[derive(Debug)]
pub struct StoreRuntimeSpec {
    pub name: Text,
    pub kind: StoreKind,
    pub config: StoreConfig,
    pub promise: oneshot::Sender<Result<Io, OpenStoreError>>,
}

impl StoreRuntimeSpec {
    pub fn new(
        name: Text,
        kind: StoreKind,
        config: StoreConfig,
        promise: oneshot::Sender<Result<Io, OpenStoreError>>,
    ) -> Self {
        StoreRuntimeSpec {
            name,
            kind,
            config,
            promise,
        }
    }
}

#[derive(Debug)]
pub struct CommandChannelRequest {
    pub promise: oneshot::Sender<Result<ByteWriter, DownlinkRuntimeError>>,
}

impl CommandChannelRequest {
    pub fn new(promise: oneshot::Sender<Result<ByteWriter, DownlinkRuntimeError>>) -> Self {
        CommandChannelRequest { promise }
    }
}

#[derive(Debug)]
pub enum ExternalLinkRequest {
    Command(CommandChannelRequest),
    Downlink(DownlinkRequest),
}

/// Type for requests that can be sent to the agent runtime task by an agent implementation.
#[derive(Debug)]
pub enum AgentRuntimeRequest {
    /// Attempt to open a channel for direct commands.
    Command(CommandChannelRequest),
    /// Attempt to open a new lane for the agent.
    AddLane(LaneRuntimeSpec),
    /// Attempt to open a new lane for the agent.
    AddHttpLane(HttpLaneRuntimeSpec),
    /// Attempt to open a new store for the agent.
    AddStore(StoreRuntimeSpec),
    /// Attempt to open a downlink to a lane on another agent.
    OpenDownlink(DownlinkRequest),
}

/// A labelled channel endpoint (or pair) for a lane.
#[derive(Debug)]
pub struct LaneEndpoint<T> {
    /// The name of the lane.
    name: Text,
    /// The subprotocol used by the lane.
    kind: UplinkKind,
    /// Whether the lane state should be persisted.
    transient: bool,
    /// The channel endpoint/s.
    io: T,
    /// Metadata reporter for the lane.
    reporter: Option<UplinkReporter>,
}

#[derive(Debug)]
pub struct HttpLaneEndpoint {
    name: Text,
    request_sender: mpsc::Sender<HttpLaneRequest>,
}

impl HttpLaneEndpoint {
    fn new(name: Text, request_sender: mpsc::Sender<HttpLaneRequest>) -> Self {
        HttpLaneEndpoint {
            name,
            request_sender,
        }
    }
}

impl<T> LaneEndpoint<T> {
    fn new(
        name: Text,
        kind: UplinkKind,
        transient: bool,
        io: T,
        reporter: Option<UplinkReporter>,
    ) -> Self {
        LaneEndpoint {
            name,
            kind,
            transient,
            io,
            reporter,
        }
    }
}

impl LaneEndpoint<Io> {
    /// Split an instance with two channel endpoints into two, one for each constituent.
    fn split(self) -> (LaneEndpoint<ByteWriter>, LaneEndpoint<ByteReader>) {
        let LaneEndpoint {
            name,
            kind,
            transient,
            io: (tx, rx),
            reporter,
        } = self;

        let read = LaneEndpoint::new(name.clone(), kind, transient, rx, reporter.clone());

        let write = LaneEndpoint::new(name, kind, transient, tx, reporter);

        (write, read)
    }
}

impl LaneEndpoint<ByteReader> {
    /// Create a [`Stream`] that will read messages from an endpoint.
    fn into_lane_stream<I>(
        self,
        store_id: Option<I>,
        state: &mut WriteTaskState,
    ) -> ResponseReceiver<I> {
        let LaneEndpoint {
            name,
            kind,
            io: reader,
            reporter,
            ..
        } = self;
        let id = state.register_lane(name, reporter);
        match kind {
            UplinkKind::Value => ResponseReceiver::value_like_lane(id, store_id, reader),
            UplinkKind::Supply => ResponseReceiver::supply_lane(id, store_id, reader),
            UplinkKind::Map => ResponseReceiver::map_lane(id, store_id, reader),
        }
    }
}

impl LaneEndpoint<ByteWriter> {
    fn into_read_task_message(self) -> ReadTaskMessage {
        let LaneEndpoint {
            name,
            kind,
            io: tx,
            reporter,
            ..
        } = self;
        let sender = LaneSender::new(tx, kind, reporter);
        ReadTaskMessage::Lane { name, sender }
    }
}

/// A labelled channel endpoint (or pair) for a lane.
#[derive(Debug)]
pub struct StoreEndpoint {
    /// The name of the lane.
    name: Text,
    /// The subprotocol used by the lane.
    kind: StoreKind,
    /// The channel endpoint.
    reader: ByteReader,
}

impl StoreEndpoint {
    fn new(name: Text, kind: StoreKind, reader: ByteReader) -> Self {
        StoreEndpoint { name, kind, reader }
    }

    /// Create a [`Stream`] that will read messages from an endpoint.
    fn into_store_stream<I>(self, store_id: I, state: &mut WriteTaskState) -> ResponseReceiver<I> {
        let StoreEndpoint { kind, reader, .. } = self;
        let item_id = state.item_id_for_store();
        match kind {
            StoreKind::Value => ResponseReceiver::value_store(item_id, store_id, reader),
            StoreKind::Map => ResponseReceiver::map_store(item_id, store_id, reader),
        }
    }
}

#[derive(Debug, Default)]
pub struct Endpoints {
    lane_endpoints: Vec<LaneEndpoint<Io>>,
    http_lane_endpoints: Vec<HttpLaneEndpoint>,
    store_endpoints: Vec<StoreEndpoint>,
}

/// Result of the agent initialization task (detailing the lanes that were created during initialization).
#[derive(Debug)]
pub struct InitialEndpoints {
    reporting: Option<NodeReporting>,
    rx: mpsc::Receiver<AgentRuntimeRequest>,
    endpoints: Endpoints,
    ext_link_state: LinksTaskState,
}

impl InitialEndpoints {
    fn new(
        reporting: Option<NodeReporting>,
        rx: mpsc::Receiver<AgentRuntimeRequest>,
        endpoints: Endpoints,
        ext_link_state: LinksTaskState,
    ) -> Self {
        InitialEndpoints {
            reporting,
            rx,
            endpoints,
            ext_link_state,
        }
    }
}

#[derive(Debug)]
pub struct NodeDescriptor {
    identity: Uuid,
    node_uri: Text,
}

impl NodeDescriptor {
    pub fn new(identity: Uuid, node_uri: Text) -> Self {
        NodeDescriptor { identity, node_uri }
    }
}

/// The runtime task for an agent instance. This consists of three logical sub-components. The
/// first reads from remote attached to the agent and sends the results to the lanes. The second
/// consumes events produced by the lanes, maintains uplinks to remote endpoints and forwards
/// events to linked remotes. The final task manages the registration of new remotes with the
/// other two.
#[derive(Debug)]
pub struct AgentRuntimeTask<Store = StoreDisabled> {
    node: NodeDescriptor,
    init: InitialEndpoints,
    attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
    http_requests: mpsc::Receiver<HttpLaneRequest>,
    stopping: trigger::Receiver,
    config: AgentRuntimeConfig,
    store: Store,
}

/// Message type used by the read and write tasks to communicate with each other.
#[derive(Debug, Clone)]
enum RwCoordinationMessage {
    /// An envelope was received for an unknown lane (and so the write task should issue an appropriate error response).
    UnknownLane {
        origin: Uuid,
        path: RelativeAddress<Text>,
    },
    /// An envelope that was invalid for the subprotocol used by the specified lane was received.
    BadEnvelope {
        origin: Uuid,
        lane: Text,
        error: MessageExtractError,
    },
    /// Instruct the write task to create an uplink from the specified lane to the specified remote.
    Link { origin: Uuid, lane: Text },
    /// Instruct the write task to remove an uplink from the specified lane to the specified remote.
    Unlink { origin: Uuid, lane: Text },
}

impl AgentRuntimeTask {
    /// Create the agent runtime task.
    /// # Arguments
    /// * `node` - The routing ID and node URI of this agent instance.
    /// * `init` - The initial lane and store endpoints for this agent.
    /// * `attachment_rx` - Channel to accept requests to attach remote connections to the agent.
    /// * `http_requests` - Channel to accept HTTP requests targetted at agent lanes.
    /// * `stopping` - A signal for initiating a clean shutdown for the agent instance.
    /// * `config` - Configuration parameters for the agent runtime.
    pub fn new(
        node: NodeDescriptor,
        init: InitialEndpoints,
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        http_requests: mpsc::Receiver<HttpLaneRequest>,
        stopping: trigger::Receiver,
        config: AgentRuntimeConfig,
    ) -> Self {
        AgentRuntimeTask {
            node,
            init,
            attachment_rx,
            http_requests,
            stopping,
            config,
            store: StoreDisabled,
        }
    }
}

impl<Store> AgentRuntimeTask<Store>
where
    Store: AgentPersistence + Send + Sync + 'static,
{
    /// Create the agent runtime task with a store implementation.
    /// # Arguments
    /// * `node` - The routing ID and node URI of this agent instance.
    /// * `init` - The initial lane and store endpoints for this agent.
    /// * `attachment_rx` - Channel to accept requests to attach remote connections to the agent.
    /// * `http_requests` - Channel to accept HTTP requests targetted at agent lanes.
    /// * `stopping` - A signal for initiating a clean shutdown for the agent instance.
    /// * `config` - Configuration parameters for the agent runtime.
    /// * `store` - Persistence store for the agent.
    pub fn with_store(
        node: NodeDescriptor,
        init: InitialEndpoints,
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        http_requests: mpsc::Receiver<HttpLaneRequest>,
        stopping: trigger::Receiver,
        config: AgentRuntimeConfig,
        store: Store,
    ) -> Self {
        AgentRuntimeTask {
            node,
            init,
            attachment_rx,
            http_requests,
            stopping,
            config,
            store,
        }
    }
}

impl<Store> AgentRuntimeTask<Store>
where
    Store: AgentPersistence + Send + Sync,
{
    pub async fn run(self) -> Result<(), StoreError> {
        let AgentRuntimeTask {
            node: NodeDescriptor { identity, node_uri },
            init:
                InitialEndpoints {
                    reporting,
                    rx,
                    endpoints:
                        Endpoints {
                            lane_endpoints,
                            http_lane_endpoints,
                            store_endpoints,
                        },
                    ext_link_state,
                },
            attachment_rx,
            http_requests,
            stopping,
            config,
            store,
        } = self;

        let (write_endpoints, read_endpoints): (Vec<_>, Vec<_>) =
            lane_endpoints.into_iter().map(LaneEndpoint::split).unzip();

        let (read_tx, read_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (write_tx, write_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (http_tx, http_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (ext_link_tx, ext_link_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (read_vote, write_vote, http_vote, vote_waiter) =
            timeout_coord::agent_timeout_coordinator();

        let (kill_switch_tx, kill_switch_rx) = trigger::trigger();

        let combined_stop = select(select(stopping.clone(), kill_switch_rx), vote_waiter);

        let att = attachment_task(
            rx,
            attachment_rx,
            read_tx.clone(),
            write_tx.clone(),
            http_tx,
            ext_link_tx,
            combined_stop,
        )
        .instrument(info_span!("Agent Runtime Attachment Task", %identity, %node_uri));

        let read = read_task(
            config,
            write_endpoints,
            read_rx,
            write_tx,
            read_vote,
            stopping.clone(),
            reporting.as_ref().map(NodeReporting::aggregate),
        )
        .instrument(info_span!("Agent Runtime Read Task", %identity, %node_uri));

        let write = write_task(
            WriteTaskConfiguration::new(identity, node_uri.clone(), config),
            WriteTaskEndpoints::new(read_endpoints, store_endpoints),
            ReceiverStream::new(write_rx).take_until(stopping.clone()),
            read_tx,
            write_vote,
            reporting,
            store,
        )
        .instrument(info_span!("Agent Runtime Write Task", %identity, %node_uri));

        let http_task = http_task(
            stopping,
            config,
            http_requests,
            http_lane_endpoints,
            http_rx,
            http_vote,
        );

        let ext_link_config = LinksTaskConfig {
            buffer_size: config.command_msg_buffer,
            retry_strategy: config.command_output_retry,
            timeout_delay: config.command_output_timeout,
        };

        let ext_links = external_links::external_links_task::<NoReport>(
            identity,
            ext_link_rx,
            ext_link_state,
            ext_link_config,
            None,
        )
        .instrument(info_span!("Agent Ad Hoc Command Task", %identity, %node_uri));

        let io = await_io_tasks(read, write, kill_switch_tx);
        let (_, _, _, result) = join4(att, ext_links, http_task, io).await;
        result
    }
}

/// Control messages consumed by the read task.
enum ReadTaskMessage {
    /// Create a new lane endpoint.
    Lane { name: Text, sender: LaneSender },
    /// Attach a new remote.
    Remote {
        reader: ByteReader,
        on_attached: Option<trigger::Sender>,
    },
    /// Instruct the read task to stop cleanly.
    Stop,
}

/// Control messages consumed by the write task.
#[derive(Debug)]
enum WriteTaskMessage {
    /// Create a new lane endpoint.
    Lane(LaneRuntimeSpec),
    /// Create a new store endpoint.
    Store(StoreRuntimeSpec),
    /// Attach a new remote.
    Remote {
        id: Uuid,
        writer: ByteWriter,
        completion: promise::Sender<DisconnectionReason>,
        on_attached: Option<trigger::Sender>,
    },
    /// A coordination message send by the read task.
    Coord(RwCoordinationMessage),
    /// Instruct the write task to stop cleanly.
    Stop,
}

impl WriteTaskMessage {
    /// Determines whether a write task message constitutes "activity" for the purpose of the agent
    /// timing out.
    fn generates_activity(&self) -> bool {
        matches!(self, WriteTaskMessage::Coord(_))
    }
}

/// The task that coordinates the attachment of new lanes and remotes to the read and write tasks.
/// # Arguments
/// * `runtime` - Requests from the agent.
/// * `attachment` - External requests to attach new remotes.
/// * `read_tx` - Channel to communicate with the read task.
/// * `write_tx` - Channel to communicate with the write task.
/// * `http_tx` - Channel to the HTTP lane task.
/// * `ext_link_tx` - Channel to communicate with the external links task.
/// * `combined_stop` - The task will stop when this future completes. This should combined the overall
///    shutdown-signal with latch that ensures this task will stop if the read/write tasks stop (to avoid
///    deadlocks).
async fn attachment_task<F>(
    mut runtime: mpsc::Receiver<AgentRuntimeRequest>,
    mut attachment: mpsc::Receiver<AgentAttachmentRequest>,
    read_tx: mpsc::Sender<ReadTaskMessage>,
    write_tx: mpsc::Sender<WriteTaskMessage>,
    http_tx: mpsc::Sender<HttpLaneRuntimeSpec>,
    ext_link_tx: mpsc::Sender<ExternalLinkRequest>,
    mut combined_stop: F,
) where
    F: Future + Unpin,
{
    let mut attachments = FuturesUnordered::new();

    loop {
        tokio::select! {
            biased;
            _ = &mut combined_stop => break,
            _ = attachments.next(), if !attachments.is_empty() => {},
            maybe_event = async { tokio::select! {
                maybe_runtime = runtime.recv() => maybe_runtime.map(Either::Left),
                maybe_att = attachment.recv() => maybe_att.map(Either::Right),
            }} => {
                if let Some(event) = maybe_event {
                    match event {
                        Either::Left(request) => {
                            let succeeded = match request {
                                AgentRuntimeRequest::AddLane(req) => write_tx.send(WriteTaskMessage::Lane(req)).await.is_ok(),
                                AgentRuntimeRequest::AddHttpLane(req) => http_tx.send(req).await.is_ok(),
                                AgentRuntimeRequest::AddStore(req) => write_tx.send(WriteTaskMessage::Store(req)).await.is_ok(),
                                AgentRuntimeRequest::Command(request) => ext_link_tx.send(ExternalLinkRequest::Command(request)).await.is_ok(),
                                AgentRuntimeRequest::OpenDownlink(req) => ext_link_tx.send(ExternalLinkRequest::Downlink(req)).await.is_ok(),
                            };
                            if !succeeded {
                                break;
                            }
                        }
                        Either::Right(request) => {
                            if !handle_att_request(request, &read_tx, &write_tx, |read_rx, maybe_write_rx, on_attached| {
                                attachments.push(async move {
                                    if let Some(write_rx) = maybe_write_rx {
                                        if matches!(join(read_rx, write_rx).await, (Ok(_), Ok(_))) {
                                            on_attached.trigger();
                                        }
                                    } else if read_rx.await.is_ok() {
                                        on_attached.trigger();
                                    }
                                })
                            }).await {
                                break;
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }
    let _ = join(
        read_tx.send(ReadTaskMessage::Stop),
        write_tx.send(WriteTaskMessage::Stop),
    )
    .await;
}

#[inline]
async fn handle_att_request<F>(
    request: AgentAttachmentRequest,
    read_tx: &mpsc::Sender<ReadTaskMessage>,
    write_tx: &mpsc::Sender<WriteTaskMessage>,
    add_att: F,
) -> bool
where
    F: FnOnce(trigger::Receiver, Option<trigger::Receiver>, trigger::Sender),
{
    match request {
        AgentAttachmentRequest::TwoWay {
            id,
            io: (tx, rx),
            completion,
            on_attached,
        } => {
            info!(
                "Attaching a new remote endpoint with ID {id} to the agent.",
                id = id
            );
            let read_permit = match read_tx.reserve().await {
                Err(_) => {
                    warn!("Read task stopped while attempting to attach a remote endpoint.");
                    return false;
                }
                Ok(permit) => permit,
            };
            let write_permit = match write_tx.reserve().await {
                Err(_) => {
                    warn!("Write task stopped while attempting to attach a remote endpoint.");
                    return false;
                }
                Ok(permit) => permit,
            };
            let (read_on_attached, write_on_attached) = if let Some(on_attached) = on_attached {
                let (read_tx, read_rx) = trigger::trigger();
                let (write_tx, write_rx) = trigger::trigger();
                add_att(read_rx, Some(write_rx), on_attached);
                (Some(read_tx), Some(write_tx))
            } else {
                (None, None)
            };
            read_permit.send(ReadTaskMessage::Remote {
                reader: rx,
                on_attached: read_on_attached,
            });
            write_permit.send(WriteTaskMessage::Remote {
                id,
                writer: tx,
                completion,
                on_attached: write_on_attached,
            });
            true
        }
        AgentAttachmentRequest::OneWay {
            io: rx,
            id,
            on_attached,
        } => {
            info!(
                "Attaching a new command channel from ID {id} to the agent.",
                id = id
            );
            let read_permit = match read_tx.reserve().await {
                Err(_) => {
                    warn!("Read task stopped while attempting to attach a command channel.");
                    return false;
                }
                Ok(permit) => permit,
            };
            let read_on_attached = if let Some(on_attached) = on_attached {
                let (read_tx, read_rx) = trigger::trigger();
                add_att(read_rx, None, on_attached);
                Some(read_tx)
            } else {
                None
            };
            read_permit.send(ReadTaskMessage::Remote {
                reader: rx,
                on_attached: read_on_attached,
            });
            true
        }
    }
}

type RemoteReceiver = FramedRead<ByteReader, RawRequestMessageDecoder>;

fn remote_receiver(reader: ByteReader) -> RemoteReceiver {
    RemoteReceiver::new(reader, Default::default())
}

const TASK_COORD_ERR: &str = "Stopping after communicating with the write task failed.";
const STOP_VOTED: &str = "Stopping as read, HTTP and write tasks have all voted to do so.";
const STOP_RESCINDED: &str = "Vote to stop rescinded.";
const ATTEMPTING_RESCIND: &str = "Attempting to rescind stop vote.";

/// Events that can occur within the read task.
enum ReadTaskEvent {
    /// Register a new lane or remote.
    Registration(ReadTaskMessage),
    /// An envelope was received from a connected remote.
    Envelope(RequestMessage<BytesStr, Bytes>),
    /// The read task timed out due to inactivity.
    Timeout,
}

/// The read task of the agent runtime. This receives envelopes from attached remotes and forwards
/// them on to the appropriate lanes. It also communicates with the write task to maintain uplinks
/// and report on invalid envelopes.
///
/// # Arguments
/// * `config` - Configuration parameters for the task.
/// * `initial_endpoints` - Initial lane endpoints that were created in the agent initialization phase.
/// * `reg_rx` - Channel for registering new lanes and remotes.
/// * `write_tx` - Channel to communicate with the write task.
/// * `stop_vote` - Votes to stop if this task becomes inactive (unanimity with the write task is required).
/// * `stopping` - Initiates the clean shutdown procedure.
/// * `aggregate_reporter` - Aggregated uplink reporter for all lanes of the agent.
async fn read_task(
    config: AgentRuntimeConfig,
    initial_endpoints: Vec<LaneEndpoint<ByteWriter>>,
    reg_rx: mpsc::Receiver<ReadTaskMessage>,
    write_tx: mpsc::Sender<WriteTaskMessage>,
    stop_vote: timeout_coord::Voter,
    stopping: trigger::Receiver,
    aggregate_reporter: Option<UplinkReporter>,
) {
    let mut remotes = SelectAll::new();

    let mut reg_stream = ReceiverStream::new(reg_rx).take_until(stopping);

    let mut counter: u64 = 0;

    let mut next_id = move || {
        let id = counter;
        counter += 1;
        id
    };

    let mut name_mapping = HashMap::new();
    let mut lanes = HashMap::new();
    let mut needs_flush = None;
    let mut voted = false;

    for LaneEndpoint {
        name,
        kind,
        io,
        reporter,
        ..
    } in initial_endpoints.into_iter()
    {
        let i = next_id();
        name_mapping.insert(name, i);
        lanes.insert(i, LaneSender::new(io, kind, reporter));
    }

    loop {
        let flush = flush_lane(&mut lanes, &mut needs_flush);
        let next = if remotes.is_empty() {
            match immediate_or_join(timeout(config.inactive_timeout, reg_stream.next()), flush)
                .await
            {
                (Ok(Some(reg)), _) => ReadTaskEvent::Registration(reg),
                (Err(_), _) => ReadTaskEvent::Timeout,
                _ => {
                    break;
                }
            }
        } else {
            let select_next = timeout(
                config.inactive_timeout,
                select(reg_stream.next(), remotes.next()),
            );
            let (result, _) = immediate_or_join(select_next, flush).await;
            match result {
                Ok(Either::Left((Some(reg), _))) => ReadTaskEvent::Registration(reg),
                Ok(Either::Left((_, _))) => {
                    info!("Terminating after registration task stopped.");
                    break;
                }
                Ok(Either::Right((Some(Ok(envelope)), _))) => ReadTaskEvent::Envelope(envelope),
                Ok(Either::Right((Some(Err(error)), _))) => {
                    error!(error = ?error, "Failed reading from lane: {}", error);
                    continue;
                }
                Ok(Either::Right((_, _))) => {
                    continue;
                }
                Err(_) => ReadTaskEvent::Timeout,
            }
        };
        match next {
            ReadTaskEvent::Registration(reg) => match reg {
                ReadTaskMessage::Lane { name, sender } => {
                    let id = next_id();
                    info!(
                        "Reading from new lane named '{}'. Assigned ID is {}.",
                        name, id
                    );
                    name_mapping.insert(name, id);
                    lanes.insert(id, sender);
                }
                ReadTaskMessage::Remote {
                    reader,
                    on_attached,
                } => {
                    info!("Reading from new remote endpoint.");
                    let rx = StopAfterError::new(remote_receiver(reader));
                    remotes.push(rx);
                    if let Some(on_attached) = on_attached {
                        on_attached.trigger();
                    }
                }
                ReadTaskMessage::Stop => break,
            },
            ReadTaskEvent::Envelope(msg) => {
                if voted {
                    trace!(ATTEMPTING_RESCIND);
                    if stop_vote.rescind() == VoteResult::Unanimous {
                        info!(STOP_VOTED);
                        break;
                    } else {
                        info!(STOP_RESCINDED);
                        voted = false;
                    }
                }
                debug!(message = ?msg, "Processing envelope.");
                let RequestMessage {
                    path,
                    origin,
                    envelope,
                } = msg;

                if let Some(id) = name_mapping.get(path.lane.as_str()) {
                    if matches!(&needs_flush, Some(i) if i != id) {
                        trace!(
                            "Flushing lane '{name}' (id = {id})",
                            name = path.lane,
                            id = id
                        );
                        flush_lane(&mut lanes, &mut needs_flush).await;
                    }
                    if let Some(lane_tx) = lanes.get_mut(id) {
                        let RelativeAddress { lane, .. } = path;
                        let origin: Uuid = origin;
                        match envelope {
                            Operation::Link => {
                                debug!(
                                    "Attempting to set up link to {} from lane '{}'.",
                                    origin, lane
                                );
                                if write_tx
                                    .send(WriteTaskMessage::Coord(RwCoordinationMessage::Link {
                                        origin,
                                        lane: Text::new(lane.as_str()),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    error!(TASK_COORD_ERR);
                                    break;
                                }
                            }
                            Operation::Sync => {
                                debug!(
                                    "Attempting to synchronize {} with lane '{}'.",
                                    origin, lane
                                );
                                if lane_tx.start_sync(origin).await.is_err() {
                                    error!(
                                        "Failed to communicate with lane '{}'. Removing handle.",
                                        lane
                                    );
                                    if let Some(id) = name_mapping.remove(lane.as_str()) {
                                        lanes.remove(&id);
                                    }
                                };
                            }
                            Operation::Command(body) => {
                                trace!(body = ?body, "Dispatching command envelope from {} to lane '{}'.", origin, lane);
                                if let Some(reporter) = &aggregate_reporter {
                                    reporter.count_commands(1);
                                }
                                match lane_tx.feed_frame(body).await {
                                    Err(LaneSendError::Io(_)) => {
                                        error!("Failed to communicate with lane '{}'. Removing handle.", lane);
                                        if let Some(id) = name_mapping.remove(lane.as_str()) {
                                            lanes.remove(&id);
                                        }
                                    }
                                    Err(LaneSendError::Extraction(error)) => {
                                        error!(error = ?error, "Received invalid envelope from {} for lane '{}'", origin, lane);
                                        if write_tx
                                            .send(WriteTaskMessage::Coord(
                                                RwCoordinationMessage::BadEnvelope {
                                                    origin,
                                                    lane: Text::new(lane.as_str()),
                                                    error,
                                                },
                                            ))
                                            .await
                                            .is_err()
                                        {
                                            error!(TASK_COORD_ERR);
                                            break;
                                        }
                                    }
                                    _ => {
                                        let _ = lane_tx.flush().await;
                                        needs_flush = Some(*id);
                                    }
                                }
                            }
                            Operation::Unlink => {
                                debug!(
                                    "Attempting to stop the link to {} from lane '{}'.",
                                    origin, lane
                                );
                                if write_tx
                                    .send(WriteTaskMessage::Coord(RwCoordinationMessage::Unlink {
                                        origin,
                                        lane: Text::new(lane.as_str()),
                                    }))
                                    .await
                                    .is_err()
                                {
                                    error!(TASK_COORD_ERR);
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    info!("Received envelope for non-existent lane '{}'.", path.lane);
                    let flush = flush_lane(&mut lanes, &mut needs_flush);
                    let result = if envelope.is_command() {
                        flush.await;
                        Ok(())
                    } else {
                        let send_err = write_tx.send(WriteTaskMessage::Coord(
                            RwCoordinationMessage::UnknownLane {
                                origin,
                                path: RelativeAddress::text(path.node.as_str(), path.lane.as_str()),
                            },
                        ));
                        join(flush, send_err).await.1
                    };
                    if result.is_err() {
                        error!(TASK_COORD_ERR);
                        break;
                    }
                }
            }
            ReadTaskEvent::Timeout => {
                info!(
                    "No envelopes received within {:?}. Voting to stop.",
                    config.inactive_timeout
                );
                if stop_vote.vote() == VoteResult::Unanimous {
                    info!(STOP_VOTED);
                    break;
                }
                voted = true;
            }
        }
    }
}

async fn flush_lane(lanes: &mut HashMap<u64, LaneSender>, needs_flush: &mut Option<u64>) {
    if let Some(id) = needs_flush.take() {
        if let Some(tx) = lanes.get_mut(&id) {
            if tx.flush().await.is_err() {
                lanes.remove(&id);
            }
        }
    }
}

/// Events that can occur in the write task.
#[derive(Debug)]
enum WriteTaskEvent<I> {
    /// A message received either from the read task or the coordination task.
    Message(WriteTaskMessage),
    /// An message received from one of the attached lanes or stores.
    Event(ItemResponse<I>),
    /// A write (to one of the attached remotes) completed.
    WriteDone(WriteResult),
    /// Reading from a lane failed.
    LaneFailed(u64),
    /// Reading from a store failed.
    StoreFailed(u64),
    /// A remote may have been without active links beyond the configured timeout.
    PruneRemote(Uuid),
    /// The task timed out due to inactivity.
    Timeout,
    /// The stop signal was received.
    Stop,
}

/// Parameters for the write task.
#[derive(Debug)]
struct WriteTaskConfiguration {
    identity: Uuid,
    node_uri: Text,
    runtime_config: AgentRuntimeConfig,
}

impl WriteTaskConfiguration {
    fn new(identity: Uuid, node_uri: Text, runtime_config: AgentRuntimeConfig) -> Self {
        WriteTaskConfiguration {
            identity,
            node_uri,
            runtime_config,
        }
    }
}

/// Manages the timeout for the write task. This can be disabled (to prevent it from firing repeatedly
/// after the task has alreay voted to stop).
#[derive(Debug)]
struct InactiveTimeout<'a> {
    timeout: Duration,
    timeout_delay: Pin<&'a mut Sleep>,
    enabled: bool,
}

pub enum ItemEndpoint {
    Lane { endpoint: LaneEndpoint<Io> },
    Store { endpoint: StoreEndpoint },
}

type InitResult<T> = Result<T, AgentItemInitError>;

type LaneResult<I> = InitResult<(LaneEndpoint<Io>, Option<I>)>;
type StoreResult<I> = InitResult<(StoreEndpoint, I)>;

type ItemInitTask<'a> = BoxFuture<'a, InitResult<ItemEndpoint>>;

/// Aggregates all of the streams of events for the write task.
#[derive(Debug)]
struct WriteTaskEvents<'a, S, W, I> {
    inactive_timeout: InactiveTimeout<'a>,
    remote_timeout: Duration,
    prune_remotes: PruneRemotes<'a>,
    message_stream: S,
    lanes_and_stores: SelectAll<StopAfterError<ResponseReceiver<I>>>,
    pending_writes: FuturesUnordered<W>,
}

impl<'a, S, W, I> WriteTaskEvents<'a, S, W, I>
where
    I: Unpin + Copy,
{
    /// # Arguments
    /// * `inactive_timeout` - Time after which the task will vote to stop due to inactivity.
    /// * `remote_timeout` - Time after which a task with no links and no activity should be removed.
    /// * `timeout_delay` - Timer for the agent timeout (held on the stack of the write task to avoid
    ///    having it in a separate allocation).
    /// * `prune_delay` - Timer for pruning inactive remotes (held on the stack of the write task to
    ///    avoid having it in a separte allocation).
    /// * `message_stream` - Stream of messages from the attachment and read tasks.
    fn new(
        inactive_timeout: Duration,
        remote_timeout: Duration,
        timeout_delay: Pin<&'a mut Sleep>,
        prune_delay: Pin<&'a mut Sleep>,
        message_stream: S,
    ) -> Self {
        WriteTaskEvents {
            inactive_timeout: InactiveTimeout {
                timeout: inactive_timeout,
                timeout_delay,
                enabled: true,
            },
            remote_timeout,
            prune_remotes: PruneRemotes::new(prune_delay),
            message_stream,

            lanes_and_stores: Default::default(),
            pending_writes: Default::default(),
        }
    }

    /// Add a new receiver to receive messages from a lane or store in the agent.
    fn add_receiver(&mut self, receiver: ResponseReceiver<I>) {
        self.lanes_and_stores.push(StopAfterError::new(receiver));
    }

    /// Remove (and destroy) all receivers from the agent.
    fn clear_lanes_and_stores(&mut self) {
        self.lanes_and_stores.clear();
    }

    /// Schedule a new write task (to a remote) to be polled.
    fn schedule_write(&mut self, write: W) {
        self.pending_writes.push(write);
    }

    /// Schedule a remote to be pruned (after a period of inactivity).
    fn schedule_prune(&mut self, remote_id: Uuid) {
        let WriteTaskEvents {
            remote_timeout,
            prune_remotes,
            ..
        } = self;
        prune_remotes.push(remote_id, *remote_timeout);
    }

    /// Disable the agent timeout (if the stop vote has been made and not yet rescinded).
    fn disable_timeout(&mut self) {
        self.inactive_timeout.enabled = false;
    }

    /// Reenable the agent timeout (the vote to stop has been rescinded).
    fn enable_timeout(&mut self) {
        self.inactive_timeout.enabled = true;
    }
}

impl<'a, S, W, I> WriteTaskEvents<'a, S, W, I>
where
    S: Stream<Item = WriteTaskMessage> + Unpin,
    W: Future<Output = WriteResult> + Send + 'static,
    I: Unpin + Copy,
{
    /// Select the next of any type of event. This is biased and will try to clear existing work
    /// before adding more work.
    async fn select_next(&mut self) -> WriteTaskEvent<I> {
        let WriteTaskEvents {
            inactive_timeout,
            message_stream,
            lanes_and_stores,
            pending_writes,
            prune_remotes,
            ..
        } = self;

        let InactiveTimeout {
            timeout,
            timeout_delay,
            enabled: timeout_enabled,
        } = inactive_timeout;

        let mut delay = timeout_delay.as_mut();

        loop {
            tokio::select! {
                biased;
                maybe_remote = prune_remotes.next(), if !prune_remotes.is_empty() => {
                    if let Some(remote_id) = maybe_remote {
                        break WriteTaskEvent::PruneRemote(remote_id);
                    }
                }
                maybe_msg = message_stream.next() => {
                    break if let Some(msg) = maybe_msg {
                        if msg.generates_activity() {
                            delay.as_mut().reset(
                                Instant::now()
                                    .checked_add(*timeout)
                                    .expect("Timer overflow."),
                            );
                        }
                        WriteTaskEvent::Message(msg)
                    } else {
                        trace!("Stopping as the coordination task stopped.");
                        WriteTaskEvent::Stop
                    };
                }
                maybe_write_done = pending_writes.next(), if !pending_writes.is_empty() => {
                    if let Some(result) = maybe_write_done {
                        break WriteTaskEvent::WriteDone(result);
                    }
                }
                maybe_result = lanes_and_stores.next(), if !lanes_and_stores.is_empty() => {
                    match maybe_result {
                        Some(Ok(response)) =>  {
                            if response.is_lane() {
                                delay.as_mut().reset(
                                    Instant::now()
                                        .checked_add(*timeout)
                                        .expect("Timer overflow."),
                                );
                            }
                            break WriteTaskEvent::Event(response);
                        },
                        Some(Err(Failed::Lane(item_id))) => {
                            break WriteTaskEvent::LaneFailed(item_id);
                        },
                        Some(Err(Failed::Store(item_id))) => {
                            break WriteTaskEvent::StoreFailed(item_id);
                        },
                        _ => {}
                    }
                },
                _ = &mut delay, if *timeout_enabled => {
                    break if lanes_and_stores.is_empty() {
                        trace!("Stopping as there are no active lanes.");
                        WriteTaskEvent::Stop
                    } else {
                        WriteTaskEvent::Timeout
                    };
                }
            };
        }
    }

    /// Select only from pending writes (used in the shutdown process).
    async fn next_write(&mut self) -> Option<WriteResult> {
        let WriteTaskEvents { pending_writes, .. } = self;
        if pending_writes.is_empty() {
            None
        } else {
            pending_writes.next().await
        }
    }
}

/// The internal state of the write task.
#[derive(Debug)]
struct WriteTaskState {
    /// Tracks links between lanes and remotes.
    links: Links,
    /// Manages writes to remotes (particularly backpressure relief).
    remote_tracker: RemoteTracker,
    store_counter: u64,
}

/// Possible results of handling a message from the coordination/read tasks.
#[derive(Debug)]
#[must_use]
enum TaskMessageResult<I> {
    /// Register a new lane.
    AddLane(LaneEndpoint<Io>, Option<I>),
    /// Register a new store.
    AddStore(StoreEndpoint, I),
    /// Schedule a write to one or all remotes (if no ID is specified).
    ScheduleWrite {
        write: WriteTask,
        schedule_prune: Option<Uuid>,
    },
    /// Track a remote to be pruned after the configured timeout (as it no longer has any links).
    AddPruneTimeout(Uuid),
    /// Initializing a lane from the store failed.
    StoreInitFailure(AgentItemInitError),
    /// No effect.
    Nothing,
    Stop,
}

impl<I> From<Option<WriteTask>> for TaskMessageResult<I> {
    fn from(opt: Option<WriteTask>) -> Self {
        if let Some(write) = opt {
            TaskMessageResult::ScheduleWrite {
                write,
                schedule_prune: None,
            }
        } else {
            TaskMessageResult::Nothing
        }
    }
}

fn discard_error<W>(error: InvalidKey) -> Option<W> {
    warn!("Discarding invalid map lane event: {}.", error);
    None
}

/// Sequence of writes with 0, 1 or 2 entries.
#[derive(Default)]
enum Writes<W> {
    #[default]
    Zero,
    Single(W),
    Two(W, W),
}

impl<W> From<Option<W>> for Writes<W> {
    fn from(opt: Option<W>) -> Self {
        match opt {
            Some(w) => Writes::Single(w),
            _ => Writes::Zero,
        }
    }
}

impl<W> From<(Option<W>, Option<W>)> for Writes<W> {
    fn from(pair: (Option<W>, Option<W>)) -> Self {
        match pair {
            (Some(w1), Some(w2)) => Writes::Two(w1, w2),
            (Some(w), _) => Writes::Single(w),
            (_, Some(w)) => Writes::Single(w),
            _ => Writes::Zero,
        }
    }
}

impl<W> Iterator for Writes<W> {
    type Item = W;

    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::take(self) {
            Writes::Two(w1, w2) => {
                *self = Writes::Single(w2);
                Some(w1)
            }
            Writes::Single(w) => Some(w),
            _ => None,
        }
    }
}

impl WriteTaskState {
    fn new(identity: Uuid, node_uri: Text, aggregate_reporter: Option<UplinkReporter>) -> Self {
        WriteTaskState {
            links: Links::new(aggregate_reporter),
            remote_tracker: RemoteTracker::new(identity, node_uri),
            store_counter: 0,
        }
    }

    /// Register a new lane with the state, assigning it a unique ID.
    fn register_lane(&mut self, name: Text, reporter: Option<UplinkReporter>) -> u64 {
        let WriteTaskState {
            links,
            remote_tracker,
            ..
        } = self;
        let lane_id = remote_tracker.lane_registry().add_endpoint(name);
        if let Some(reporter) = reporter {
            links.register_reporter(lane_id, reporter);
        }
        lane_id
    }

    /// Register a new store with the state, assigning it a unique ID.
    fn item_id_for_store(&mut self) -> u64 {
        let id = self.store_counter;
        self.store_counter += 1;
        id
    }

    /// Handle a message from the coordination or read task.
    async fn handle_task_message<'a, Store>(
        &mut self,
        reg: WriteTaskMessage,
        initialization: &'a Initialization,
        store: &'a Store,
    ) -> TaskMessageResult<Store::StoreId>
    where
        Store: AgentPersistence + Send + Sync,
    {
        let WriteTaskState {
            links,
            remote_tracker,
            ..
        } = self;
        match reg {
            WriteTaskMessage::Lane(LaneRuntimeSpec {
                name,
                kind,
                config,
                promise,
            }) => {
                info!("Registering a new {} lane with name {}.", kind, name);
                match initialization.add_lane(store, name, kind, config, promise) {
                    Some(fut) => match fut.await {
                        Ok((endpoint, store_id)) => TaskMessageResult::AddLane(endpoint, store_id),
                        Err(err) => TaskMessageResult::StoreInitFailure(err),
                    },
                    _ => TaskMessageResult::Nothing,
                }
            }
            WriteTaskMessage::Store(StoreRuntimeSpec {
                name,
                kind,
                config,
                promise,
            }) => {
                info!("Registering a new {} store with name {}.", kind, name);
                match initialization.add_store(store, name, kind, config, promise) {
                    Ok(Some(fut)) => match fut.await {
                        Ok((endpoint, store_id)) => TaskMessageResult::AddStore(endpoint, store_id),
                        Err(err) => TaskMessageResult::StoreInitFailure(err),
                    },
                    Err(err) => TaskMessageResult::StoreInitFailure(err),
                    _ => TaskMessageResult::Nothing,
                }
            }
            WriteTaskMessage::Remote {
                id,
                writer,
                completion,
                on_attached,
            } => {
                remote_tracker.insert(id, writer, completion);
                if let Some(on_attached) = on_attached {
                    on_attached.trigger();
                }
                TaskMessageResult::AddPruneTimeout(id)
            }
            WriteTaskMessage::Coord(RwCoordinationMessage::Link { origin, lane }) => {
                info!("Attempting to set up link from '{}' to {}.", lane, origin);
                match remote_tracker.lane_registry().id_for(lane.as_str()) {
                    Some(id) if remote_tracker.has_remote(origin) => {
                        links.insert(id, origin);
                        remote_tracker
                            .push_special(SpecialAction::Linked(id), &origin)
                            .into()
                    }
                    Some(_) => {
                        error!("No remote with ID {}.", origin);
                        TaskMessageResult::Nothing
                    }
                    _ => {
                        if remote_tracker.has_remote(origin) {
                            error!("No lane named '{}'.", lane);
                        } else {
                            error!("No lane named '{}' or remote with ID {}.", lane, origin);
                        }
                        TaskMessageResult::Nothing
                    }
                }
            }
            WriteTaskMessage::Coord(RwCoordinationMessage::Unlink { origin, lane }) => {
                info!(
                    "Attempting to close any link from '{}' to {}.",
                    lane, origin
                );
                if let Some(lane_id) = remote_tracker.lane_registry().id_for(lane.as_str()) {
                    if links.is_linked(origin, lane_id) {
                        let schedule_prune = links.remove(lane_id, origin).into_option();
                        let message = Text::new("\"Link closed.\"");
                        let maybe_write = remote_tracker
                            .push_special(SpecialAction::unlinked(lane_id, message), &origin);
                        if let Some(write) = maybe_write {
                            TaskMessageResult::ScheduleWrite {
                                write,
                                schedule_prune,
                            }
                        } else if let Some(remote_id) = schedule_prune {
                            TaskMessageResult::AddPruneTimeout(remote_id)
                        } else {
                            TaskMessageResult::Nothing
                        }
                    } else {
                        info!("Lane {} is not linked to {}.", lane, origin);
                        TaskMessageResult::Nothing
                    }
                } else {
                    error!("No lane named '{}'.", lane);
                    TaskMessageResult::Nothing
                }
            }
            WriteTaskMessage::Coord(RwCoordinationMessage::UnknownLane { origin, path }) => {
                info!(
                    "Received envelope for non-existent lane '{}' from {}.",
                    path.lane, origin
                );
                remote_tracker
                    .push_special(SpecialAction::lane_not_found(path.lane), &origin)
                    .into()
            }
            WriteTaskMessage::Coord(RwCoordinationMessage::BadEnvelope {
                origin,
                lane,
                error,
            }) => {
                info!(error = ?error, "Received in invalid envelope for lane '{}' from {}.", lane, origin);
                TaskMessageResult::Nothing
            }
            WriteTaskMessage::Stop => TaskMessageResult::Stop,
        }
    }

    /// Handle a message from one of the lanes of the agent.
    fn handle_event(
        &mut self,
        id: u64,
        response: LaneData,
    ) -> impl Iterator<Item = WriteTask> + '_ {
        let WriteTaskState {
            links,
            remote_tracker: write_tracker,
            ..
        } = self;

        use either::Either;

        let LaneData { target, response } = response;
        if let Some(remote_id) = target {
            trace!(response = ?response, "Routing response to {}.", remote_id);
            links.count_single(id);
            let write = if !links.is_linked(remote_id, id) {
                trace!(response = ?response, "Sending implicit linked message to {}.", remote_id);
                links.insert(id, remote_id);
                let write1 = write_tracker.push_special(SpecialAction::Linked(id), &remote_id);
                let write2 = write_tracker
                    .push_write(id, response, &remote_id)
                    .unwrap_or_else(discard_error);
                Writes::from((write1, write2))
            } else {
                Writes::from(
                    write_tracker
                        .push_write(id, response, &remote_id)
                        .unwrap_or_else(discard_error),
                )
            };
            Either::Left(write)
        } else if let Some(targets) = links.linked_from(id) {
            trace!(response = ?response, targets = ?targets, "Broadcasting response to all linked remotes.");
            links.count_broadcast(id);
            Either::Right(targets.iter().zip(std::iter::repeat(response)).flat_map(
                move |(remote_id, response)| {
                    write_tracker
                        .push_write(id, response, remote_id)
                        .unwrap_or_else(discard_error)
                },
            ))
        } else {
            trace!(response = ?response, id, "Discarding response.");
            Either::Left(Writes::Zero)
        }
    }

    /// Remove a registered remote.
    fn remove_remote(&mut self, remote_id: Uuid, reason: DisconnectionReason) {
        info!("Removing remote connection {}.", remote_id);
        self.links.remove_remote(remote_id);
        self.remote_tracker.remove_remote(remote_id, reason);
    }

    /// Remove a registered remote only if it has no links.
    fn remove_remote_if_idle(&mut self, remote_id: Uuid) {
        if self.links.linked_to(remote_id).is_none() {
            self.remove_remote(remote_id, DisconnectionReason::RemoteTimedOut);
        }
    }

    /// Remove a failed lane.
    fn remove_lane(
        &mut self,
        lane_id: u64,
    ) -> impl Iterator<Item = (TriggerUnlink, Option<WriteTask>)> + '_ {
        let WriteTaskState {
            links,
            remote_tracker: write_tracker,
            ..
        } = self;
        info!("Attempting to remove lane with id {}.", lane_id);
        let linked_remotes = links.remove_lane(lane_id);
        linked_remotes.into_iter().map(move |unlink| {
            let TriggerUnlink { remote_id, .. } = unlink;
            info!(
                "Unlinking remote {} connected to lane with id {}.",
                remote_id, lane_id
            );
            let task = write_tracker.unlink_lane(remote_id, lane_id);
            (unlink, task)
        })
    }

    /// Return the remote sender and associated buffer from a completed write. This will
    /// trigger a new task if more work is queued fro that remote.
    fn replace(&mut self, writer: RemoteSender, buffer: BytesMut) -> Option<WriteTask> {
        let WriteTaskState { remote_tracker, .. } = self;
        trace!(
            "Replacing writer {} after completed write.",
            writer.remote_id()
        );
        remote_tracker.replace_and_pop(writer, buffer)
    }

    fn has_remotes(&self) -> bool {
        !self.remote_tracker.is_empty()
    }

    /// Unlink all open links.
    fn unlink_all(&mut self) -> impl Iterator<Item = WriteTask> + '_ {
        info!("Unlinking all open links for shutdown.");
        let WriteTaskState {
            links,
            remote_tracker,
            ..
        } = self;
        links
            .remove_all_links()
            .flat_map(move |(lane_id, remote_id)| remote_tracker.unlink_lane(remote_id, lane_id))
    }

    /// Close all open remotes with the reason the agent is stopping.
    fn dispose_of_remotes(self, reason: DisconnectionReason) {
        let WriteTaskState { remote_tracker, .. } = self;
        remote_tracker.dispose_of_remotes(reason);
    }
}

#[derive(Debug)]
struct WriteTaskEndpoints {
    lane_endpoints: Vec<LaneEndpoint<ByteReader>>,
    store_endpoints: Vec<StoreEndpoint>,
}

impl WriteTaskEndpoints {
    fn new(
        lane_endpoints: Vec<LaneEndpoint<ByteReader>>,
        store_endpoints: Vec<StoreEndpoint>,
    ) -> Self {
        WriteTaskEndpoints {
            lane_endpoints,
            store_endpoints,
        }
    }
}

/// The write task of the agent runtime. This receives messages from the agent lanes and forwards them
/// to linked remotes. It also receives messages from the read task to maintain the set of uplinks.
///
/// # Arguments
/// * `configuration` - Configuration parameters for the task.
/// * `initial_endpoints` - Initial lane and store endpoints that were created in the agent initialization phase.
/// * `message_stream` - Channel for messages from the read and coordination tasks. This will terminate when the agent
///    runtime is stopping.
/// * `read_task_tx` - Channel to communicate with the read task (after initializing new lanes).
/// * `stop_voter` - Votes to stop if this task becomes inactive (unanimity with the write task is required).
/// * `reporting` - Introspection reporting context for the agent (if introspection is enabled).
/// * `store` - Persistence for the state of the lanes.
async fn write_task<Msg, Store>(
    configuration: WriteTaskConfiguration,
    initial_endpoints: WriteTaskEndpoints,
    message_stream: Msg,
    read_task_tx: mpsc::Sender<ReadTaskMessage>,
    stop_voter: timeout_coord::Voter,
    reporting: Option<NodeReporting>,
    mut store: Store,
) -> Result<(), StoreError>
where
    Msg: Stream<Item = WriteTaskMessage> + Send + Unpin,
    Store: AgentPersistence + Send + Sync,
{
    let aggregate_reporter = reporting.as_ref().map(NodeReporting::aggregate);

    let WriteTaskConfiguration {
        identity,
        node_uri,
        runtime_config,
    } = configuration;

    let initialization = Initialization::new(reporting, runtime_config.item_init_timeout);

    let mut timeout_delay = pin!(sleep(runtime_config.inactive_timeout));
    let remote_prune_delay = pin!(sleep(Duration::default()));

    let mut streams = WriteTaskEvents::new(
        runtime_config.inactive_timeout,
        runtime_config.prune_remote_delay,
        timeout_delay.as_mut(),
        remote_prune_delay,
        message_stream,
    );
    let mut state = WriteTaskState::new(identity, node_uri, aggregate_reporter);

    info!(endpoints = ?initial_endpoints, "Adding initial endpoints.");

    let WriteTaskEndpoints {
        lane_endpoints,
        store_endpoints,
    } = initial_endpoints;

    for endpoint in lane_endpoints {
        let store_id = if endpoint.transient {
            None
        } else {
            match store.store_id(endpoint.name.as_str()) {
                Ok(id) => Some(id),
                Err(StoreError::NoStoreAvailable) => None,
                Err(err) => return Err(err),
            }
        };
        let lane_stream = endpoint.into_lane_stream(store_id, &mut state);
        streams.add_receiver(lane_stream);
    }

    for endpoint in store_endpoints.into_iter() {
        let store_id = store.store_id(endpoint.name.as_str())?;
        let store_stream = endpoint.into_store_stream(store_id, &mut state);
        streams.add_receiver(store_stream);
    }

    let mut voted = false;

    let mut remote_reason = DisconnectionReason::AgentStoppedExternally;

    loop {
        let next = streams.select_next().await;
        trace!(event = ?next, "Processing write task event");
        match next {
            WriteTaskEvent::Message(reg) => match state
                .handle_task_message(reg, &initialization, &store)
                .await
            {
                TaskMessageResult::AddLane(lane, store_id) => {
                    let (read_endpoint, write_endpoint) = lane.split();
                    streams.add_receiver(write_endpoint.into_lane_stream(store_id, &mut state));
                    if read_task_tx
                        .send(read_endpoint.into_read_task_message())
                        .await
                        .is_err()
                    {
                        error!("Could not communicate with read task.");
                        break;
                    }
                }
                TaskMessageResult::AddStore(store, store_id) => {
                    streams.add_receiver(store.into_store_stream(store_id, &mut state));
                }
                TaskMessageResult::ScheduleWrite {
                    write,
                    schedule_prune,
                } => {
                    if voted {
                        trace!(ATTEMPTING_RESCIND);
                        if stop_voter.rescind() == VoteResult::Unanimous {
                            info!(STOP_VOTED);
                            remote_reason = DisconnectionReason::AgentTimedOut;
                            break;
                        } else {
                            info!(STOP_RESCINDED);
                        }
                        streams.enable_timeout();
                        voted = false;
                    }
                    streams.schedule_write(write.into_future());
                    if let Some(remote_id) = schedule_prune {
                        streams.schedule_prune(remote_id);
                    }
                }
                TaskMessageResult::AddPruneTimeout(remote_id) => {
                    streams.schedule_prune(remote_id);
                }
                TaskMessageResult::StoreInitFailure(error) => {
                    let AgentItemInitError { name, source } = error;
                    error!(error = %source, "Initializing a store for {} failed.", name);
                    if let StoreInitError::Store(err) = source {
                        return Err(err);
                    }
                }
                TaskMessageResult::Nothing => {}
                TaskMessageResult::Stop => break,
            },
            WriteTaskEvent::Event(response) => {
                if response.is_lane() && voted {
                    trace!(ATTEMPTING_RESCIND);
                    if stop_voter.rescind() == VoteResult::Unanimous {
                        info!(STOP_VOTED);
                        remote_reason = DisconnectionReason::AgentTimedOut;
                        break;
                    } else {
                        info!(STOP_RESCINDED);
                    }
                    streams.enable_timeout();
                    voted = false;
                }
                persist_response(&mut store, &response)?;
                if let Some((item_id, response)) = response.into_uplink_response() {
                    for write in state.handle_event(item_id, response) {
                        streams.schedule_write(write.into_future());
                    }
                }
            }
            WriteTaskEvent::WriteDone((writer, buffer, Ok(_))) => {
                if let Some(write) = state.replace(writer, buffer) {
                    streams.schedule_write(write.into_future());
                }
            }
            WriteTaskEvent::WriteDone((writer, _, Err(err))) => {
                let remote_id = writer.remote_id();
                info!(
                    error = %err,
                    "Writing to remote {} failed. Removing attached uplinks.",
                    remote_id
                );
                state.remove_remote(remote_id, DisconnectionReason::ChannelClosed);
            }
            WriteTaskEvent::LaneFailed(lane_id) => {
                error!(
                    "Lane with ID {} failed. Unlinking all attached uplinks.",
                    lane_id
                );
                for (unlink, maybe_write) in state.remove_lane(lane_id) {
                    if let Some(write) = maybe_write {
                        streams.schedule_write(write.into_future());
                    }
                    let TriggerUnlink {
                        remote_id,
                        schedule_prune,
                    } = unlink;
                    if schedule_prune {
                        streams.schedule_prune(remote_id);
                    }
                }
            }
            WriteTaskEvent::StoreFailed(item_id) => {
                error!("Store with ID {} failed.", item_id);
            }
            WriteTaskEvent::PruneRemote(remote_id) => {
                state.remove_remote_if_idle(remote_id);
            }
            WriteTaskEvent::Timeout => {
                info!(
                    "No events sent within {:?}, voting to stop.",
                    runtime_config.inactive_timeout
                );
                if !state.has_remotes() {
                    info!("Stopping after timeout with no remotes.");
                    break;
                }
                voted = true;
                streams.disable_timeout();
                if stop_voter.vote() == VoteResult::Unanimous {
                    info!(STOP_VOTED);
                    remote_reason = DisconnectionReason::AgentTimedOut;
                    break;
                }
            }
            WriteTaskEvent::Stop => {
                info!("Write task stopping.");
                break;
            }
        }
    }
    let cleanup_result = timeout(runtime_config.shutdown_timeout, async move {
        info!("Unlinking all links on shutdown.");
        streams.clear_lanes_and_stores();
        for write in state.unlink_all() {
            streams.schedule_write(write.into_future());
        }
        while let Some((writer, buffer, result)) = streams.next_write().await {
            if result.is_ok() {
                if let Some(write) = state.replace(writer, buffer) {
                    streams.schedule_write(write.into_future());
                }
            }
        }
        state.dispose_of_remotes(remote_reason);
    })
    .await;

    if cleanup_result.is_err() {
        error!(
            "Unlinking lanes on shutdown did not complete within {:?}.",
            runtime_config.shutdown_timeout
        );
    }
    Ok(())
}

async fn await_io_tasks<F1, F2>(
    read: F1,
    write: F2,
    kill_switch_tx: trigger::Sender,
) -> Result<(), StoreError>
where
    F1: Future<Output = ()>,
    F2: Future<Output = Result<(), StoreError>>,
{
    let read = pin!(read);
    let write = pin!(write);
    let first_finished = select(read, write).await;
    kill_switch_tx.trigger();
    match first_finished {
        Either::Left((_, write_fut)) => write_fut.await,
        Either::Right((result, read_fut)) => {
            read_fut.await;
            result
        }
    }
}

fn persist_response<Store>(
    store: &mut Store,
    response: &ItemResponse<Store::StoreId>,
) -> Result<(), StoreError>
where
    Store: AgentPersistence,
{
    let ItemResponse { store_id, body, .. } = response;
    if let Some(store_id) = store_id {
        match body {
            ResponseData::Lane(LaneData {
                response: UplinkResponse::Value(body),
                ..
            })
            | ResponseData::Lane(LaneData {
                response: UplinkResponse::Supply(body),
                ..
            })
            | ResponseData::Store(StoreData::Value(body)) => {
                store.put_value(*store_id, body.as_ref())
            }
            ResponseData::Lane(LaneData {
                response: UplinkResponse::Map(body),
                ..
            })
            | ResponseData::Store(StoreData::Map(body)) => store.apply_map(*store_id, body),
            _ => Ok(()),
        }
    } else {
        Ok(())
    }
}

/// Events that can occur in the HTTP task.
enum HttpTaskEvent {
    /// Request to register a new HTTP lane.
    Registration(HttpLaneRuntimeSpec),
    /// An incoming HTTP request to be routed to a lane.
    Request(HttpLaneRequest),
    /// Time-out after new requests received.
    Timeout,
}

/// A task that routes incoming HTTP requests to the HTTP lanes of the agent.
///
/// # Arguments
/// * `stopping` - A signal that the agent is stopping and this task should stop immediately.
/// * `config` - Configuration parameters for the agent runtime.
/// * `requests` - Incoming HTTP requests.
/// * `initial_endpoints` - HTTP lanes that were registered during agent initialization.
/// * `registrations` - Requests to register new HTTP lanes.
/// * `stop_voter` - Used by this task to vote for the agent to stop after a period of inactivity.
async fn http_task(
    mut stopping: trigger::Receiver,
    config: AgentRuntimeConfig,
    mut requests: mpsc::Receiver<HttpLaneRequest>,
    initial_endpoints: Vec<HttpLaneEndpoint>,
    mut registrations: mpsc::Receiver<HttpLaneRuntimeSpec>,
    stop_voter: timeout_coord::Voter,
) {
    let mut endpoints: HashMap<_, _> = initial_endpoints
        .into_iter()
        .map(
            |HttpLaneEndpoint {
                 name,
                 request_sender,
             }| (name, request_sender),
        )
        .collect();

    let mut voted = false;

    loop {
        let event = tokio::select! {
            _ = &mut stopping => break,
            maybe_reg = registrations.recv() => {
                if let Some(reg) = maybe_reg {
                    HttpTaskEvent::Registration(reg)
                } else {
                    break;
                }
            },
            result = tokio::time::timeout(config.inactive_timeout, requests.recv()) => {
                match result {
                    Ok(Some(req)) => HttpTaskEvent::Request(req),
                    Err(_) => HttpTaskEvent::Timeout,
                    _ => break,
                }
            }
        };
        match event {
            HttpTaskEvent::Registration(HttpLaneRuntimeSpec { name, promise }) => {
                let (tx, rx) = mpsc::channel(config.lane_http_request_channel_size.get());
                if promise.send(Ok(rx)).is_err() {
                    error!(name = %name, "Request for a new HTTP lane was dropped before it was completed.");
                } else {
                    endpoints.insert(name, tx);
                }
            }
            HttpTaskEvent::Request(request) => {
                if voted {
                    trace!(ATTEMPTING_RESCIND);
                    match stop_voter.rescind() {
                        VoteResult::Unanimous => {
                            info!(STOP_VOTED);
                            break;
                        }
                        VoteResult::UnanimityPending => {
                            info!(STOP_RESCINDED);
                            voted = false;
                        }
                    }
                }
                let extracted_name = uri_params::extract_lane(&request.request.uri);
                if let Some((lane_name, tx)) = extracted_name.as_ref().and_then(|lane_name| {
                    endpoints
                        .get(lane_name.as_ref())
                        .map(move |tx| (lane_name, tx))
                }) {
                    match tx.reserve().await {
                        Ok(res) => res.send(request),
                        err => {
                            drop(err); //Surprisingly, this is needed. Binding to _ does not pass the borrow checker.
                            endpoints.remove(lane_name.as_ref());
                        }
                    }
                } else {
                    let name = extracted_name.map(|s| s.to_string());
                    let (_, response_tx) = request.into_parts();
                    not_found(name.as_deref(), response_tx);
                }
            }
            HttpTaskEvent::Timeout => {
                if !voted {
                    info!(
                        "No HTTP requests received within {:?}. Voting to stop.",
                        config.inactive_timeout
                    );
                    if stop_voter.vote() == VoteResult::Unanimous {
                        info!(STOP_VOTED);
                        break;
                    }
                    voted = true;
                }
            }
        }
    }
}

/// Send a 404 if the target lane for an HTTP request does not exist.
fn not_found(lane_name: Option<&str>, response_tx: HttpResponseSender) {
    let payload = if let Some(name) = lane_name {
        Bytes::from(format!(
            "This agent does not have an HTTP lane called `{}`",
            name
        ))
    } else {
        Bytes::from_static(b"No lane name was specified.")
    };
    let content_len = Header::new(StandardHeaderName::ContentLength, payload.len().to_string());
    let not_found_response = HttpResponse {
        status_code: StatusCode::NOT_FOUND,
        version: Version::HTTP_1_1,
        headers: vec![content_len],
        payload,
    };
    if response_tx.send(not_found_response).is_err() {
        error!("HTTP connection was terminated before the response cound be sent.");
    }
}
