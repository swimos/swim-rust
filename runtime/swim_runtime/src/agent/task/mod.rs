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

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::agent::task::links::TriggerUnlink;
use crate::agent::task::sender::LaneSendError;
use crate::agent::task::timeout_coord::VoteResult;
use crate::agent::task::write_fut::SpecialAction;
use crate::error::InvalidKey;

use self::links::Links;
use self::prune::PruneRemotes;
use self::remotes::{RemoteSender, RemoteTracker, UplinkResponse};
use self::sender::LaneSender;
use self::write_fut::{WriteResult, WriteTask};

use super::reporting::UplinkReporter;
use super::store::AgentPersistence;
use super::{
    AgentAttachmentRequest, AgentRuntimeConfig, DisconnectionReason, DownlinkRequest, Io,
    NodeReporting,
};
use bytes::{Bytes, BytesMut};
use futures::{ready, pin_mut};
use futures::stream::FuturesUnordered;
use futures::{
    future::{join, select as fselect, Either},
    stream::{select as sselect, SelectAll},
    Stream, StreamExt,
};
use swim_api::agent::LaneConfig;
use swim_api::error::StoreError;
use swim_api::meta::lane::LaneKind;
use swim_api::protocol::agent::{LaneResponse, MapLaneResponse};
use swim_api::store::StoreDisabled;
use swim_api::{
    agent::UplinkKind,
    error::AgentRuntimeError,
    protocol::agent::{MapLaneResponseDecoder, ValueLaneResponseDecoder},
};
use swim_messages::bytes_str::BytesStr;
use swim_messages::protocol::{Operation, Path, RawRequestMessageDecoder, RequestMessage};
use swim_model::Text;
use swim_recon::parser::MessageExtractError;
use swim_utilities::future::{immediate_or_join, StopAfterError};
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swim_utilities::trigger::{self, promise};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout, Instant, Sleep};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use tracing::{debug, error, info, info_span, trace, warn};
use tracing_futures::Instrument;

mod init;
mod links;
mod prune;
mod remotes;
mod sender;
mod timeout_coord;
mod write_fut;

pub use init::AgentInitTask;

#[cfg(test)]
mod fake_store;
#[cfg(test)]
mod tests;

/// Type for requests that can be sent to the agent runtime task by an agent implementation.
#[derive(Debug)]
pub enum AgentRuntimeRequest {
    /// Attempt to open a new lane for the agent.
    AddLane {
        name: Text,
        kind: LaneKind,
        config: LaneConfig,
        promise: oneshot::Sender<Result<Io, AgentRuntimeError>>,
    },
    /// Attempt to open a downlink to a lane on another agent.
    OpenDownlink(DownlinkRequest),
}

/// A labelled channel endpoint (or pair) for a lane.
#[derive(Debug)]
struct LaneEndpoint<T> {
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
    /// #Arguments
    /// * `registry` - The registry that assigns identifiiers to active lanes.
    fn into_lane_stream<Store>(
        self,
        store: &Store,
        state: &mut WriteTaskState,
    ) -> Result<LaneStream<Store::LaneId>, StoreError>
    where
        Store: AgentPersistence,
    {
        let LaneEndpoint {
            name,
            kind,
            transient,
            io: reader,
            reporter,
        } = self;
        let store_id = if transient {
            None
        } else {
            Some(store.lane_id(name.as_str())?)
        };
        let id = state.register(name, reporter);
        Ok(match kind {
            UplinkKind::Value => {
                let receiver = ValueLikeLaneReceiver::value(id, store_id, reader);
                StopAfterError::new(Either::Left(receiver))
            }
            UplinkKind::Supply => {
                let receiver = ValueLikeLaneReceiver::supply(id, store_id, reader);
                StopAfterError::new(Either::Left(receiver))
            }
            UplinkKind::Map => {
                let receiver = MapLaneReceiver::new(id, store_id, reader);
                StopAfterError::new(Either::Right(receiver))
            }
        })
    }
}

/// Result of the agent initialization task (detailing the lanes that were created during initialization).
#[derive(Debug)]
pub struct InitialEndpoints {
    reporting: Option<NodeReporting>,
    rx: mpsc::Receiver<AgentRuntimeRequest>,
    endpoints: Vec<LaneEndpoint<Io>>,
}

impl InitialEndpoints {
    fn new(
        reporting: Option<NodeReporting>,
        rx: mpsc::Receiver<AgentRuntimeRequest>,
        endpoints: Vec<LaneEndpoint<Io>>,
    ) -> Self {
        InitialEndpoints {
            reporting,
            rx,
            endpoints,
        }
    }

    /// Create the agent runtime task based on these intial lane endpoints.
    /// #Arguments
    /// * `identity` The routing ID of this agent instance for outgoing envelopes.
    /// * `node_uri` - The node URI of this agent instance for outgoing envelopes.
    /// * `attachment_rx` - Channel to accept requests to attach remote connections to the agent.
    /// * `config` - Configuration parameters for the agent runtime.
    /// * `stopping` - A signal for initiating a clean shutdown for the agent instance.
    pub fn make_runtime_task(
        self,
        identity: Uuid,
        node_uri: Text,
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        config: AgentRuntimeConfig,
        stopping: trigger::Receiver,
    ) -> AgentRuntimeTask {
        AgentRuntimeTask::new(
            NodeDescriptor::new(identity, node_uri),
            self,
            attachment_rx,
            stopping,
            config,
        )
    }

    /// Create the agent runtime task based on these intial lane endpoints.
    /// #Arguments
    /// * `identity` The routing ID of this agent instance for outgoing envelopes.
    /// * `node_uri` - The node URI of this agent instance for outgoing envelopes.
    /// * `attachment_rx` - Channel to accept requests to attach remote connections to the agent.
    /// * `config` - Configuration parameters for the agent runtime.
    /// * `stopping` - A signal for initiating a clean shutdown for the agent instance.
    /// * `store` - Persistence store for the agent.
    pub fn make_runtime_task_with_store<Store>(
        self,
        identity: Uuid,
        node_uri: Text,
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        config: AgentRuntimeConfig,
        stopping: trigger::Receiver,
        store: Store,
    ) -> AgentRuntimeTask<Store>
    where
        Store: AgentPersistence + Clone + Send + Sync + 'static,
    {
        AgentRuntimeTask::with_store(
            NodeDescriptor::new(identity, node_uri),
            self,
            attachment_rx,
            stopping,
            config,
            store,
        )
    }
}

#[derive(Debug)]
struct NodeDescriptor {
    identity: Uuid,
    node_uri: Text,
}

impl NodeDescriptor {
    fn new(identity: Uuid, node_uri: Text) -> Self {
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
    stopping: trigger::Receiver,
    config: AgentRuntimeConfig,
    store: Store,
}

/// Message type used by the read and write tasks to communicate with each other.
#[derive(Debug, Clone)]
enum RwCoorindationMessage {
    /// An envelope was received for an unknown lane (and so the write task should issue an appropriate error response).
    UnknownLane { origin: Uuid, path: Path<Text> },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueOrSupply {
    Value,
    Supply,
}

/// Recives a stream of messages sent by a lane.
#[derive(Debug)]
struct ValueLikeLaneReceiver<I> {
    lane_id: u64,
    store_id: Option<I>,
    kind: ValueOrSupply,
    receiver: FramedRead<ByteReader, ValueLaneResponseDecoder>,
}

/// Recives a stream of messages sent by a lane.
#[derive(Debug)]
struct MapLaneReceiver<I> {
    lane_id: u64,
    store_id: Option<I>,
    receiver: FramedRead<ByteReader, MapLaneResponseDecoder>,
}

/// Unified response type from all lane types.
#[derive(Debug)]
struct RawLaneResponse {
    /// A specific remote to forward the message to (otherwise broadcast to all linked).
    target: Option<Uuid>,
    /// The response to send to the specified remote.
    response: UplinkResponse,
}

impl RawLaneResponse {
    /// Target a response at a specific remote.
    /// #Arguments
    /// * `id` - The target remote.
    /// * `response` - The message to send.
    pub fn targetted(id: Uuid, response: UplinkResponse) -> Self {
        RawLaneResponse {
            target: Some(id),
            response,
        }
    }

    /// Broadcast a response to all linked remotes.
    /// #Arguments
    /// * `response` = The message to send.
    pub fn broadcast(response: UplinkResponse) -> Self {
        RawLaneResponse {
            target: None,
            response,
        }
    }
}

fn value_or_supply_raw_response(
    resp: LaneResponse<BytesMut>,
    kind: ValueOrSupply,
) -> Option<RawLaneResponse> {
    match resp {
        LaneResponse::StandardEvent(body) => Some(RawLaneResponse::broadcast(match kind {
            ValueOrSupply::Value => UplinkResponse::Value(body.freeze()),
            ValueOrSupply::Supply => UplinkResponse::Supply(body.freeze()),
        })),
        LaneResponse::Initialized => None,
        LaneResponse::SyncEvent(id, body) => Some(RawLaneResponse::targetted(
            id,
            UplinkResponse::Value(body.freeze()),
        )),
        LaneResponse::Synced(id) => Some(RawLaneResponse::targetted(
            id,
            UplinkResponse::Synced(UplinkKind::Value),
        )),
    }
}

fn map_raw_response(resp: MapLaneResponse<BytesMut, BytesMut>) -> Option<RawLaneResponse> {
    match resp {
        LaneResponse::StandardEvent(body) => {
            Some(RawLaneResponse::broadcast(UplinkResponse::Map(body)))
        }
        LaneResponse::Initialized => None,
        LaneResponse::SyncEvent(id, body) => {
            Some(RawLaneResponse::targetted(id, UplinkResponse::Map(body)))
        }
        LaneResponse::Synced(id) => Some(RawLaneResponse::targetted(
            id,
            UplinkResponse::Synced(UplinkKind::Map),
        )),
    }
}

impl<I> ValueLikeLaneReceiver<I> {
    fn value(lane_id: u64, store_id: Option<I>, reader: ByteReader) -> Self {
        ValueLikeLaneReceiver {
            lane_id,
            store_id,
            kind: ValueOrSupply::Value,
            receiver: FramedRead::new(reader, Default::default()),
        }
    }

    fn supply(lane_id: u64, store_id: Option<I>, reader: ByteReader) -> Self {
        ValueLikeLaneReceiver {
            lane_id,
            store_id,
            kind: ValueOrSupply::Supply,
            receiver: FramedRead::new(reader, Default::default()),
        }
    }
}

impl<I> MapLaneReceiver<I> {
    fn new(lane_id: u64, store_id: Option<I>, reader: ByteReader) -> Self {
        MapLaneReceiver {
            lane_id,
            store_id,
            receiver: FramedRead::new(reader, Default::default()),
        }
    }
}

/// Error type indicating that a lane has failed (specifying its ID).
#[derive(Debug)]
struct Failed(u64);

impl<I> Stream for ValueLikeLaneReceiver<I>
where
    I: Copy + Unpin,
{
    type Item = Result<(u64, Option<I>, RawLaneResponse), Failed>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let id = this.lane_id;
        let store_id = this.store_id;
        let next = loop {
            let maybe_result = ready!(this.receiver.poll_next_unpin(cx));

            match maybe_result {
                Some(Ok(r)) => {
                    if let Some(raw) = value_or_supply_raw_response(r, this.kind) {
                        break Some(Ok((id, store_id, raw)));
                    }
                }
                Some(Err(_)) => break Some(Err(Failed(id))),
                _ => break None,
            };
        };
        Poll::Ready(next)
    }
}

impl<I> Stream for MapLaneReceiver<I>
where
    I: Copy + Unpin,
{
    type Item = Result<(u64, Option<I>, RawLaneResponse), Failed>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let id = this.lane_id;
        let store_id = this.store_id;
        let next = loop {
            let maybe_result = ready!(this.receiver.poll_next_unpin(cx));
            match maybe_result {
                Some(Ok(r)) => {
                    if let Some(raw) = map_raw_response(r) {
                        break Some(Ok((id, store_id, raw)));
                    }
                }
                Some(Err(_)) => break Some(Err(Failed(id))),
                _ => break None,
            };
        };
        Poll::Ready(next)
    }
}

impl AgentRuntimeTask {
    fn new(
        node: NodeDescriptor,
        init: InitialEndpoints,
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        stopping: trigger::Receiver,
        config: AgentRuntimeConfig,
    ) -> Self {
        AgentRuntimeTask {
            node,
            init,
            attachment_rx,
            stopping,
            config,
            store: StoreDisabled::default(),
        }
    }
}

impl<Store> AgentRuntimeTask<Store>
where
    Store: AgentPersistence + Clone + Send + Sync + 'static,
{
    fn with_store(
        node: NodeDescriptor,
        init: InitialEndpoints,
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        stopping: trigger::Receiver,
        config: AgentRuntimeConfig,
        store: Store,
    ) -> Self {
        AgentRuntimeTask {
            node,
            init,
            attachment_rx,
            stopping,
            config,
            store,
        }
    }
}

impl<Store> AgentRuntimeTask<Store>
where
    Store: AgentPersistence + Clone + Send + Sync,
{
    pub async fn run(self) -> Result<(), StoreError> {
        let AgentRuntimeTask {
            node: NodeDescriptor { identity, node_uri },
            init:
                InitialEndpoints {
                    reporting,
                    rx,
                    endpoints,
                },
            attachment_rx,
            stopping,
            config,
            store,
        } = self;

        let (write_endpoints, read_endpoints): (Vec<_>, Vec<_>) =
            endpoints.into_iter().map(LaneEndpoint::split).unzip();

        let (read_tx, read_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (write_tx, write_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (read_vote, write_vote, vote_waiter) = timeout_coord::timeout_coordinator();

        let (kill_switch_tx, kill_switch_rx) = trigger::trigger();

        let combined_stop = fselect(fselect(stopping.clone(), kill_switch_rx), vote_waiter);
        let att = attachment_task(
            rx,
            attachment_rx,
            reporting.clone(),
            read_tx,
            write_tx.clone(),
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
            read_endpoints,
            write_rx,
            write_vote,
            stopping,
            reporting.as_ref().map(NodeReporting::aggregate),
            store,
        )
        .instrument(info_span!("Agent Runtime Write Task", %identity, %node_uri));

        let io = await_io_tasks(read, write, kill_switch_tx);
        let (_, result) = join(att, io).await;
        result
    }
}

/// Control messages consumed by the read task.
enum ReadTaskMessages {
    /// Create a new lane endpoint.
    Lane { name: Text, sender: LaneSender },
    /// Attach a new remote.
    Remote {
        reader: ByteReader,
        on_attached: Option<trigger::Sender>,
    },
}

/// Control messages consumed by the write task.
#[derive(Debug)]
enum WriteTaskMessage {
    /// Create a new lane endpoint.
    Lane(LaneEndpoint<ByteReader>),
    /// Attach a new remote.
    Remote {
        id: Uuid,
        writer: ByteWriter,
        completion: promise::Sender<DisconnectionReason>,
        on_attached: Option<trigger::Sender>,
    },
    /// A coordination message send by the read task.
    Coord(RwCoorindationMessage),
}

impl WriteTaskMessage {
    /// Determines whether a write task message constitutes "activity" for the purpose of the agent
    /// timing out.
    fn generates_activity(&self) -> bool {
        matches!(self, WriteTaskMessage::Coord(_))
    }
}

const BAD_LANE_REG: &str = "Agent failed to receive lane registration result.";

/// The task that coordinates the attachment of new lanes and remotes to the read and write tasks.
/// #Arguments
/// * `runtime` - Requests from the agent.
/// * `attachment` - External requests to attach new remotes.
/// * `reporting` - Uplink metadata reporter.
/// * `read_tx` - Channel to communicate with the read task.
/// * `write_tx` - Channel to communicate with the write task.
/// * `combined_stop` - The task will stop when this future completes. This should combined the overall
/// shutdown-signal with latch that ensures this task will stop if the read/write tasks stop (to avoid
/// deadlocks).
async fn attachment_task<F>(
    runtime: mpsc::Receiver<AgentRuntimeRequest>,
    attachment: mpsc::Receiver<AgentAttachmentRequest>,
    reporting: Option<NodeReporting>,
    read_tx: mpsc::Sender<ReadTaskMessages>,
    write_tx: mpsc::Sender<WriteTaskMessage>,
    combined_stop: F,
) where
    F: Future + Unpin,
{
    let mut stream = sselect(
        ReceiverStream::new(runtime).map(Either::Left),
        ReceiverStream::new(attachment).map(Either::Right),
    )
    .take_until(combined_stop);

    let mut attachments = FuturesUnordered::new();

    loop {
        tokio::select! {
            biased;
            _ = attachments.next(), if !attachments.is_empty() => {},
            maybe_event = stream.next() => {
                if let Some(event) = maybe_event {
                    match event {
                        Either::Left(request) => {
                            if !handle_runtime_request(request, &read_tx, &write_tx, reporting.as_ref()).await {
                                break;
                            }
                        }
                        Either::Right(request) => {
                            if !handle_att_request(request, &read_tx, &write_tx, |read_rx, write_rx, on_attached| {
                                attachments.push(async move {
                                    if matches!(join(read_rx, write_rx).await, (Ok(_), Ok(_))) {
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
}

#[inline]
async fn handle_runtime_request(
    request: AgentRuntimeRequest,
    read_tx: &mpsc::Sender<ReadTaskMessages>,
    write_tx: &mpsc::Sender<WriteTaskMessage>,
    reporting: Option<&NodeReporting>,
) -> bool {
    match request {
        AgentRuntimeRequest::AddLane {
            name,
            kind,
            config,
            promise,
        } => {
            info!("Registering a new {} lane with name {}.", kind, name);
            let uplink_kind = kind.uplink_kind();
            let (in_tx, in_rx) = byte_channel(config.input_buffer_size);
            let (out_tx, out_rx) = byte_channel(config.output_buffer_size);

            let reporter = if let Some(reporting) = reporting {
                reporting.register(name.clone(), kind).await
            } else {
                None
            };

            let sender = LaneSender::new(in_tx, uplink_kind, reporter.clone());
            let read_permit = match read_tx.reserve().await {
                Err(_) => {
                    warn!(
                        "Read task stopped while attempting to register a new {} lane named '{}'.",
                        kind, name
                    );
                    if promise.send(Err(AgentRuntimeError::Terminated)).is_err() {
                        error!(BAD_LANE_REG);
                    }
                    return false;
                }
                Ok(permit) => permit,
            };
            let write_permit = match write_tx.reserve().await {
                Err(_) => {
                    warn!(
                        "Write task stopped while attempting to register a new {} lane named '{}'.",
                        kind, name
                    );
                    if promise.send(Err(AgentRuntimeError::Terminated)).is_err() {
                        error!(BAD_LANE_REG);
                    }
                    return false;
                }
                Ok(permit) => permit,
            };
            read_permit.send(ReadTaskMessages::Lane {
                name: name.clone(),
                sender,
            });
            let endpoint = LaneEndpoint::new(name, uplink_kind, config.transient, out_rx, reporter);
            write_permit.send(WriteTaskMessage::Lane(endpoint));
            if promise.send(Ok((out_tx, in_rx))).is_err() {
                error!(BAD_LANE_REG);
            }
            true
        }
        _ => todo!("Opening downlinks form agents not implemented."),
    }
}

#[inline]
async fn handle_att_request<F>(
    request: AgentAttachmentRequest,
    read_tx: &mpsc::Sender<ReadTaskMessages>,
    write_tx: &mpsc::Sender<WriteTaskMessage>,
    add_att: F,
) -> bool
where
    F: FnOnce(trigger::Receiver, trigger::Receiver, trigger::Sender),
{
    let AgentAttachmentRequest {
        id,
        io: (tx, rx),
        completion,
        on_attached,
    } = request;
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
        add_att(read_rx, write_rx, on_attached);
        (Some(read_tx), Some(write_tx))
    } else {
        (None, None)
    };
    read_permit.send(ReadTaskMessages::Remote {
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

type RemoteReceiver = FramedRead<ByteReader, RawRequestMessageDecoder>;

fn remote_receiver(reader: ByteReader) -> RemoteReceiver {
    RemoteReceiver::new(reader, Default::default())
}

const TASK_COORD_ERR: &str = "Stopping after communcating with the write task failed.";
const STOP_VOTED: &str = "Stopping as read and write tasks have both voted to do so.";

/// Events that can occur within the read task.
enum ReadTaskEvent {
    /// Register a new lane or remote.
    Registration(ReadTaskMessages),
    /// An envelope was received from a connected remote.
    Envelope(RequestMessage<BytesStr, Bytes>),
    /// The read task timed out due to inactivity.
    Timeout,
}

/// The read task of the agent runtime. This receives envelopes from attached remotes and forwards
/// them on to the appropriate lanes. It also communicates with the write task to maintain uplinks
/// and report on invalid envelopes.
///
/// #Arguments
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
    reg_rx: mpsc::Receiver<ReadTaskMessages>,
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
                fselect(reg_stream.next(), remotes.next()),
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
                ReadTaskMessages::Lane { name, sender } => {
                    let id = next_id();
                    info!(
                        "Reading from new lane named '{}'. Assigned ID is {}.",
                        name, id
                    );
                    name_mapping.insert(name, id);
                    lanes.insert(id, sender);
                }
                ReadTaskMessages::Remote {
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
            },
            ReadTaskEvent::Envelope(msg) => {
                if voted {
                    trace!("Attempting to rescind stop vote.");
                    if stop_vote.rescind() == VoteResult::Unanimous {
                        info!(STOP_VOTED);
                        break;
                    } else {
                        info!("Vote to stop rescinded.");
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
                        let Path { lane, .. } = path;
                        let origin: Uuid = origin;
                        match envelope {
                            Operation::Link => {
                                debug!(
                                    "Attempting to set up link to {} from lane '{}'.",
                                    origin, lane
                                );
                                if write_tx
                                    .send(WriteTaskMessage::Coord(RwCoorindationMessage::Link {
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
                                                RwCoorindationMessage::BadEnvelope {
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
                                    .send(WriteTaskMessage::Coord(RwCoorindationMessage::Unlink {
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
                    info!("Recevied envelope for non-existent lane '{}'.", path.lane);
                    let flush = flush_lane(&mut lanes, &mut needs_flush);
                    let send_err = write_tx.send(WriteTaskMessage::Coord(
                        RwCoorindationMessage::UnknownLane {
                            origin,
                            path: Path::text(
                                path.node.as_str(),
                                path.lane.as_str(),
                            ),
                        },
                    ));
                    let (_, result) = join(flush, send_err).await;
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
    /// An message received from one of the attached lanes.
    Event {
        id: u64,
        store_id: Option<I>,
        response: RawLaneResponse,
    },
    /// A write (to one of the attached remotes) completed.
    WriteDone(WriteResult),
    /// Reading from a lane failed.
    LaneFailed(u64),
    /// A remote may have been without active links beyond the configured timeout.
    PruneRemote(Uuid),
    /// The task timed out due to inactivity.
    Timeout,
    /// The stop signal was received.
    Stop,
}

type LaneStream<I> = StopAfterError<Either<ValueLikeLaneReceiver<I>, MapLaneReceiver<I>>>;

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

/// Aggregates all of the streams of events for the write task.
#[derive(Debug)]
struct WriteTaskEvents<'a, S, W, I> {
    inactive_timeout: InactiveTimeout<'a>,
    remote_timeout: Duration,
    prune_remotes: PruneRemotes<'a>,
    message_stream: S,
    lanes: SelectAll<LaneStream<I>>,
    pending_writes: FuturesUnordered<W>,
}

impl<'a, S, W, I> WriteTaskEvents<'a, S, W, I>
where
    I: Unpin + Copy,
{
    /// #Arguments
    /// * `inactive_timeout` - Time after which the task will vote to stop due to inactivity.
    /// * `remote_timeout` - Time after which a task with no links and no activity should be removed.
    /// * `timeout_delay` - Timer for the agent timeout (held on the stack of the write task to avoid
    /// having it in a separate allocation).
    /// * `prune_delay` - Timer for pruning inactive remotes (held on the stack of the write task to
    /// avoid having it in a separte allocation).
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
            lanes: Default::default(),
            pending_writes: Default::default(),
        }
    }

    /// Add a new lane stream.
    fn add_lane(&mut self, lane: LaneStream<I>) {
        self.lanes.push(lane);
    }

    /// Clear all lane streams.
    fn clear_lanes(&mut self) {
        self.lanes.clear()
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
            lanes,
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
                maybe_result = lanes.next(), if !lanes.is_empty() => {
                    match maybe_result {
                        Some(Ok((id, store_id, response))) =>  {
                            delay.as_mut().reset(
                                Instant::now()
                                    .checked_add(*timeout)
                                    .expect("Timer overflow."),
                            );
                            break WriteTaskEvent::Event { id, store_id, response };
                        },
                        Some(Err(Failed(lane_id))) => {
                            break WriteTaskEvent::LaneFailed(lane_id);
                        }
                        _ => {}
                    }
                }
                _ = &mut delay, if *timeout_enabled => {
                    break if lanes.is_empty() {
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
}

/// Possible results of handling a message from the coordination/read tasks.
#[derive(Debug)]
#[must_use]
enum TaskMessageResult<I> {
    /// Register a new lane.
    AddLane(LaneStream<I>),
    /// Schedule a write to one or all remotes (if no ID is specified).
    ScheduleWrite {
        write: WriteTask,
        schedule_prune: Option<Uuid>,
    },
    /// Track a remote to be pruned after the configured timeout (as it no longer has any links).
    AddPruneTimeout(Uuid),
    /// Persisting the state of a lane failed.
    StoreFailure(StoreError),
    /// No effect.
    Nothing,
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
enum Writes<W> {
    Zero,
    Single(W),
    Two(W, W),
}

impl<W> Default for Writes<W> {
    fn default() -> Self {
        Writes::Zero
    }
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
        }
    }

    fn register(&mut self, name: Text, reporter: Option<UplinkReporter>) -> u64 {
        let WriteTaskState {
            links,
            remote_tracker,
        } = self;
        let lane_id = remote_tracker.lane_registry().add_endpoint(name);
        if let Some(reporter) = reporter {
            links.register_reporter(lane_id, reporter);
        }
        lane_id
    }

    /// Handle a message from the coordination or read task.
    fn handle_task_message<Store>(
        &mut self,
        reg: WriteTaskMessage,
        store: &Store,
    ) -> TaskMessageResult<Store::LaneId>
    where
        Store: AgentPersistence,
    {
        let WriteTaskState {
            links,
            remote_tracker,
            ..
        } = self;
        match reg {
            WriteTaskMessage::Lane(endpoint) => match endpoint.into_lane_stream(store, self) {
                Ok(lane_stream) => TaskMessageResult::AddLane(lane_stream),
                Err(error) => TaskMessageResult::StoreFailure(error),
            },
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
            WriteTaskMessage::Coord(RwCoorindationMessage::Link { origin, lane }) => {
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
            WriteTaskMessage::Coord(RwCoorindationMessage::Unlink { origin, lane }) => {
                info!(
                    "Attempting to close any link from '{}' to {}.",
                    lane, origin
                );
                if let Some(lane_id) = remote_tracker.lane_registry().id_for(lane.as_str()) {
                    if links.is_linked(origin, lane_id) {
                        let schedule_prune = links.remove(lane_id, origin).into_option();
                        let message = Text::new("Link closed.");
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
            WriteTaskMessage::Coord(RwCoorindationMessage::UnknownLane { origin, path }) => {
                info!(
                    "Received envelope for non-existent lane '{}' from {}.",
                    path.lane, origin
                );
                remote_tracker
                    .push_special(SpecialAction::lane_not_found(path.lane), &origin)
                    .into()
            }
            WriteTaskMessage::Coord(RwCoorindationMessage::BadEnvelope {
                origin,
                lane,
                error,
            }) => {
                info!(error = ?error, "Received in invalid envelope for lane '{}' from {}.", lane, origin);
                TaskMessageResult::Nothing
            }
        }
    }

    /// Handle a message from one of the lanes of the agent.
    fn handle_event(
        &mut self,
        id: u64,
        response: RawLaneResponse,
    ) -> impl Iterator<Item = WriteTask> + '_ {
        let WriteTaskState {
            links,
            remote_tracker: write_tracker,
            ..
        } = self;

        use either::Either;

        let RawLaneResponse { target, response } = response;
        if let Some(remote_id) = target {
            trace!(response = ?response, "Routing response to {}.", remote_id);
            links.count_single(id);
            let write = if response.is_synced() && !links.is_linked(remote_id, id) {
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
            trace!(response = ?response, "Discarding response.");
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

/// The write task of the agent runtime. This receives messages from the agent lanes and forwards them
/// to linked remotes. It also receives messages from the read task to maintain the set of uplinks.
///
/// #Arguments
/// * `configuration` - Configuration parameters for the task.
/// * `initial_endpoints` - Initial lane endpoints that were created in the agent initialization phase.
/// * `message_rx` - Channel for messages from the read and coordination tasks.
/// * `stop_voter` - Votes to stop if this task becomes inactive (unanimity with the write task is required).
/// * `stopping` - Initiates the clean shutdown procedure.
/// * `aggregate_reporter` - Aggregated uplink reporter for all lanes of the agent.
/// * `store` - Persistence for the state of the lanes.
///
async fn write_task<Store>(
    configuration: WriteTaskConfiguration,
    initial_endpoints: Vec<LaneEndpoint<ByteReader>>,
    message_rx: mpsc::Receiver<WriteTaskMessage>,
    stop_voter: timeout_coord::Voter,
    stopping: trigger::Receiver,
    aggregate_reporter: Option<UplinkReporter>,
    store: Store,
) -> Result<(), StoreError>
where
    Store: AgentPersistence + Clone + Send + Sync,
{
    let message_stream = ReceiverStream::new(message_rx).take_until(stopping);

    let WriteTaskConfiguration {
        identity,
        node_uri,
        runtime_config,
    } = configuration;

    let timeout_delay = sleep(runtime_config.inactive_timeout);
    let remote_prune_delay = sleep(Duration::default());

    pin_mut!(timeout_delay);
    pin_mut!(remote_prune_delay);
    let mut streams = WriteTaskEvents::new(
        runtime_config.inactive_timeout,
        runtime_config.prune_remote_delay,
        timeout_delay.as_mut(),
        remote_prune_delay,
        message_stream,
    );
    let mut state = WriteTaskState::new(identity, node_uri, aggregate_reporter);

    info!(endpoints = ?initial_endpoints, "Adding initial endpoints.");
    for endpoint in initial_endpoints {
        let lane_stream = endpoint.into_lane_stream(&store, &mut state)?;
        streams.add_lane(lane_stream);
    }

    let mut voted = false;

    let mut remote_reason = DisconnectionReason::AgentStoppedExternally;

    loop {
        let next = streams.select_next().await;
        match next {
            WriteTaskEvent::Message(reg) => match state.handle_task_message(reg, &store) {
                TaskMessageResult::AddLane(lane) => {
                    streams.add_lane(lane);
                }
                TaskMessageResult::ScheduleWrite {
                    write,
                    schedule_prune,
                } => {
                    if voted {
                        if stop_voter.rescind() == VoteResult::Unanimous {
                            info!(STOP_VOTED);
                            remote_reason = DisconnectionReason::AgentTimedOut;
                            break;
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
                TaskMessageResult::StoreFailure(error) => {
                    return Err(error);
                }
                TaskMessageResult::Nothing => {}
            },
            WriteTaskEvent::Event {
                id,
                store_id,
                response,
            } => {
                if voted {
                    if stop_voter.rescind() == VoteResult::Unanimous {
                        info!(STOP_VOTED);
                        remote_reason = DisconnectionReason::AgentTimedOut;
                        break;
                    }
                    streams.enable_timeout();
                    voted = false;
                }
                if let Some(store_id) = store_id {
                    persist_response(&store, store_id, &response)?;
                }
                for write in state.handle_event(id, response) {
                    streams.schedule_write(write.into_future());
                }
            }
            WriteTaskEvent::WriteDone((writer, buffer, result)) => {
                if result.is_ok() {
                    if let Some(write) = state.replace(writer, buffer) {
                        streams.schedule_write(write.into_future());
                    }
                } else {
                    let remote_id = writer.remote_id();
                    error!(
                        "Writing to remote {} failed. Removing attached uplinks.",
                        remote_id
                    );
                    state.remove_remote(remote_id, DisconnectionReason::ChannelClosed);
                }
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
        streams.clear_lanes();
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
    pin_mut!(read);
    pin_mut!(write);
    let first_finished = fselect(read, write).await;
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
    store: &Store,
    store_id: Store::LaneId,
    response: &RawLaneResponse,
) -> Result<(), StoreError>
where
    Store: AgentPersistence,
{
    match response {
        RawLaneResponse {
            target: None,
            response: UplinkResponse::Value(body),
        } => store.put_value(store_id, body.as_ref()),
        RawLaneResponse {
            target: None,
            response: UplinkResponse::Map(operation),
        } => store.apply_map(store_id, operation),
        _ => Ok(()),
    }
}
