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

use crate::compat::{Notification, Operation, RawRequestMessageDecoder, RequestMessage};
use crate::pressure::DownlinkBackpressure;
use crate::routing::RoutingAddr;

use self::links::Links;
use self::uplink::{RemoteSender, SpecialUplinkAction, UplinkResponse, WriteAction, WriteResult};
use self::write_tracker::RemoteWriteTracker;

use super::{AgentAttachmentRequest, AgentRuntimeConfig, AgentRuntimeRequest, Io};
use bytes::{Bytes, BytesMut};
use futures::ready;
use futures::stream::FuturesUnordered;
use futures::{
    future::{join, select as fselect, Either},
    stream::{select as sselect, SelectAll},
    SinkExt, Stream, StreamExt,
};
use pin_utils::pin_mut;
use swim_api::protocol::agent::{LaneResponseKind, MapLaneResponse, ValueLaneResponse};
use swim_api::{
    agent::UplinkKind,
    error::AgentRuntimeError,
    protocol::{
        agent::{
            LaneRequest, LaneRequestEncoder, MapLaneResponseDecoder, ValueLaneResponseDecoder,
        },
        map::{extract_header, MapMessage, MapMessageEncoder, RawMapOperationEncoder},
        WithLengthBytesCodec,
    },
};
use swim_model::path::RelativePath;
use swim_model::Text;
use swim_recon::parser::MessageExtractError;
use swim_utilities::future::{immediate_or_join, StopAfterError, SwimStreamExt};
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swim_utilities::trigger;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant, Sleep};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};
use uuid::Uuid;

use tracing::{debug, error, info, info_span, trace, warn};
use tracing_futures::Instrument;

mod init;
mod links;
mod timeout_coord;
mod uplink;
mod write_tracker;

pub use init::{AgentInitTask, NoLanes};

#[derive(Debug)]
pub struct LaneEndpoint<T> {
    name: Text,
    kind: UplinkKind,
    io: T,
}

const LANE_NOT_FOUND_BODY: &[u8] = b"@laneNotFound";

impl LaneEndpoint<Io> {
    fn split(self) -> (LaneEndpoint<ByteReader>, LaneEndpoint<ByteWriter>) {
        let LaneEndpoint {
            name,
            kind,
            io: (rx, tx),
        } = self;

        let read = LaneEndpoint {
            name: name.clone(),
            kind,
            io: rx,
        };

        let write = LaneEndpoint { name, kind, io: tx };

        (read, write)
    }
}

#[derive(Debug)]
pub struct InitialEndpoints {
    rx: mpsc::Receiver<AgentRuntimeRequest>,
    endpoints: Vec<LaneEndpoint<Io>>,
}

impl InitialEndpoints {
    pub fn make_runtime_task(
        self,
        identity: RoutingAddr,
        node_uri: Text,
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        config: AgentRuntimeConfig,
        stopping: trigger::Receiver,
    ) -> AgentRuntimeTask {
        AgentRuntimeTask {
            identity,
            node_uri,
            init: self,
            attachment_rx,
            stopping,
            config,
        }
    }
}

#[derive(Debug)]
pub struct AgentRuntimeTask {
    identity: RoutingAddr,
    node_uri: Text,
    init: InitialEndpoints,
    attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
    stopping: trigger::Receiver,
    config: AgentRuntimeConfig,
}

type ValueLaneEncoder = LaneRequestEncoder<WithLengthBytesCodec>;
type MapLaneEncoder = LaneRequestEncoder<MapMessageEncoder<RawMapOperationEncoder>>;

/// Sender to communicate with a lane.
#[derive(Debug)]
enum LaneSender {
    Value {
        sender: FramedWrite<ByteWriter, ValueLaneEncoder>,
    },
    Map {
        sender: FramedWrite<ByteWriter, MapLaneEncoder>,
    },
}

enum RwCoorindationMessage {
    UnknownLane {
        origin: Uuid,
        path: RelativePath,
    },
    BadEnvelope {
        origin: Uuid,
        lane: Text,
        error: MessageExtractError,
    },
    Link {
        origin: Uuid,
        lane: Text,
    },
    Unlink {
        origin: Uuid,
        lane: Text,
    },
}

#[derive(Debug, Error)]
enum LaneSendError {
    #[error("Sending lane message failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Interpreting lane message failed: {0}")]
    Extraction(#[from] MessageExtractError),
}

impl LaneSender {
    fn new(tx: ByteWriter, kind: UplinkKind) -> Self {
        match kind {
            UplinkKind::Value => LaneSender::Value {
                sender: FramedWrite::new(tx, LaneRequestEncoder::value()),
            },
            UplinkKind::Map => LaneSender::Map {
                sender: FramedWrite::new(tx, LaneRequestEncoder::map()),
            },
        }
    }

    async fn start_sync(&mut self, id: Uuid) -> Result<(), std::io::Error> {
        match self {
            LaneSender::Value { sender } => {
                let req: LaneRequest<Bytes> = LaneRequest::Sync(id);
                sender.send(req).await
            }
            LaneSender::Map { sender } => {
                let req: LaneRequest<MapMessage<Bytes, Bytes>> = LaneRequest::Sync(id);
                sender.send(req).await
            }
        }
    }

    async fn feed_frame(&mut self, data: Bytes) -> Result<(), LaneSendError> {
        match self {
            LaneSender::Value { sender } => {
                sender.feed(LaneRequest::Command(data)).await?;
            }
            LaneSender::Map { sender } => {
                let message = extract_header(&data)?;
                sender.send(LaneRequest::Command(message)).await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), std::io::Error> {
        match self {
            LaneSender::Value { sender } => flush_sender_val(sender).await,
            LaneSender::Map { sender } => flush_sender_map(sender).await,
        }
    }
}

async fn flush_sender_val<T>(sender: &mut FramedWrite<ByteWriter, T>) -> Result<(), T::Error>
where
    T: Encoder<LaneRequest<Bytes>>,
{
    sender.flush().await
}

async fn flush_sender_map<T>(sender: &mut FramedWrite<ByteWriter, T>) -> Result<(), T::Error>
where
    T: Encoder<LaneRequest<MapMessage<Bytes, Bytes>>>,
{
    sender.flush().await
}

#[derive(Debug)]
struct LaneReceiver<D> {
    lane_id: u64,
    receiver: FramedRead<ByteReader, D>,
}

type ValueLaneReceiver = LaneReceiver<ValueLaneResponseDecoder>;
type MapLaneReceiver = LaneReceiver<MapLaneResponseDecoder>;

#[derive(Debug)]
struct RawLaneResponse {
    target: Option<Uuid>,
    response: UplinkResponse,
}

impl RawLaneResponse {
    pub fn targetted(id: Uuid, response: UplinkResponse) -> Self {
        RawLaneResponse {
            target: Some(id),
            response,
        }
    }

    pub fn broadcast(response: UplinkResponse) -> Self {
        RawLaneResponse {
            target: None,
            response,
        }
    }
}

impl From<ValueLaneResponse<Bytes>> for RawLaneResponse {
    fn from(resp: ValueLaneResponse<Bytes>) -> Self {
        let ValueLaneResponse { kind, value } = resp;
        match kind {
            LaneResponseKind::StandardEvent => {
                RawLaneResponse::broadcast(UplinkResponse::Value(value))
            }
            LaneResponseKind::SyncEvent(id) => {
                RawLaneResponse::targetted(id, UplinkResponse::SyncedValue(value))
            }
        }
    }
}

impl From<MapLaneResponse<Bytes, Bytes>> for RawLaneResponse {
    fn from(resp: MapLaneResponse<Bytes, Bytes>) -> Self {
        match resp {
            MapLaneResponse::Event { kind, operation } => match kind {
                LaneResponseKind::StandardEvent => {
                    RawLaneResponse::broadcast(UplinkResponse::Map(operation))
                }
                LaneResponseKind::SyncEvent(id) => {
                    RawLaneResponse::targetted(id, UplinkResponse::Map(operation))
                }
            },
            MapLaneResponse::SyncComplete(id) => {
                RawLaneResponse::targetted(id, UplinkResponse::SyncedMap)
            }
        }
    }
}

impl LaneReceiver<ValueLaneResponseDecoder> {
    fn value(lane_id: u64, reader: ByteReader) -> Self {
        LaneReceiver {
            lane_id,
            receiver: FramedRead::new(reader, Default::default()),
        }
    }
}

impl LaneReceiver<MapLaneResponseDecoder> {
    fn map(lane_id: u64, reader: ByteReader) -> Self {
        LaneReceiver {
            lane_id,
            receiver: FramedRead::new(reader, Default::default()),
        }
    }
}

#[derive(Debug)]
struct Failed(u64);

impl Stream for ValueLaneReceiver {
    type Item = Result<(u64, RawLaneResponse), Failed>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let maybe_result = ready!(this.receiver.poll_next_unpin(cx));
        let id = this.lane_id;
        Poll::Ready(
            maybe_result.map(|result| result.map(|r| (id, r.into())).map_err(|_| Failed(id))),
        )
    }
}

impl Stream for MapLaneReceiver {
    type Item = Result<(u64, RawLaneResponse), Failed>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let maybe_result = ready!(this.receiver.poll_next_unpin(cx));
        let id = this.lane_id;
        Poll::Ready(
            maybe_result.map(|result| result.map(|r| (id, r.into())).map_err(|_| Failed(id))),
        )
    }
}

impl AgentRuntimeTask {
    pub async fn run(self) {
        let AgentRuntimeTask {
            identity,
            node_uri,
            init: InitialEndpoints { rx, endpoints },
            attachment_rx,
            stopping,
            config,
        } = self;

        let (write_endpoints, read_endpoints): (Vec<_>, Vec<_>) =
            endpoints.into_iter().map(LaneEndpoint::split).unzip();

        let (read_tx, read_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (write_tx, write_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (read_vote, write_vote, vote_waiter) = timeout_coord::timeout_coordinator();

        let (kill_switch_tx, kill_switch_rx) = trigger::trigger();

        let combined_stop = fselect(fselect(stopping.clone(), kill_switch_rx), vote_waiter);
        let att = attachment_task(rx, attachment_rx, read_tx, write_tx.clone(), combined_stop)
            .instrument(info_span!("Agent Runtime Attachment Task", %identity, %node_uri));
        let read = read_task(
            config,
            read_endpoints,
            read_rx,
            write_tx,
            read_vote,
            stopping.clone(),
        )
        .instrument(info_span!("Agent Runtime Read Task", %identity, %node_uri));
        let write = write_task(
            WriteTaskConfiguration::new(identity, node_uri.clone(), config),
            write_endpoints,
            write_rx,
            write_vote,
            stopping,
        )
        .instrument(info_span!("Agent Runtime Write Task", %identity, %node_uri));

        let io = await_io_tasks(read, write, kill_switch_tx);
        join(att, io).await;
    }
}

enum ReadTaskRegistration {
    Lane { name: Text, sender: LaneSender },
    Remote { reader: ByteReader },
}

enum WriteTaskRegistration {
    Lane(LaneEndpoint<ByteReader>),
    Remote { id: Uuid, writer: ByteWriter },
    Coord(RwCoorindationMessage),
}

const BAD_LANE_REG: &str = "Agent failed to receive lane registration result.";

async fn attachment_task<F>(
    runtime: mpsc::Receiver<AgentRuntimeRequest>,
    attachment: mpsc::Receiver<AgentAttachmentRequest>,
    read_tx: mpsc::Sender<ReadTaskRegistration>,
    write_tx: mpsc::Sender<WriteTaskRegistration>,
    combined_stop: F,
) where
    F: Future + Unpin,
{
    let mut stream = sselect(
        ReceiverStream::new(runtime).map(Either::Left),
        ReceiverStream::new(attachment).map(Either::Right),
    )
    .take_until(combined_stop);

    while let Some(event) = stream.next().await {
        match event {
            Either::Left(AgentRuntimeRequest::AddLane {
                name,
                kind,
                config,
                promise,
            }) => {
                info!("Registering a new {} lane with name {}.", kind, name);
                let lane_config = config.unwrap_or_default();
                let (in_tx, in_rx) = byte_channel(lane_config.input_buffer_size);
                let (out_tx, out_rx) = byte_channel(lane_config.output_buffer_size);
                let sender = LaneSender::new(in_tx, kind);
                let read_permit = match read_tx.reserve().await {
                    Err(_) => {
                        warn!("Read task stopped while attempting to register a new {} lane named '{}'.", kind, name);
                        if promise.send(Err(AgentRuntimeError::Terminated)).is_err() {
                            error!(BAD_LANE_REG);
                        }
                        break;
                    }
                    Ok(permit) => permit,
                };
                let write_permit = match write_tx.reserve().await {
                    Err(_) => {
                        warn!("Write task stopped while attempting to register a new {} lane named '{}'.", kind, name);
                        if promise.send(Err(AgentRuntimeError::Terminated)).is_err() {
                            error!(BAD_LANE_REG);
                        }
                        break;
                    }
                    Ok(permit) => permit,
                };
                read_permit.send(ReadTaskRegistration::Lane {
                    name: name.clone(),
                    sender,
                });
                match kind {
                    UplinkKind::Value => {
                        write_permit.send(WriteTaskRegistration::Lane(LaneEndpoint {
                            io: out_rx,
                            name: name.clone(),
                            kind: UplinkKind::Value,
                        }));
                    }
                    UplinkKind::Map => {
                        write_permit.send(WriteTaskRegistration::Lane(LaneEndpoint {
                            io: out_rx,
                            name: name.clone(),
                            kind: UplinkKind::Map,
                        }));
                    }
                }
                if promise.send(Ok((in_rx, out_tx))).is_err() {
                    error!(BAD_LANE_REG);
                }
            }
            Either::Left(_) => todo!("Opening downlinks form agents not implemented."),
            Either::Right(AgentAttachmentRequest { id, io: (rx, tx) }) => {
                info!(
                    "Attaching a new remote endpoint with ID {id} to the agent.",
                    id = id
                );
                let read_permit = match read_tx.reserve().await {
                    Err(_) => {
                        warn!("Read task stopped while attempting to attach a remote endpoint.");
                        break;
                    }
                    Ok(permit) => permit,
                };
                let write_permit = match write_tx.reserve().await {
                    Err(_) => {
                        warn!("Write task stopped while attempting to attach a remote endpoint.");
                        break;
                    }
                    Ok(permit) => permit,
                };
                read_permit.send(ReadTaskRegistration::Remote { reader: rx });
                write_permit.send(WriteTaskRegistration::Remote { id, writer: tx });
            }
        }
    }
}

type RemoteReceiver = FramedRead<ByteReader, RawRequestMessageDecoder>;

fn remote_receiver(reader: ByteReader) -> RemoteReceiver {
    RemoteReceiver::new(reader, Default::default())
}

const TASK_COORD_ERR: &str = "Stopping after communcating with the write task failed.";
const STOP_VOTED: &str = "Stopping as read and write tasks have both voted to do so.";

enum ReadTaskEvent {
    Registration(ReadTaskRegistration),
    Envelope(RequestMessage<Bytes>),
    Timeout,
}

async fn read_task(
    config: AgentRuntimeConfig,
    initial_endpoints: Vec<LaneEndpoint<ByteWriter>>,
    reg_rx: mpsc::Receiver<ReadTaskRegistration>,
    write_tx: mpsc::Sender<WriteTaskRegistration>,
    stop_vote: timeout_coord::Voter,
    stopping: trigger::Receiver,
) {
    let mut remotes = SelectAll::<StopAfterError<RemoteReceiver>>::new();

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

    for LaneEndpoint { name, kind, io } in initial_endpoints.into_iter() {
        let i = next_id();
        name_mapping.insert(name, i);
        lanes.insert(i, LaneSender::new(io, kind));
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
                ReadTaskRegistration::Lane { name, sender } => {
                    let id = next_id();
                    info!(
                        "Reading from new lane named '{}'. Assigned ID is {}.",
                        name, id
                    );
                    name_mapping.insert(name, id);
                    lanes.insert(id, sender);
                }
                ReadTaskRegistration::Remote { reader } => {
                    info!("Reading from new remote endpoint.");
                    let rx = remote_receiver(reader).stop_after_error();
                    remotes.push(rx);
                }
            },
            ReadTaskEvent::Envelope(msg) => {
                if voted {
                    trace!("Attempting to rescind stop vote.");
                    if stop_vote.rescind() {
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

                if let Some(id) = name_mapping.get(&path.lane) {
                    if matches!(&needs_flush, Some(i) if i != id) {
                        trace!(
                            "Flushing lane '{name}' (id = {id})",
                            name = path.lane,
                            id = id
                        );
                        flush_lane(&mut lanes, &mut needs_flush).await;
                    }
                    if let Some(lane_tx) = lanes.get_mut(id) {
                        let RelativePath { lane, .. } = path;
                        let origin: Uuid = origin.into();

                        match envelope {
                            Operation::Link => {
                                debug!(
                                    "Attempting to set up link to {} from lane '{}'.",
                                    origin, lane
                                );
                                if write_tx
                                    .send(WriteTaskRegistration::Coord(
                                        RwCoorindationMessage::Link { origin, lane },
                                    ))
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
                                    if let Some(id) = name_mapping.remove(&lane) {
                                        lanes.remove(&id);
                                    }
                                };
                            }
                            Operation::Command(body) => {
                                trace!(body = ?body, "Dispatching command envelope from {} to lane '{}'.", origin, lane);
                                match lane_tx.feed_frame(body).await {
                                    Err(LaneSendError::Io(_)) => {
                                        error!("Failed to communicate with lane '{}'. Removing handle.", lane);
                                        if let Some(id) = name_mapping.remove(&lane) {
                                            lanes.remove(&id);
                                        }
                                    }
                                    Err(LaneSendError::Extraction(error)) => {
                                        error!(error = ?error, "Received invalid envelope from {} for lane '{}'", origin, lane);
                                        if write_tx
                                            .send(WriteTaskRegistration::Coord(
                                                RwCoorindationMessage::BadEnvelope {
                                                    origin,
                                                    lane,
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
                                    .send(WriteTaskRegistration::Coord(
                                        RwCoorindationMessage::Unlink { origin, lane },
                                    ))
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
                    let send_err = write_tx.send(WriteTaskRegistration::Coord(
                        RwCoorindationMessage::UnknownLane {
                            origin: origin.into(),
                            path,
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
                if stop_vote.vote() {
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

enum WriteTaskEvent {
    Registration(WriteTaskRegistration),
    Event { id: u64, response: RawLaneResponse },
    WriteDone(WriteResult),
    LaneFailed(u64),
    Timeout,
    Stop,
}

type LaneStream = StopAfterError<Either<ValueLaneReceiver, MapLaneReceiver>>;

#[derive(Debug, Default)]
struct LaneChannels {
    lane_id_counter: u64,
    lane_names: HashMap<Text, u64>,
    lane_names_rev: HashMap<u64, Text>,
}

impl LaneChannels {
    fn next_id(&mut self) -> u64 {
        let id = self.lane_id_counter;
        self.lane_id_counter += 1;
        id
    }

    #[must_use]
    fn add_endpoint(&mut self, endpoint: LaneEndpoint<ByteReader>) -> LaneStream {
        let id = self.next_id();
        let LaneChannels {
            lane_names,
            lane_names_rev,
            ..
        } = self;
        let LaneEndpoint {
            name,
            kind,
            io: reader,
        } = endpoint;
        debug!("Adding lane with name '{}' and ID {}.", name, id);
        lane_names.insert(name.clone(), id);
        lane_names_rev.insert(id, name);

        match kind {
            UplinkKind::Value => {
                let receiver = LaneReceiver::value(id, reader);
                Either::Left(receiver).stop_after_error()
            }
            UplinkKind::Map => {
                let receiver = LaneReceiver::map(id, reader);
                Either::Right(receiver).stop_after_error()
            }
        }
    }

    fn remove(&mut self, id: u64) -> Option<Text> {
        let LaneChannels {
            lane_names,
            lane_names_rev,
            ..
        } = self;
        if let Some(name) = lane_names_rev.remove(&id) {
            lane_names.remove(&name);
            Some(name)
        } else {
            None
        }
    }

    fn id_for(&self, name: &Text) -> Option<u64> {
        self.lane_names.get(name).copied()
    }
}

async fn perform_write(
    mut writer: RemoteSender,
    mut buffer: BytesMut,
    action: WriteAction,
) -> (RemoteSender, BytesMut, Result<(), std::io::Error>) {
    let result = perform_write_inner(&mut writer, &mut buffer, action).await;
    (writer, buffer, result)
}

async fn perform_write_inner(
    writer: &mut RemoteSender,
    buffer: &mut BytesMut,
    action: WriteAction,
) -> Result<(), std::io::Error> {
    match action {
        WriteAction::Event => {
            writer
                .send_notification(Notification::Event(&*buffer))
                .await?;
        }
        WriteAction::EventAndSynced => {
            writer
                .send_notification(Notification::Event(&*buffer))
                .await?;
            writer.send_notification(Notification::Synced).await?;
        }
        WriteAction::MapSynced(maybe_queue) => {
            if let Some(mut queue) = maybe_queue {
                while queue.has_data() {
                    queue.prepare_write(buffer);
                    writer
                        .send_notification(Notification::Event(&*buffer))
                        .await?;
                }
                writer.send_notification(Notification::Synced).await?;
            }
        }
        WriteAction::Special(SpecialUplinkAction::Linked(_)) => {
            writer.send_notification(Notification::Linked).await?;
        }
        WriteAction::Special(SpecialUplinkAction::Unlinked(_, msg)) => {
            writer
                .send_notification(Notification::Unlinked(Some(msg.as_bytes())))
                .await?;
        }
        WriteAction::Special(SpecialUplinkAction::LaneNotFound) => {
            writer
                .send_notification(Notification::Unlinked(Some(LANE_NOT_FOUND_BODY)))
                .await?;
        }
    }

    Ok(())
}

#[derive(Debug)]
struct WriteTaskConfiguration {
    identity: RoutingAddr,
    node_uri: Text,
    runtime_config: AgentRuntimeConfig,
}

impl WriteTaskConfiguration {
    fn new(identity: RoutingAddr, node_uri: Text, runtime_config: AgentRuntimeConfig) -> Self {
        WriteTaskConfiguration {
            identity,
            node_uri,
            runtime_config,
        }
    }
}

#[derive(Debug)]
struct WriteTaskEvents<'a, S, W> {
    inactive_timeout: Duration,
    timeout_delay: Pin<&'a mut Sleep>,
    reg_stream: S,
    lanes: SelectAll<LaneStream>,
    pending_writes: FuturesUnordered<W>,
}

impl<'a, S, W> WriteTaskEvents<'a, S, W> {
    fn new(inactive_timeout: Duration, timeout_delay: Pin<&'a mut Sleep>, reg_stream: S) -> Self {
        WriteTaskEvents {
            inactive_timeout,
            timeout_delay,
            reg_stream,
            lanes: Default::default(),
            pending_writes: Default::default(),
        }
    }

    fn add_lane(&mut self, lane: LaneStream) {
        self.lanes.push(lane);
    }

    fn schedule_write(&mut self, write: W) {
        self.pending_writes.push(write);
    }
}

impl<'a, S, W> WriteTaskEvents<'a, S, W>
where
    S: Stream<Item = WriteTaskRegistration> + Unpin,
    W: Future<Output = WriteResult> + Send + 'static,
{
    async fn select_next(&mut self) -> WriteTaskEvent {
        let WriteTaskEvents {
            inactive_timeout,
            timeout_delay,
            reg_stream,
            lanes,
            pending_writes,
        } = self;

        let mut delay = timeout_delay.as_mut();

        loop {
            tokio::select! {
                biased;
                maybe_reg = reg_stream.next() => {
                    break if let Some(reg) = maybe_reg {
                        WriteTaskEvent::Registration(reg)
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
                        Some(Ok((id, response))) =>  {
                            delay.as_mut().reset(
                                Instant::now()
                                    .checked_add(*inactive_timeout)
                                    .expect("Timer overflow."),
                            );
                            break WriteTaskEvent::Event { id, response };
                        },
                        Some(Err(Failed(lane_id))) => {
                            break WriteTaskEvent::LaneFailed(lane_id);
                        }
                        _ => {}
                    }
                }
                _ = &mut delay => {
                    if lanes.is_empty() {
                        trace!("Stopping as there are no active lanes.");
                        WriteTaskEvent::Stop
                    } else {
                        WriteTaskEvent::Timeout
                    };
                }
            };
        }
    }
}

#[derive(Debug)]
struct WriteTaskState<F> {
    configuration: WriteTaskConfiguration,
    links: Links,
    lane_channels: LaneChannels,
    write_tracker: RemoteWriteTracker<F>,
}

#[derive(Debug)]
enum RegistrationResult<W> {
    AddLane(LaneStream),
    ScheduleWrite(W),
    Nothing,
}

impl<W> From<Option<W>> for RegistrationResult<W> {
    fn from(opt: Option<W>) -> Self {
        if let Some(write) = opt {
            RegistrationResult::ScheduleWrite(write)
        } else {
            RegistrationResult::Nothing
        }
    }
}

impl<W, F> WriteTaskState<F>
where
    F: Fn(RemoteSender, BytesMut, WriteAction) -> W + Clone,
    W: Future<Output = WriteResult> + Send + 'static,
{
    fn new(configuration: WriteTaskConfiguration, write_op: F) -> Self {
        WriteTaskState {
            configuration,
            links: Default::default(),
            lane_channels: Default::default(),
            write_tracker: RemoteWriteTracker::new(write_op),
        }
    }

    #[must_use]
    fn handle_registration(&mut self, reg: WriteTaskRegistration) -> RegistrationResult<W> {
        let WriteTaskState {
            configuration:
                WriteTaskConfiguration {
                    identity, node_uri, ..
                },
            lane_channels,
            links,
            write_tracker,
            ..
        } = self;
        match reg {
            WriteTaskRegistration::Lane(endpoint) => {
                RegistrationResult::AddLane(lane_channels.add_endpoint(endpoint))
            }
            WriteTaskRegistration::Remote { id, writer } => {
                write_tracker.insert(id, node_uri.clone(), *identity, writer);
                RegistrationResult::Nothing
            }
            WriteTaskRegistration::Coord(RwCoorindationMessage::Link { origin, lane }) => {
                info!("Attempting to set up link from '{}' to {}.", lane, origin);
                match lane_channels.id_for(&lane) {
                    Some(id) if write_tracker.has_remote(origin) => {
                        links.insert(id, origin);
                        let lane_name = &lane_channels.lane_names_rev[&id];
                        write_tracker
                            .push_special(
                                lane_name.as_str(),
                                SpecialUplinkAction::Linked(id),
                                &origin,
                            )
                            .into()
                    }
                    Some(_) => {
                        error!("No remote with ID {}.", origin);
                        RegistrationResult::Nothing
                    }
                    _ => {
                        if write_tracker.has_remote(origin) {
                            error!("No lane named '{}'.", lane);
                        } else {
                            error!("No lane named '{}' or remote with ID {}.", lane, origin);
                        }
                        RegistrationResult::Nothing
                    }
                }
            }
            WriteTaskRegistration::Coord(RwCoorindationMessage::Unlink { origin, lane }) => {
                info!(
                    "Attempting to close any link from '{}' to {}.",
                    lane, origin
                );
                if let Some(lane_id) = lane_channels.id_for(&lane) {
                    links.remove(lane_id, origin);
                    let message = Text::new("Link closed.");
                    let lane_name = &lane_channels.lane_names_rev[&lane_id];
                    write_tracker
                        .push_special(
                            lane_name.as_str(),
                            SpecialUplinkAction::Unlinked(lane_id, message),
                            &origin,
                        )
                        .into()
                } else {
                    error!("No lane named '{}'.", lane);
                    RegistrationResult::Nothing
                }
            }
            WriteTaskRegistration::Coord(RwCoorindationMessage::UnknownLane { origin, path }) => {
                info!(
                    "Received envelope for non-existent lane '{}' from {}.",
                    path.lane, origin
                );
                write_tracker
                    .push_special(
                        path.lane.as_str(),
                        SpecialUplinkAction::LaneNotFound,
                        &origin,
                    )
                    .into()
            }
            WriteTaskRegistration::Coord(RwCoorindationMessage::BadEnvelope {
                origin,
                lane,
                error,
            }) => {
                info!(error = ?error, "Received in invalid envelope for lane '{}' from {}.", lane, origin);
                RegistrationResult::Nothing
            }
        }
    }

    fn handle_event(&mut self, id: u64, response: RawLaneResponse) -> impl Iterator<Item = W> + '_ {
        let WriteTaskState {
            lane_channels,
            links,
            write_tracker,
            ..
        } = self;

        use either::Either;

        let RawLaneResponse { target, response } = response;
        let lane_name = &lane_channels.lane_names_rev[&id];
        if let Some(remote_id) = target {
            trace!(response = ?response, "Routing response to {}.", remote_id);
            let write = write_tracker.push_write(id, lane_name.as_str(), response, &remote_id);
            Either::Left(write.into_iter())
        } else if let Some(targets) = links.linked_from(id) {
            trace!(response = ?response, targets = ?targets, "Broadcasting response to all linked remotes.");
            Either::Right(targets.iter().zip(std::iter::repeat(response)).flat_map(
                move |(remote_id, response)| {
                    write_tracker.push_write(id, lane_name.as_str(), response, remote_id)
                },
            ))
        } else {
            trace!(response = ?response, "Discarding response.");
            Either::Left(None.into_iter())
        }
    }

    fn remove_remote(&mut self, remote_id: Uuid) {
        info!("Removing remote connection {}.", remote_id);
        self.links.remove_remote(remote_id);
        self.write_tracker.remove_remote(remote_id);
    }

    fn remove_lane(&mut self, lane_id: u64) -> impl Iterator<Item = W> + '_ {
        let WriteTaskState {
            lane_channels,
            links,
            write_tracker,
            ..
        } = self;
        info!("Attempting to remove lane with id {}.", lane_id);
        lane_channels.remove(lane_id);
        let linked_remotes = links.remove_lane(lane_id);
        linked_remotes.into_iter().flat_map(move |remote_id| {
            let lane_name = &lane_channels.lane_names_rev[&lane_id];
            info!("Unlinking remotes connected to {}.", lane_name);
            write_tracker.unlink_lane(remote_id, lane_id, lane_name.as_str())
        })
    }

    fn replace(&mut self, writer: RemoteSender, buffer: BytesMut) -> Option<W> {
        trace!(
            "Replacing writer {} after completed write.",
            writer.remote_id()
        );
        self.write_tracker.replace_and_pop(writer, buffer)
    }

    fn has_remotes(&self) -> bool {
        !self.write_tracker.is_empty()
    }
}

async fn write_task(
    configuration: WriteTaskConfiguration,
    initial_endpoints: Vec<LaneEndpoint<ByteReader>>,
    reg_rx: mpsc::Receiver<WriteTaskRegistration>,
    stop_voter: timeout_coord::Voter,
    stopping: trigger::Receiver,
) {
    let reg_stream = ReceiverStream::new(reg_rx).take_until(stopping);
    let runtime_config = configuration.runtime_config;

    let timeout_delay = sleep(runtime_config.inactive_timeout);

    pin_mut!(timeout_delay);
    let mut streams = WriteTaskEvents::new(
        runtime_config.inactive_timeout,
        timeout_delay.as_mut(),
        reg_stream,
    );
    let mut state = WriteTaskState::new(configuration, perform_write);

    info!(endpoints = ?initial_endpoints, "Adding initial endpoints.");
    for endpoint in initial_endpoints {
        let lane = state.lane_channels.add_endpoint(endpoint);
        streams.add_lane(lane);
    }

    let mut voted = false;

    loop {
        let next = streams.select_next().await;
        match next {
            WriteTaskEvent::Registration(reg) => match state.handle_registration(reg) {
                RegistrationResult::AddLane(lane) => {
                    streams.add_lane(lane);
                }
                RegistrationResult::ScheduleWrite(write) => {
                    streams.schedule_write(write);
                }
                _ => {}
            },
            WriteTaskEvent::Event { id, response } => {
                if voted {
                    if stop_voter.rescind() {
                        info!(STOP_VOTED);
                        break;
                    }
                    voted = false;
                }
                for write in state.handle_event(id, response) {
                    streams.schedule_write(write);
                }
            }
            WriteTaskEvent::WriteDone((writer, buffer, result)) => {
                if result.is_ok() {
                    if let Some(write) = state.replace(writer, buffer) {
                        streams.schedule_write(write);
                    }
                } else {
                    let remote_id = writer.remote_id();
                    error!(
                        "Writing to remote {} failed. Removing attached uplinks.",
                        remote_id
                    );
                    state.remove_remote(remote_id);
                }
            }
            WriteTaskEvent::LaneFailed(lane_id) => {
                error!(
                    "Lane with ID {} failed. Unlinking all attached uplinks.",
                    lane_id
                );
                for write in state.remove_lane(lane_id) {
                    streams.schedule_write(write);
                }
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
                if stop_voter.vote() {
                    info!(STOP_VOTED);
                    break;
                }
            }
            WriteTaskEvent::Stop => {
                info!("Write task stopping.");
                break;
            }
        }
    }
}

async fn await_io_tasks<F1, F2>(read: F1, write: F2, kill_switch_tx: trigger::Sender)
where
    F1: Future<Output = ()>,
    F2: Future<Output = ()>,
{
    pin_mut!(read);
    pin_mut!(write);
    let first_finished = fselect(read, write).await;
    kill_switch_tx.trigger();
    match first_finished {
        Either::Left((_, write_fut)) => write_fut.await,
        Either::Right((_, read_fut)) => read_fut.await,
    }
}
