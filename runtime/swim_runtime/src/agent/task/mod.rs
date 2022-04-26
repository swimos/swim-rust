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

use std::collections::hash_map::Entry;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::str::Utf8Error;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{collections::HashMap, num::NonZeroUsize};

use crate::compat::{
    Notification, Operation, RawRequestMessageDecoder, RawResponseMessageEncoder, RequestMessage,
    ResponseMessage,
};
use crate::pressure::recon::MapOperationReconEncoder;
use crate::pressure::{DownlinkBackpressure, MapBackpressure, ValueBackpressure};
use crate::routing::RoutingAddr;

use super::{AgentAttachmentRequest, AgentRuntimeRequest, Io};
use bytes::{BufMut, Bytes, BytesMut};
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
    agent::{LaneConfig, UplinkKind},
    error::AgentRuntimeError,
    protocol::{
        agent::{
            LaneRequest, LaneRequestEncoder, MapLaneResponseDecoder, ValueLaneResponseDecoder,
        },
        map::{
            extract_header, MapMessage, MapMessageEncoder, MapOperation, RawMapOperationEncoder,
        },
        WithLengthBytesCodec,
    },
};
use swim_model::path::RelativePath;
use swim_model::Text;
use swim_recon::parser::MessageExtractError;
use swim_utilities::future::{immediate_or_join, StopAfterError, SwimStreamExt};
use swim_utilities::io::byte_channel::{self, byte_channel, ByteReader, ByteWriter};
use swim_utilities::trigger;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant, Sleep};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};
use uuid::Uuid;

mod timeout_coord;

#[derive(Debug, Clone, Copy)]
pub struct AgentRuntimeConfig {
    pub default_lane_config: LaneConfig,
    /// Size of the queue for accepting new subscribers to a downlink.
    pub attachment_queue_size: NonZeroUsize,
    pub inactive_timeout: Duration,
}

pub struct AgentInitTask {
    requests: mpsc::Receiver<AgentRuntimeRequest>,
    init_complete: trigger::Receiver,
    config: AgentRuntimeConfig,
}

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

        let write = LaneEndpoint {
            name: name.clone(),
            kind,
            io: tx,
        };

        (read, write)
    }
}

pub struct AgentInit {
    rx: mpsc::Receiver<AgentRuntimeRequest>,
    endpoints: Vec<LaneEndpoint<Io>>,
}

impl AgentInit {
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

pub struct NoLanes;

impl AgentInitTask {
    pub fn new(
        requests: mpsc::Receiver<AgentRuntimeRequest>,
        init_complete: trigger::Receiver,
        config: AgentRuntimeConfig,
    ) -> Self {
        AgentInitTask {
            requests,
            init_complete,
            config,
        }
    }

    pub async fn run(self) -> Result<AgentInit, NoLanes> {
        let AgentInitTask {
            requests,
            init_complete,
            config: agent_config,
        } = self;

        let mut request_stream = ReceiverStream::new(requests);
        let mut terminated = (&mut request_stream).take_until(init_complete);

        let mut endpoints = vec![];

        while let Some(request) = terminated.next().await {
            match request {
                AgentRuntimeRequest::AddLane {
                    name,
                    kind,
                    config,
                    promise,
                } => {
                    let LaneConfig {
                        input_buffer_size,
                        output_buffer_size,
                    } = config.unwrap_or(agent_config.default_lane_config);

                    let (in_tx, in_rx) = byte_channel::byte_channel(input_buffer_size);
                    let (out_tx, out_rx) = byte_channel::byte_channel(output_buffer_size);

                    let io = (in_rx, out_tx);
                    if promise.send(Ok(io)).is_ok() {
                        endpoints.push(LaneEndpoint {
                            name,
                            kind,
                            io: (out_rx, in_tx),
                        });
                    }
                }
                AgentRuntimeRequest::OpenDownlink { .. } => {
                    todo!("Opening downlinks from agents not implemented yet.")
                }
            }
        }
        if endpoints.is_empty() {
            Err(NoLanes)
        } else {
            Ok(AgentInit {
                rx: request_stream.into_inner(),
                endpoints,
            })
        }
    }
}

pub struct AgentRuntimeTask {
    identity: RoutingAddr,
    node_uri: Text,
    init: AgentInit,
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

struct LaneReceiver<D> {
    lane_id: u64,
    receiver: FramedRead<ByteReader, D>,
}

type ValueLaneReceiver = LaneReceiver<ValueLaneResponseDecoder>;
type MapLaneReceiver = LaneReceiver<MapLaneResponseDecoder>;

#[derive(Debug, Clone)]
enum UplinkResponse {
    SyncedValue(Bytes),
    SyncedMap,
    Value(Bytes),
    Map(MapOperation<Bytes, Bytes>),
    Special(SpecialUplinkAction),
}

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
            init: AgentInit { rx, endpoints },
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
        let att = attachment_task(rx, attachment_rx, read_tx, write_tx.clone(), combined_stop);
        let read = read_task(
            config,
            read_endpoints,
            read_rx,
            write_tx,
            read_vote,
            stopping.clone(),
        );
        let write = write_task(
            WriteTaskConfiguration::new(identity, node_uri, config),
            write_endpoints,
            write_rx,
            write_vote,
            stopping,
        );

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
                let lane_config = config.unwrap_or_default();
                let (in_tx, in_rx) = byte_channel(lane_config.input_buffer_size);
                let (out_tx, out_rx) = byte_channel(lane_config.output_buffer_size);
                let sender = LaneSender::new(in_tx, kind);
                let read_permit = match read_tx.reserve().await {
                    Err(_) => {
                        let _ = promise.send(Err(AgentRuntimeError::Terminated));
                        break;
                    }
                    Ok(permit) => permit,
                };
                let write_permit = match write_tx.reserve().await {
                    Err(_) => {
                        let _ = promise.send(Err(AgentRuntimeError::Terminated));
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
                let _ = promise.send(Ok((in_rx, out_tx)));
            }
            Either::Left(_) => todo!("Opening downlinks form agents not implemented."),
            Either::Right(AgentAttachmentRequest { id, io: (rx, tx) }) => {
                let read_permit = match read_tx.reserve().await {
                    Err(_) => {
                        break;
                    }
                    Ok(permit) => permit,
                };
                let write_permit = match write_tx.reserve().await {
                    Err(_) => {
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
                    break;
                }
                Ok(Either::Right((Some(Ok(envelope)), _))) => ReadTaskEvent::Envelope(envelope),
                Ok(Either::Right((Some(Err(_e)), _))) => {
                    todo!("Log error");
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
                    name_mapping.insert(name, id);
                    lanes.insert(id, sender);
                }
                ReadTaskRegistration::Remote { reader } => {
                    let rx = remote_receiver(reader).stop_after_error();
                    remotes.push(rx);
                }
            },
            ReadTaskEvent::Envelope(msg) => {
                let RequestMessage {
                    path,
                    origin,
                    envelope,
                } = msg;

                if let Some(id) = name_mapping.get(&path.lane) {
                    if matches!(&needs_flush, Some(i) if i != id) {
                        flush_lane(&mut lanes, &mut needs_flush).await;
                    }
                    if let Some(lane_tx) = lanes.get_mut(id) {
                        let RelativePath { lane, .. } = path;
                        let origin: Uuid = origin.into();

                        match envelope {
                            Operation::Link => {
                                if write_tx
                                    .send(WriteTaskRegistration::Coord(
                                        RwCoorindationMessage::Link { origin, lane },
                                    ))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Operation::Sync => {
                                if lane_tx.start_sync(origin).await.is_err() {
                                    //TODO Log failed lane.
                                    if let Some(id) = name_mapping.remove(&lane) {
                                        lanes.remove(&id);
                                    }
                                };
                            }
                            Operation::Command(body) => {
                                match lane_tx.feed_frame(body).await {
                                    Err(LaneSendError::Io(_)) => {
                                        //TODO Log failed lane.
                                        if let Some(id) = name_mapping.remove(&lane) {
                                            lanes.remove(&id);
                                        }
                                    }
                                    Err(LaneSendError::Extraction(error)) => {
                                        //TODO Log bad envelope.
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
                                            break;
                                        }
                                    }
                                    _ => {
                                        needs_flush = Some(*id);
                                    }
                                }
                            }
                            Operation::Unlink => {
                                if write_tx
                                    .send(WriteTaskRegistration::Coord(
                                        RwCoorindationMessage::Unlink { origin, lane },
                                    ))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    //TODO Log non-existant lane.
                    let flush = flush_lane(&mut lanes, &mut needs_flush);
                    let send_err = write_tx.send(WriteTaskRegistration::Coord(
                        RwCoorindationMessage::UnknownLane {
                            origin: origin.into(),
                            path,
                        },
                    ));
                    let (_, result) = join(flush, send_err).await;
                    if result.is_err() {
                        break;
                    }
                }
            }
            ReadTaskEvent::Timeout => {
                if stop_vote.vote() {
                    break;
                }
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
    Timeout,
    Stop,
}

type LaneStream = StopAfterError<Either<ValueLaneReceiver, MapLaneReceiver>>;

#[derive(Default)]
struct LaneChannels {
    lane_id_counter: u64,
    lane_names: HashMap<Text, u64>,
    lane_names_rev: HashMap<u64, Text>,
    lanes: SelectAll<LaneStream>,
}

impl Debug for LaneChannels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneChannels")
            .field("lane_id_counter", &self.lane_id_counter)
            .field("lane_names", &self.lane_names)
            .field("lane_names_rev", &self.lane_names_rev)
            .field("lanes", &"...")
            .finish()
    }
}

impl LaneChannels {
    fn next_id(&mut self) -> u64 {
        let id = self.lane_id_counter;
        self.lane_id_counter += 1;
        id
    }

    fn add_endpoint(&mut self, endpoint: LaneEndpoint<ByteReader>) {
        let id = self.next_id();
        let LaneChannels {
            lane_names,
            lane_names_rev,
            lanes,
            ..
        } = self;
        let LaneEndpoint {
            name,
            kind,
            io: reader,
        } = endpoint;
        lane_names.insert(name.clone(), id);
        lane_names_rev.insert(id, name);
        let stream = match kind {
            UplinkKind::Value => {
                let receiver = LaneReceiver::value(id, reader);
                Either::Left(receiver).stop_after_error()
            }
            UplinkKind::Map => {
                let receiver = LaneReceiver::map(id, reader);
                Either::Right(receiver).stop_after_error()
            }
        };
        lanes.push(stream);
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

    fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }

    fn id_for(&self, name: &Text) -> Option<u64> {
        self.lane_names.get(name).copied()
    }
}

#[derive(Debug, Default)]
struct Links {
    forward: HashMap<u64, HashSet<Uuid>>,
    backwards: HashMap<Uuid, HashSet<u64>>,
}

impl Links {
    fn insert(&mut self, lane_id: u64, remote_id: Uuid) {
        let Links { forward, backwards } = self;
        forward.entry(lane_id).or_default().insert(remote_id);
        backwards.entry(remote_id).or_default().insert(lane_id);
    }

    fn remove(&mut self, lane_id: u64, remote_id: Uuid) {
        let Links { forward, backwards } = self;

        if let Entry::Occupied(mut entry) = forward.entry(lane_id) {
            entry.get_mut().remove(&remote_id);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
        if let Entry::Occupied(mut entry) = backwards.entry(remote_id) {
            entry.get_mut().remove(&lane_id);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    fn linked_from(&self, id: u64) -> Option<&HashSet<Uuid>> {
        self.forward.get(&id)
    }

    fn remove_lane(&mut self, id: u64) -> HashSet<Uuid> {
        let Links { forward, backwards } = self;
        let remote_ids = forward.remove(&id).unwrap_or_default();
        for remote_id in remote_ids.iter() {
            if let Some(set) = backwards.get_mut(&remote_id) {
                set.remove(&id);
            }
        }
        remote_ids
    }

    fn remove_remote(&mut self, id: Uuid) {
        let Links { forward, backwards } = self;
        let lane_ids = backwards.remove(&id).unwrap_or_default();
        for lane_id in lane_ids.iter() {
            if let Some(set) = forward.get_mut(&lane_id) {
                set.remove(&id);
            }
        }
    }
}

#[derive(Debug, Default)]
struct Uplink<B> {
    queued: bool,
    send_synced: bool,
    backpressure: B,
}

#[derive(Debug)]
struct RemoteSender {
    sender: FramedWrite<ByteWriter, RawResponseMessageEncoder>,
    identity: RoutingAddr,
    node: Text,
    prev_lane: Option<u64>,
    lane: String,
}

impl RemoteSender {
    fn remote_id(&self) -> Uuid {
        *self.identity.uuid()
    }

    fn update_lane(&mut self, lane_id: u64, lane_names: &HashMap<u64, Text>) {
        let RemoteSender {
            prev_lane, lane, ..
        } = self;
        if *prev_lane != Some(lane_id) {
            *prev_lane = Some(lane_id);
            lane.clear();
            lane.push_str(lane_names[&lane_id].as_str());
        }
    }

    async fn send_notification(
        &mut self,
        notification: Notification<&BytesMut, &[u8]>,
        with_flush: bool,
    ) -> Result<(), std::io::Error> {
        let RemoteSender {
            sender,
            identity,
            node,
            lane,
            ..
        } = self;

        let message: ResponseMessage<&BytesMut, &[u8]> = ResponseMessage {
            origin: *identity,
            path: RelativePath::new(node.as_str(), lane.as_str()),
            envelope: notification,
        };
        if with_flush {
            sender.send(message).await?;
        } else {
            sender.feed(message).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum SpecialUplinkAction {
    Linked(u64),
    Unlinked(u64, Text),
    LaneNotFound(Text),
}

type WriteResult = (RemoteSender, BytesMut, Result<(), std::io::Error>);

struct Uplinks<F> {
    writer: Option<(RemoteSender, BytesMut)>,
    value_uplinks: HashMap<u64, Uplink<ValueBackpressure>>,
    map_uplinks: HashMap<u64, Uplink<MapBackpressure>>,
    write_queue: VecDeque<(UplinkKind, u64)>,
    special_queue: VecDeque<SpecialUplinkAction>,
    write_op: F,
}

enum WriteAction {
    Event,
    EventAndSynced,
    MapSynced(Option<MapBackpressure>),
    Special(SpecialUplinkAction),
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
                .send_notification(Notification::Event(&*buffer), true)
                .await?;
        }
        WriteAction::EventAndSynced => {
            writer
                .send_notification(Notification::Event(&*buffer), true)
                .await?;
            writer.send_notification(Notification::Synced, true).await?;
        }
        WriteAction::MapSynced(maybe_queue) => {
            if let Some(mut queue) = maybe_queue {
                while queue.has_data() {
                    queue.prepare_write(buffer);
                    writer
                        .send_notification(Notification::Event(&*buffer), true)
                        .await?;
                }
                writer.send_notification(Notification::Synced, true).await?;
            }
        }
        WriteAction::Special(SpecialUplinkAction::Linked(_)) => {
            writer.send_notification(Notification::Linked, true).await?;
        }
        WriteAction::Special(SpecialUplinkAction::Unlinked(_, msg)) => {
            writer
                .send_notification(Notification::Unlinked(Some(msg.as_bytes())), true)
                .await?;
        }
        WriteAction::Special(SpecialUplinkAction::LaneNotFound(_)) => {
            writer
                .send_notification(Notification::Unlinked(Some(LANE_NOT_FOUND_BODY)), true)
                .await?;
        }
    }

    Ok(())
}

fn write_to_buffer(response: UplinkResponse, buffer: &mut BytesMut) -> WriteAction {
    match response {
        UplinkResponse::SyncedValue(body) => {
            buffer.clear();
            buffer.reserve(body.len());
            buffer.put(body);
            WriteAction::EventAndSynced
        }
        UplinkResponse::SyncedMap => WriteAction::MapSynced(None),
        UplinkResponse::Value(body) => {
            buffer.clear();
            buffer.reserve(body.len());
            buffer.put(body);
            WriteAction::Event
        }
        UplinkResponse::Map(operation) => {
            let mut encoder = MapOperationReconEncoder;
            encoder
                .encode(operation, buffer)
                .expect("Wiritng map operations is infallible.");
            WriteAction::Event
        }
        UplinkResponse::Special(special) => WriteAction::Special(special),
    }
}

impl<F, W> Uplinks<F>
where
    F: Fn(RemoteSender, BytesMut, WriteAction) -> W + Clone,
    W: Future<Output = WriteResult> + Send + 'static,
{
    fn new(node: Text, identity: RoutingAddr, writer: ByteWriter, write_op: F) -> Self {
        let sender = RemoteSender {
            sender: FramedWrite::new(writer, Default::default()),
            identity,
            node,
            prev_lane: None,
            lane: Default::default(),
        };
        Uplinks {
            writer: Some((sender, Default::default())),
            value_uplinks: Default::default(),
            map_uplinks: Default::default(),
            write_queue: Default::default(),
            special_queue: Default::default(),
            write_op,
        }
    }

    fn push(
        &mut self,
        lane_id: u64,
        event: UplinkResponse,
        lane_names: &HashMap<u64, Text>,
    ) -> Result<Option<W>, Utf8Error> {
        let Uplinks {
            writer,
            value_uplinks,
            map_uplinks,
            write_queue,
            special_queue,
            write_op,
        } = self;
        if let Some((mut writer, mut buffer)) = writer.take() {
            let action = write_to_buffer(event, &mut buffer);
            writer.update_lane(lane_id, lane_names);
            Ok(Some(write_op(writer, buffer, action)))
        } else {
            match event {
                UplinkResponse::Value(body) => {
                    let Uplink {
                        queued,
                        backpressure,
                        ..
                    } = value_uplinks.entry(lane_id).or_default();
                    backpressure.push_bytes(body);
                    if !*queued {
                        write_queue.push_back((UplinkKind::Value, lane_id));
                        *queued = true;
                    }
                }
                UplinkResponse::Map(operation) => {
                    let Uplink {
                        queued,
                        backpressure,
                        ..
                    } = map_uplinks.entry(lane_id).or_default();
                    backpressure.push(operation)?;
                    if !*queued {
                        write_queue.push_back((UplinkKind::Map, lane_id));
                        *queued = true;
                    }
                }
                UplinkResponse::SyncedValue(body) => {
                    let Uplink {
                        queued,
                        send_synced,
                        backpressure,
                        ..
                    } = value_uplinks.entry(lane_id).or_default();
                    backpressure.push_bytes(body);
                    *send_synced = true;
                    if !*queued {
                        write_queue.push_back((UplinkKind::Value, lane_id));
                        *queued = true;
                    }
                }
                UplinkResponse::SyncedMap => {
                    let Uplink {
                        queued,
                        send_synced,
                        ..
                    } = map_uplinks.entry(lane_id).or_default();
                    *send_synced = true;
                    if !*queued {
                        write_queue.push_back((UplinkKind::Map, lane_id));
                        *queued = true;
                    }
                }
                UplinkResponse::Special(special) => {
                    if let SpecialUplinkAction::Unlinked(id, _) = &special {
                        value_uplinks.remove(id);
                        map_uplinks.remove(id);
                    }
                    special_queue.push_back(special);
                }
            }
            Ok(None)
        }
    }

    fn replace_and_pop(&mut self, sender: RemoteSender, mut buffer: BytesMut) -> Option<W> {
        let Uplinks {
            writer,
            value_uplinks,
            map_uplinks,
            write_queue,
            special_queue,
            write_op,
        } = self;
        debug_assert!(writer.is_none());
        if let Some(special) = special_queue.pop_front() {
            Some(write_op(sender, buffer, WriteAction::Special(special)))
        } else if let Some((kind, lane_id)) = write_queue.pop_front() {
            match kind {
                UplinkKind::Value => value_uplinks.get_mut(&lane_id).and_then(
                    |Uplink {
                         queued,
                         send_synced,
                         backpressure,
                     }| {
                        *queued = false;
                        let synced = std::mem::replace(send_synced, false);
                        backpressure.prepare_write(&mut buffer);
                        let action = if synced {
                            WriteAction::EventAndSynced
                        } else {
                            WriteAction::Event
                        };
                        Some(write_op(sender, buffer, action))
                    },
                ),
                UplinkKind::Map => map_uplinks.get_mut(&lane_id).and_then(
                    |Uplink {
                         queued,
                         send_synced,
                         backpressure,
                     }| {
                        let synced = std::mem::replace(send_synced, false);
                        if synced {
                            *queued = false;
                            Some(write_op(
                                sender,
                                buffer,
                                WriteAction::MapSynced(Some(std::mem::take(backpressure))),
                            ))
                        } else {
                            backpressure.prepare_write(&mut buffer);
                            if backpressure.has_data() {
                                write_queue.push_back((UplinkKind::Map, lane_id));
                            } else {
                                *queued = false;
                            }
                            Some(write_op(sender, buffer, WriteAction::Event))
                        }
                    },
                ),
            }
        } else {
            *writer = Some((sender, buffer));
            None
        }
    }

}

struct RemoteWriteTracker<W, F> {
    remotes: HashMap<Uuid, Uplinks<F>>,
    pending_writes: FuturesUnordered<W>,
    write_op: F,
}

impl<W, F> RemoteWriteTracker<W, F>
where
    F: Fn(RemoteSender, BytesMut, WriteAction) -> W + Clone,
    W: Future<Output = WriteResult> + Send + 'static,
{
    fn new(write_op: F) -> Self {
        Self {
            remotes: Default::default(),
            pending_writes: Default::default(),
            write_op,
        }
    }

    fn insert(&mut self, remote_id: Uuid, node: Text, identity: RoutingAddr, writer: ByteWriter) {
        let RemoteWriteTracker {
            remotes, write_op, ..
        } = self;
        remotes.insert(
            remote_id,
            Uplinks::new(node, identity, writer, write_op.clone()),
        );
    }

    fn push_write(
        &mut self,
        lane_names: &HashMap<u64, Text>,
        lane_id: u64,
        response: UplinkResponse,
        target: &Uuid,
    ) {
        let RemoteWriteTracker {
            remotes,
            pending_writes,
            ..
        } = self;
        if let Some(uplink) = remotes.get_mut(target) {
            match uplink.push(lane_id, response, lane_names) {
                Ok(Some(write)) => {
                    pending_writes.push(write);
                }
                Err(_) => {
                    //TODO Log error.
                }
                _ => {}
            }
        }
    }

    fn has_pending(&self) -> bool {
        self.pending_writes.is_empty()
    }

    fn unlink_lane(
        &mut self,
        remote_id: Uuid,
        lane_id: u64,
        lane_names: &HashMap<u64, Text>,
    ) -> Result<(), Utf8Error> {
        let RemoteWriteTracker {
            pending_writes,
            ..
        } = self;
        if let Some(uplinks) = self.remotes.get_mut(&remote_id) {
            if let Some(write) = uplinks.push(
                lane_id,
                UplinkResponse::Special(SpecialUplinkAction::Unlinked(lane_id, Text::empty())),
                lane_names,
            )? {
                pending_writes.push(write);
            }
        }
        Ok(())
    }

    fn replace_and_pop(&mut self, writer: RemoteSender, buffer: BytesMut) {
        let RemoteWriteTracker {
            remotes,
            pending_writes,
            ..
        } = self;
        let id = writer.remote_id();
        if let Some(write) = remotes
            .get_mut(&id)
            .and_then(|uplinks| uplinks.replace_and_pop(writer, buffer))
        {
            pending_writes.push(write);
        }
    }
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

struct WriteTaskState<S, W, F> {
    configuration: WriteTaskConfiguration,
    reg_stream: S,
    links: Links,
    lane_channels: LaneChannels,
    write_tracker: RemoteWriteTracker<W, F>,
}

impl<S, W, F> WriteTaskState<S, W, F>
where
    S: Stream<Item = WriteTaskRegistration> + Unpin,
    F: Fn(RemoteSender, BytesMut, WriteAction) -> W + Clone,
    W: Future<Output = WriteResult> + Send + 'static,
{
    fn new(configuration: WriteTaskConfiguration, reg_stream: S, write_op: F) -> Self {
        WriteTaskState {
            configuration,
            reg_stream,
            links: Default::default(),
            lane_channels: Default::default(),
            write_tracker: RemoteWriteTracker::new(write_op),
        }
    }

    async fn select_next(&mut self, mut timeout_delay: Pin<&mut Sleep>) -> WriteTaskEvent {
        let WriteTaskState {
            reg_stream,
            lane_channels,
            write_tracker,
            links,
            ..
        } = self;

        loop {
            tokio::select! {
                biased;
                maybe_reg = reg_stream.next() => {
                    break if let Some(reg) = maybe_reg {
                        WriteTaskEvent::Registration(reg)
                    } else {
                        WriteTaskEvent::Stop
                    };
                }
                maybe_write_done = write_tracker.pending_writes.next(), if write_tracker.has_pending() => {
                    if let Some((writer, buffer, result)) = maybe_write_done {
                        let remote_id = writer.remote_id();
                        if result.is_ok() {
                            write_tracker.replace_and_pop(writer, buffer);
                        } else {
                            //TODO Log failed uplink.
                            write_tracker.remotes.remove(&remote_id);
                            links.remove_remote(remote_id);
                        }
                    }
                }
                maybe_result = lane_channels.lanes.next(), if !lane_channels.is_empty() => {
                    match maybe_result {
                        Some(Ok((id, response))) =>  {
                            break WriteTaskEvent::Event { id, response };
                        },
                        Some(Err(Failed(lane_id))) => {
                            lane_channels.remove(lane_id);
                            let linked_remotes = links.remove_lane(lane_id);
                            for remote_id in linked_remotes {
                                if write_tracker.unlink_lane(remote_id, 
                                    lane_id, 
                                    &lane_channels.lane_names_rev).is_err() {
                                    todo!("Handle this.");
                                }
                            }
                        }
                        _ => {}
                    }
                }
                _ = &mut timeout_delay => {
                    break WriteTaskEvent::Timeout;
                }
            };
        }
    }

    fn handle_registration(&mut self, reg: WriteTaskRegistration) {
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
                lane_channels.add_endpoint(endpoint);
            }
            WriteTaskRegistration::Remote { id, writer } => {
                write_tracker.insert(id, node_uri.clone(), *identity, writer);
            }
            WriteTaskRegistration::Coord(RwCoorindationMessage::Link { origin, lane }) => {
                match lane_channels.id_for(&lane) {
                    Some(id) if write_tracker.remotes.contains_key(&origin) => {
                        links.insert(id, origin);
                        write_tracker.push_write(
                            &lane_channels.lane_names_rev,
                            id,
                            UplinkResponse::Special(SpecialUplinkAction::Linked(id)),
                            &origin,
                        );
                    }
                    _ => {
                        //TODO Log bad link.
                    }
                }
            }
            WriteTaskRegistration::Coord(RwCoorindationMessage::Unlink { origin, lane }) => {
                if let Some(lane_id) = lane_channels.id_for(&lane) {
                    links.remove(lane_id, origin);
                    let message = Text::new("Link closed.");
                    write_tracker.push_write(
                        &lane_channels.lane_names_rev,
                        lane_id,
                        UplinkResponse::Special(SpecialUplinkAction::Unlinked(lane_id, message)),
                        &origin,
                    );
                }
            }
            WriteTaskRegistration::Coord(RwCoorindationMessage::UnknownLane { origin, path }) => {
                write_tracker.push_write(
                    &lane_channels.lane_names_rev,
                    0,
                    UplinkResponse::Special(SpecialUplinkAction::LaneNotFound(path.lane)),
                    &origin,
                );
            }
            WriteTaskRegistration::Coord(RwCoorindationMessage::BadEnvelope { origin: _, lane: _, error: _  }) => {
                todo!("Handle this.")
            }
        }
    }

    fn handle_event(&mut self, id: u64, response: RawLaneResponse) {
        let WriteTaskState {
            lane_channels,
            links,
            write_tracker,
            ..
        } = self;
        let RawLaneResponse { target, response } = response;
        if let Some(remote_id) = target {
            write_tracker.push_write(&lane_channels.lane_names_rev, id, response, &remote_id);
        } else if let Some(targets) = links.linked_from(id) {
            for (remote_id, response) in targets.iter().zip(std::iter::repeat(response)) {
                write_tracker.push_write(&lane_channels.lane_names_rev, id, response, remote_id);
            }
        }
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
    let mut state = WriteTaskState::new(configuration, reg_stream, perform_write);

    initial_endpoints
        .into_iter()
        .for_each(|endpoint| state.lane_channels.add_endpoint(endpoint));

    pin_mut!(timeout_delay);

    let mut voted = false;

    loop {
        let next = state.select_next(timeout_delay.as_mut()).await;
        match next {
            WriteTaskEvent::Registration(reg) => {
                state.handle_registration(reg);
            }
            WriteTaskEvent::Event { id, response } => {
                if voted {
                    if stop_voter.rescind() {
                        break;
                    }
                    voted = false;
                }
                timeout_delay.as_mut().reset(
                    Instant::now()
                        .checked_add(runtime_config.inactive_timeout)
                        .expect("Timer overflow."),
                );
                state.handle_event(id, response);
            }
            WriteTaskEvent::Timeout => {
                voted = true;
                if stop_voter.vote() {
                    break;
                }
            }
            WriteTaskEvent::Stop => {
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
