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

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{collections::HashMap, num::NonZeroUsize};

use crate::compat::{Operation, RawRequestMessageDecoder, RequestMessage};

use super::{AgentAttachmentRequest, AgentRuntimeRequest, Io};
use bytes::Bytes;
use futures::{
    future::{join, select as fselect, Either},
    stream::{select as sselect, SelectAll},
    SinkExt, Stream, StreamExt,
};
use pin_utils::pin_mut;
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
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct AgentRuntimeConfig {
    pub default_lane_config: LaneConfig,
    /// Size of the queue for accepting new subscribers to a downlink.
    pub attachment_queue_size: NonZeroUsize,
    pub unknown_lane_queue_size: NonZeroUsize,
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
        attachment_rx: mpsc::Receiver<AgentAttachmentRequest>,
        config: AgentRuntimeConfig,
        stopping: trigger::Receiver,
    ) -> AgentRuntimeTask {
        AgentRuntimeTask {
            init: self,
            attachment_rx,
            stopping,
            config,
        }
    }
}

pub struct NoLanes;

impl AgentInitTask {
    pub(super) fn new(
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
    lane_name: Text,
    terminated: bool,
    receiver: FramedRead<ByteReader, D>,
}

type ValueLaneReceiver = LaneReceiver<ValueLaneResponseDecoder>;
type MapLaneReceiver = LaneReceiver<MapLaneResponseDecoder>;

impl<D> LaneReceiver<D> {
    /// Stops a receiver so that it will be removed from a [`SelectAll`] collection.
    fn terminate(&mut self) {
        self.terminated = true;
    }
}
impl LaneReceiver<ValueLaneResponseDecoder> {
    fn value(lane_name: Text, reader: ByteReader) -> Self {
        LaneReceiver {
            lane_name,
            terminated: false,
            receiver: FramedRead::new(reader, Default::default()),
        }
    }
}

impl LaneReceiver<MapLaneResponseDecoder> {
    fn map(lane_name: Text, reader: ByteReader) -> Self {
        LaneReceiver {
            lane_name,
            terminated: false,
            receiver: FramedRead::new(reader, Default::default()),
        }
    }
}

struct Failed(Text);

impl<D: Decoder> Stream for LaneReceiver<D> {
    type Item = Result<D::Item, Failed>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            Poll::Ready(None)
        } else {
            this.receiver
                .poll_next_unpin(cx)
                .map_err(|_| Failed(this.lane_name.clone()))
        }
    }
}

enum LaneResponse {
    ValueEvent(Bytes),
    MapEvent(MapOperation<Bytes, Bytes>),
}

impl AgentRuntimeTask {
    pub async fn run(self) {
        let AgentRuntimeTask {
            init: AgentInit { rx, endpoints },
            attachment_rx,
            stopping,
            config,
        } = self;

        let (write_endpoints, read_endpoints): (Vec<_>, Vec<_>) =
            endpoints.into_iter().map(LaneEndpoint::split).unzip();

        let (read_tx, read_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (write_tx, write_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (unknown_tx, unknown_rx) = mpsc::channel(config.unknown_lane_queue_size.get());

        let (kill_switch_tx, kill_switch_rx) = trigger::trigger();
        let att = attachment_task(
            rx,
            attachment_rx,
            read_tx,
            write_tx,
            stopping.clone(),
            kill_switch_rx,
        );
        let read = read_task(read_endpoints, read_rx, unknown_tx, stopping.clone());
        let write = write_task(write_endpoints, write_rx, unknown_rx, stopping);

        let io = await_io_tasks(read, write, kill_switch_tx);
        join(att, io).await;
    }
}

enum ReadTaskRegistration {
    Lane { name: Text, sender: LaneSender },
    Remote { reader: ByteReader },
}

enum WriteTaskRegistration {
    ValueLane(ValueLaneReceiver),
    MapLane(MapLaneReceiver),
    Remote(ByteWriter),
}

async fn attachment_task(
    runtime: mpsc::Receiver<AgentRuntimeRequest>,
    attachment: mpsc::Receiver<AgentAttachmentRequest>,
    read_tx: mpsc::Sender<ReadTaskRegistration>,
    write_tx: mpsc::Sender<WriteTaskRegistration>,
    stopping: trigger::Receiver,
    kill_switch: trigger::Receiver,
) {
    let combined_stop = fselect(stopping, kill_switch);

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
                        let receiver = LaneReceiver::value(name.clone(), out_rx);
                        write_permit.send(WriteTaskRegistration::ValueLane(receiver));
                    }
                    UplinkKind::Map => {
                        let receiver = LaneReceiver::map(name.clone(), out_rx);
                        write_permit.send(WriteTaskRegistration::MapLane(receiver));
                    }
                }
                let _ = promise.send(Ok((in_rx, out_tx)));
            }
            Either::Left(_) => todo!("Opening downlinks form agents not implemented."),
            Either::Right(AgentAttachmentRequest { io: (rx, tx) }) => {
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
                write_permit.send(WriteTaskRegistration::Remote(tx));
            }
        }
    }
}

type RemoteReceiver = FramedRead<ByteReader, RawRequestMessageDecoder>;

fn remote_receiver(reader: ByteReader) -> RemoteReceiver {
    RemoteReceiver::new(reader, Default::default())
}

async fn read_task(
    initial_endpoints: Vec<LaneEndpoint<ByteWriter>>,
    reg_rx: mpsc::Receiver<ReadTaskRegistration>,
    coord_tx: mpsc::Sender<RwCoorindationMessage>,
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
            //TODO Timeout on empty.
            if let (Some(reg), _) = immediate_or_join(reg_stream.next(), flush).await {
                Either::Left(reg)
            } else {
                break;
            }
        } else {
            let select_next = fselect(reg_stream.next(), remotes.next());
            let (result, _) = immediate_or_join(select_next, flush).await;
            match result {
                Either::Left((Some(reg), _)) => Either::Left(reg),
                Either::Left((_, _)) => {
                    break;
                }
                Either::Right((Some(Ok(envelope)), _)) => Either::Right(envelope),
                Either::Right((Some(Err(_e)), _)) => {
                    todo!("Log error");
                }
                Either::Right((_, _)) => {
                    continue;
                }
            }
        };
        match next {
            Either::Left(reg) => match reg {
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
            Either::Right(msg) => {
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
                                if coord_tx
                                    .send(RwCoorindationMessage::Link { origin, lane })
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
                                        if coord_tx
                                            .send(RwCoorindationMessage::BadEnvelope {
                                                origin,
                                                lane,
                                                error,
                                            })
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
                                if coord_tx
                                    .send(RwCoorindationMessage::Unlink { origin, lane })
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
                    let send_err = coord_tx.send(RwCoorindationMessage::UnknownLane {
                        origin: origin.into(),
                        path,
                    });
                    let (_, result) = join(flush, send_err).await;
                    if result.is_err() {
                        break;
                    }
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

async fn write_task(
    initial_endpoints: Vec<LaneEndpoint<ByteReader>>,
    reg_rx: mpsc::Receiver<WriteTaskRegistration>,
    unknown_rx: mpsc::Receiver<RwCoorindationMessage>,
    stopping: trigger::Receiver,
) {
    todo!()
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
