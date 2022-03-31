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

use bytes::Bytes;
use futures::{
    future::{select as fselect, Either},
    stream::{select as sselect, FuturesUnordered},
    SinkExt, Stream, StreamExt,
};
use swim_api::{
    agent::{LaneConfig, UplinkKind},
    protocol::{
        agent::{
            LaneRequest, LaneRequestEncoder, MapLaneResponseDecoder, ValueLaneResponseDecoder,
        },
        map::{
            extract_header, MapMessage, MapMessageEncoder, MapOperation, RawMapOperationEncoder,
        },
        WithLengthBytesCodec,
    }, error::AgentRuntimeError,
};
use swim_model::Text;
use swim_recon::parser::MessageExtractError;
use swim_utilities::io::byte_channel::{self, ByteReader, ByteWriter, byte_channel};
use swim_utilities::trigger;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use super::{AgentAttachmentRequest, AgentRuntimeRequest, Io};

#[derive(Debug, Clone, Copy)]
pub struct AgentRuntimeConfig {
    pub default_lane_config: LaneConfig,
}

pub struct AgentInitTask {
    requests: mpsc::Receiver<AgentRuntimeRequest>,
    init_complete: trigger::Receiver,
    config: AgentRuntimeConfig,
}

pub struct LaneEndpoint {
    name: Text,
    kind: UplinkKind,
    io: Io,
}

pub struct AgentInit {
    rx: mpsc::Receiver<AgentRuntimeRequest>,
    endpoints: Vec<LaneEndpoint>,
}

impl AgentInit {
    pub fn make_runtime_task(
        self,
        config: AgentRuntimeConfig,
        stopping: trigger::Receiver,
    ) -> AgentRuntimeTask {
        AgentRuntimeTask {
            init: self,
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

    async fn start_sync(&mut self, id: u64) -> Result<(), std::io::Error> {
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

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            std::task::Poll::Ready(None)
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
            stopping,
            config,
        } = self;
    }
}

enum ReadTaskRegistration {
    Lane {
        name: Text,
        sender: LaneSender,
    },
    Remote(ByteReader),
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

    let mut remote_id: u64 = 0;
    //let mut pending = FuturesUnordered::new();

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
                if read_tx.send(ReadTaskRegistration::Lane {
                    name: name.clone(),
                    sender,
                }).await.is_err() {
                    let _ = promise.send(Err(AgentRuntimeError::Terminated));
                    break;
                }
                let failed = match kind {
                    UplinkKind::Value => {
                        let receiver = LaneReceiver::value(name.clone(), out_rx);
                        write_tx.send(WriteTaskRegistration::ValueLane(receiver)).await.is_err()
                    }
                    UplinkKind::Map => {
                        let receiver = LaneReceiver::map(name.clone(), out_rx);
                        write_tx.send(WriteTaskRegistration::MapLane(receiver)).await.is_err()
                    }
                };
                if failed {
                    let _ = promise.send(Err(AgentRuntimeError::Terminated));
                    break;
                }
                let _ = promise.send(Ok((in_rx, out_tx)));
                todo!()
            }
            Either::Left(_) => todo!("Opening downlinks form agents not implemented."),
            Either::Right(AgentAttachmentRequest { io: (rx, tx) }) => {}
        }
    }
    todo!()
}
