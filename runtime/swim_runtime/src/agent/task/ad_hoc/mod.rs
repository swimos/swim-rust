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

use std::{
    collections::{hash_map::Entry, HashMap},
    num::NonZeroUsize,
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use futures::{future::Either, stream::FuturesUnordered, Future, FutureExt, StreamExt};
use swim_api::{
    error::{AgentRuntimeError, DownlinkRuntimeError},
    protocol::{
        agent::{AdHocCommand, AdHocCommandDecoder},
        WithLengthBytesCodec,
    },
};
use swim_messages::protocol::{RawRequestMessageEncoder, RequestMessage};
use swim_model::{
    address::{Address, RelativeAddress},
    BytesStr, Text,
};
use swim_utilities::{
    future::retryable::RetryStrategy,
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
};
use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_util::codec::{Encoder, FramedRead};
use tracing::{debug, error, trace};
use uuid::Uuid;

use crate::{
    agent::{CommanderKey, CommanderRequest, LinkRequest},
    net::SchemeHostPort,
};

use super::AdHocChannelRequest;

/// Sender to write outgoing frames to to remotes connected to the agent.
#[derive(Debug)]
pub struct AdHocSender {
    sender: ByteWriter,
    buffer: BytesMut,
}

impl AdHocSender {
    fn new(sender: ByteWriter) -> Self {
        AdHocSender {
            sender,
            buffer: BytesMut::new(),
        }
    }

    fn swap_buffer(&mut self, buffer: &mut BytesMut) {
        self.buffer.clear();
        std::mem::swap(&mut self.buffer, buffer);
    }

    fn append_buffer(&mut self, buffer: &mut BytesMut) {
        self.buffer.put(buffer)
    }

    async fn send_commands<'a>(mut self) -> Result<Self, std::io::Error> {
        let AdHocSender { sender, buffer } = &mut self;
        sender.write_all(buffer).await?;
        Ok(self)
    }
}

#[derive(Debug, Default)]
struct LaneBuffer {
    buffer: BytesMut,
    offset: usize,
}

#[derive(Debug)]
struct AdHocOutput {
    identity: Uuid,
    count: usize,
    writer: Option<AdHocSender>,
    ids: HashMap<RelativeAddress<Text>, usize>,
    lane_buffers: HashMap<usize, LaneBuffer>,
    dirty: Vec<usize>,
    retry_strategy: RetryStrategy,
    last_used: Instant,
}

enum RetryResult {
    Stop,
    Immediate,
    Delayed(Duration),
}

impl AdHocOutput {
    fn new(identity: Uuid, strategy: RetryStrategy) -> Self {
        AdHocOutput {
            identity,
            retry_strategy: strategy,
            count: 0,
            writer: None,
            ids: Default::default(),
            lane_buffers: Default::default(),
            dirty: Default::default(),
            last_used: Instant::now(),
        }
    }

    fn replace_writer(&mut self, sender: AdHocSender) {
        self.writer = Some(sender);
        self.retry_strategy.reset();
        self.last_used = Instant::now();
    }

    fn retry(&mut self) -> RetryResult {
        match self.retry_strategy.next() {
            Some(Some(t)) => RetryResult::Delayed(t),
            Some(_) => RetryResult::Immediate,
            _ => RetryResult::Stop,
        }
    }

    fn timed_out(&self, timeout: Duration) -> bool {
        let AdHocOutput {
            writer, last_used, ..
        } = self;
        let now = Instant::now();
        writer.is_some() && now.duration_since(*last_used) >= timeout
    }

    fn get_buffer(&mut self, key: &RelativeAddress<Text>) -> (usize, &mut LaneBuffer) {
        let AdHocOutput {
            count,
            ids,
            lane_buffers,
            ..
        } = self;
        let i = match ids.entry(key.clone()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let i = *count;
                *count += 1;
                *entry.insert(i)
            }
        };
        (
            i,
            match lane_buffers.entry(i) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(Default::default()),
            },
        )
    }

    fn append(
        &mut self,
        key: RelativeAddress<Text>,
        body: &mut BytesMut,
        overwrite_permitted: bool,
    ) {
        let id = self.identity;
        let (i, LaneBuffer { buffer, offset, .. }) = self.get_buffer(&key);
        let addr = RelativeAddress::new(key.node.as_str(), key.lane.as_str());
        let message = RequestMessage::command(id, addr, body.as_ref());
        buffer.truncate(*offset);
        let off = buffer.len();
        let mut encoder = RawRequestMessageEncoder::default();
        encoder
            .encode(message, buffer)
            .expect("Encoding should be infallible.");
        if overwrite_permitted {
            *offset = off;
        } else {
            *offset = buffer.len();
        }
        self.dirty.push(i);
    }

    fn write(
        &mut self,
    ) -> Option<impl Future<Output = Result<AdHocSender, std::io::Error>> + 'static> {
        let AdHocOutput {
            writer,
            lane_buffers,
            dirty,
            ..
        } = self;
        let l = dirty.len();
        match (
            writer.take(),
            dirty.first().and_then(|i| lane_buffers.get_mut(i)),
        ) {
            (_, None) | (None, _) => None,
            (Some(mut writer), Some(lane_buffer)) if l == 1 => {
                let LaneBuffer { buffer, offset } = lane_buffer;
                writer.swap_buffer(buffer);
                *offset = 0;
                Some(writer.send_commands())
            }
            (Some(mut writer), _) => {
                for i in dirty.drain(..) {
                    if let Some(LaneBuffer { buffer, offset }) = lane_buffers.get_mut(&i) {
                        writer.append_buffer(buffer);
                        buffer.clear();
                        *offset = 0;
                    }
                }
                Some(writer.send_commands())
            }
        }
    }
}

type AdHocReader = FramedRead<ByteReader, AdHocCommandDecoder<BytesStr, WithLengthBytesCodec>>;

enum AdHocEvent {
    Request(AdHocChannelRequest),
    Command(AdHocCommand<BytesStr, BytesMut>),
    NewChannel(
        CommanderKey,
        Result<Result<ByteWriter, DownlinkRuntimeError>, oneshot::error::RecvError>,
    ),
    WriteDone(CommanderKey, Result<AdHocSender, std::io::Error>),
    Timeout(CommanderKey),
}

#[derive(Debug)]
pub struct AdHocTaskState {
    reader: Option<AdHocReader>,
    outputs: HashMap<CommanderKey, AdHocOutput>,
    link_requests: mpsc::Sender<LinkRequest>,
}

impl AdHocTaskState {
    pub fn new(link_requests: mpsc::Sender<LinkRequest>) -> Self {
        AdHocTaskState {
            reader: Default::default(),
            outputs: Default::default(),
            link_requests,
        }
    }
}

pub struct AdHocTaskConfig {
    pub buffer_size: NonZeroUsize,
    pub retry_strategy: RetryStrategy,
    pub timeout_delay: Duration,
}

pub async fn ad_hoc_commands_task(
    identity: Uuid,
    mut open_requests: mpsc::Receiver<AdHocChannelRequest>,
    state: AdHocTaskState,
    config: AdHocTaskConfig,
) -> AdHocTaskState {
    let AdHocTaskState {
        mut reader,
        mut outputs,
        link_requests,
    } = state;
    let AdHocTaskConfig {
        buffer_size,
        retry_strategy,
        timeout_delay,
    } = config;
    let mut pending = FuturesUnordered::new();

    loop {
        let event = if let Some(rx) = reader.as_mut() {
            tokio::select! {
                biased;
                maybe_req = open_requests.recv() => {
                    if let Some(request) = maybe_req {
                        AdHocEvent::Request(request)
                    } else {
                        debug!(identity = %identity, "Stopping after the request channel terminated.");
                        break;
                    }
                }
                maybe_result = pending.next(), if !pending.is_empty() => {
                    if let Some(result) = maybe_result {
                        result
                    } else {
                        continue;
                    }
                },
                maybe_msg = rx.next() => {
                    if let Some(Ok(msg)) = maybe_msg {
                        AdHocEvent::Command(msg)
                    } else {
                        debug!(identity = %identity, "The agent dropped its ad hoc command channel.");
                        reader = None;
                        continue;
                    }
                },
            }
        } else {
            tokio::select! {
                biased;
                maybe_req = open_requests.recv() => {
                    if let Some(request) = maybe_req {
                        AdHocEvent::Request(request)
                    } else {
                        debug!(identity = %identity, "Stopping after the request channel terminated.");
                        break;
                    }
                }
                maybe_result = pending.next(), if !pending.is_empty() => {
                    if let Some(result) = maybe_result {
                        result
                    } else {
                        continue;
                    }
                },
            }
        };
        match event {
            AdHocEvent::Request(AdHocChannelRequest { promise }) => {
                let (tx, rx) = byte_channel(buffer_size);
                if promise.send(Ok(tx)).is_ok() {
                    debug!(identity = %identity, "Attaching a new ad hoc command channel.");
                    reader = Some(FramedRead::new(rx, Default::default()));
                } else {
                    debug!(identity = %identity, "The agent dropped its request for an ad hoc command channel before it was completed.");
                }
            }
            AdHocEvent::Command(AdHocCommand {
                address,
                mut command,
                overwrite_permitted,
            }) => {
                trace!(identify = % identity, address = %address, overwrite_permitted, "Handling an ad hoc command for an agent.");
                let Address { host, node, lane } = &address;
                let key = match host.as_ref().map(|h| h.as_ref().parse::<SchemeHostPort>()) {
                    Some(Ok(shp)) => CommanderKey::Remote(shp),
                    None => {
                        CommanderKey::Local(RelativeAddress::text(node.as_str(), lane.as_str()))
                    }
                    _ => {
                        error!(host = ?host, "Invalid host specified for ad-hoc message.");
                        continue;
                    }
                };
                if let Some(output) = outputs.get_mut(&key) {
                    let addr = RelativeAddress::text(node.as_str(), lane.as_str());
                    output.append(addr, &mut command, overwrite_permitted);
                    if let Some(fut) = output.write() {
                        pending.push(Either::Left(Either::Left(
                            fut.map(move |r| AdHocEvent::WriteDone(key, r)),
                        )));
                    } else {
                        pending.push(Either::Right(output_timeout(key, timeout_delay)))
                    }
                } else {
                    outputs.insert(key.clone(), AdHocOutput::new(identity, retry_strategy));
                    let fut = try_open_new(identity, key, link_requests.clone(), None);
                    pending.push(Either::Left(Either::Right(fut)));
                }
            }
            AdHocEvent::NewChannel(key, Ok(Ok(channel))) => {
                if let Some(output) = outputs.get_mut(&key) {
                    debug!(identity = %identity, key = ?key, "Registered a new outgoing ad hoc command channel.");
                    output.replace_writer(AdHocSender::new(channel));
                }
            }
            AdHocEvent::NewChannel(key, Ok(Err(err))) => {
                if matches!(err, DownlinkRuntimeError::RuntimeError(_)) {
                    debug!(identity = %identity, "Stopping after the link request channel was dropped.");
                    break;
                }
                if let Some(output) = outputs.get_mut(&key) {
                    match output.retry() {
                        RetryResult::Stop => {
                            error!(error = %err, "Opening a new ad hoc command channel failed.");
                            outputs.remove(&key);
                        }
                        RetryResult::Immediate => {
                            error!(error = %err, "Opening a new ad hoc command channel failed. Retrying immediately.");
                            let fut = try_open_new(identity, key, link_requests.clone(), None);
                            pending.push(Either::Left(Either::Right(fut)));
                        }
                        RetryResult::Delayed(t) => {
                            error!(error = %err, delay = ?t, "Opening a new ad hoc command channel failed. Retrying after a delay.");
                            let fut = try_open_new(identity, key, link_requests.clone(), Some(t));
                            pending.push(Either::Left(Either::Right(fut)));
                        }
                    }
                }
            }
            AdHocEvent::NewChannel(key, _) => {
                outputs.remove(&key);
                debug!("A request for a channel was dropped.");
            }
            AdHocEvent::WriteDone(key, result) => {
                if let Some(output) = outputs.get_mut(&key) {
                    match result {
                        Ok(writer) => {
                            trace!(identify = %identity, key = ?key, "Completed writing an ad hoc command.");
                            output.replace_writer(writer);
                        }
                        Err(err) => {
                            error!(error = %err, "Writing ad hoc command to channel failed.");
                            outputs.remove(&key);
                        }
                    }
                }
            }
            AdHocEvent::Timeout(key) => {
                if let Some(output) = outputs.get(&key) {
                    if output.timed_out(timeout_delay) {
                        debug!(identify = %identity, key = ?key, "Ad hoc output channel closed after a period of inactivity.");
                        outputs.remove(&key);
                    }
                }
            }
        }
    }
    AdHocTaskState {
        reader,
        outputs,
        link_requests,
    }
}

async fn try_open_new(
    agent_id: Uuid,
    key: CommanderKey,
    link_requests: mpsc::Sender<LinkRequest>,
    delay: Option<Duration>,
) -> AdHocEvent {
    if let Some(delay) = delay {
        trace!(delay = ?delay, "Waiting before next connection attempt.");
        tokio::time::sleep(delay).await;
    }
    let (tx, rx) = oneshot::channel();
    let req = CommanderRequest::new(agent_id, key.clone(), tx);
    if link_requests
        .send(LinkRequest::Commander(req))
        .await
        .is_ok()
    {
        let result = rx.await;
        AdHocEvent::NewChannel(key, result)
    } else {
        AdHocEvent::NewChannel(
            key,
            Ok(Err(DownlinkRuntimeError::RuntimeError(
                AgentRuntimeError::Stopping,
            ))),
        )
    }
}

async fn output_timeout(key: CommanderKey, delay: Duration) -> AdHocEvent {
    tokio::time::sleep(delay).await;
    AdHocEvent::Timeout(key)
}
