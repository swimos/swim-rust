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

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    num::NonZeroUsize,
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use futures::{stream::FuturesUnordered, Future, StreamExt};
use swimos_agent_protocol::encoding::command::RawCommandMessageDecoder;
use swimos_agent_protocol::CommandMessage;
use swimos_api::{
    address::{Address, RelativeAddress},
    error::{AgentRuntimeError, DownlinkRuntimeError},
};
use swimos_messages::protocol::{RawRequestMessageEncoder, RequestMessage};
use swimos_model::Text;
use swimos_remote::SchemeHostPort;
use swimos_utilities::{
    byte_channel::{byte_channel, ByteReader, ByteWriter},
    encoding::BytesStr,
    errors::Recoverable,
    future::{RetryStrategy, UnionFuture4},
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
    agent::{CommanderKey, CommanderRequest, DownlinkRequest, LinkRequest},
    Io,
};

use super::{CommandChannelRequest, ExternalLinkRequest};

#[cfg(test)]
mod tests;

/// Sender to write outgoing frames to to remotes connected to the agent.
#[derive(Debug)]
struct CmdChannelWriter {
    sender: ByteWriter,
    buffer: BytesMut,
}

impl CmdChannelWriter {
    fn new(sender: ByteWriter) -> Self {
        CmdChannelWriter {
            sender,
            buffer: BytesMut::new(),
        }
    }

    /// Replace the send buffer with a filled buffer (used when sending commands for a single
    /// target).
    fn swap_buffer(&mut self, buffer: &mut BytesMut) {
        self.buffer.clear();
        std::mem::swap(&mut self.buffer, buffer);
    }

    /// Append the contents of another buffer to the send buffer (used when sending commands
    /// to multiple targets).
    fn append_buffer(&mut self, buffer: &mut BytesMut) {
        self.buffer.put(buffer)
    }

    /// Send the contents of the buffer.
    async fn send_commands<'a>(mut self) -> Result<Self, std::io::Error> {
        let CmdChannelWriter { sender, buffer } = &mut self;
        sender.write_all(buffer).await?;
        Ok(self)
    }
}

/// Buffer to collect commands destined for a single target.
#[derive(Debug, Default)]
struct LaneBuffer {
    buffer: BytesMut, // Buffer containing one or or more records.
    offset: usize,    // The offset in the buffer of the last written record.
}

/// State to keep track of the commands being sent to a single endpoint. For remote targets
/// this will be attached to a socket and will track multiple lanes. For a local target it
/// will have a direct connection and will track only a single lane.
#[derive(Debug)]
struct CommandOutput {
    identity: Uuid,                             // ID of the agent sending commands.
    count: usize,                               // Running counter of IDs for targets.
    writer: Option<CmdChannelWriter>,           // Sender for the output channel.
    ids: HashMap<RelativeAddress<Text>, usize>, // Mapping from target paths to IDs.
    lane_buffers: HashMap<usize, LaneBuffer>,   // Mapping from IDs to data buffers.
    dirty: Vec<usize>, // Targets that have been written to since the last write was scheduled.
    retry_strategy: RetryStrategy, // Retry strategy to use when establishing the outgoing channel.
    last_used: Instant, // The last time new data was provided (for timing out the output).
}

pub type PendingWrites = Vec<(RelativeAddress<Text>, BytesMut)>;

enum RetryResult {
    Stop,
    Immediate,
    Delayed(Duration),
}

impl CommandOutput {
    /// Create a new output tracker.
    /// # Arguments
    /// * `identity` - The unique ID of the agent that owns this task.
    /// * `strategy - Retry strategy to use for establishing the outgoing connection.
    fn new(identity: Uuid, strategy: RetryStrategy) -> Self {
        CommandOutput {
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

    /// Replace the writer when the connection is initially established or when a write
    /// completes.
    fn replace_writer(&mut self, sender: CmdChannelWriter) {
        self.writer = Some(sender);
        self.retry_strategy.reset();
    }

    /// If establishing the connection fails, determine whether to retry.
    fn retry(&mut self) -> RetryResult {
        match self.retry_strategy.next() {
            Some(Some(t)) => RetryResult::Delayed(t),
            Some(_) => RetryResult::Immediate,
            _ => RetryResult::Stop,
        }
    }

    /// Check if the output has timed out.
    fn timed_out(&self, timeout: Duration) -> bool {
        let CommandOutput {
            writer, last_used, ..
        } = self;
        let now = Instant::now();
        writer.is_some() && now.duration_since(*last_used) >= timeout
    }

    /// Get the ID and output buffer for a target path (creating a new entry if it does not exist).
    fn get_buffer<'a>(&'a mut self, key: &RelativeAddress<Text>) -> (usize, &'a mut LaneBuffer) {
        let CommandOutput {
            count,
            ids,
            lane_buffers,
            ..
        } = self;
        let i = if let Some(i) = ids.get(key) {
            *i
        } else {
            let i = *count;
            *count += 1;
            ids.insert(key.clone(), i);
            i
        };
        (
            i,
            match lane_buffers.entry(i) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(Default::default()),
            },
        )
    }

    /// Append a command for specified target.
    fn append(&mut self, key: &RelativeAddress<Text>, body: &[u8], overwrite_permitted: bool) {
        let id = self.identity;
        let (i, LaneBuffer { buffer, offset, .. }) = self.get_buffer(key);
        let borrowed_key = key.borrow_parts::<str>();
        let message = RequestMessage::command(id, borrowed_key, body);
        buffer.truncate(*offset);
        let off = buffer.len();
        let mut encoder = RawRequestMessageEncoder;
        encoder
            .encode(message, buffer)
            .expect("Encoding should be infallible.");
        if overwrite_permitted {
            *offset = off;
        } else {
            *offset = buffer.len();
        }
        self.last_used = Instant::now();
        self.dirty.push(i);
    }

    /// If the write is present, create a future that will write all pending commands to the
    /// output channel.
    fn write(
        &mut self,
    ) -> Option<impl Future<Output = Result<CmdChannelWriter, std::io::Error>> + 'static> {
        let CommandOutput {
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
            (w, None) | (w @ None, _) => {
                *writer = w;
                None
            }
            (Some(mut writer), Some(lane_buffer)) if l == 1 => {
                let LaneBuffer { buffer, offset } = lane_buffer;
                writer.swap_buffer(buffer);
                *offset = 0;
                dirty.clear();
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

    /// Close the output tracker returning all pending writes.
    fn into_pending(self) -> PendingWrites {
        let CommandOutput {
            ids,
            mut lane_buffers,
            dirty,
            ..
        } = self;
        let dirty_set = dirty.into_iter().collect::<HashSet<_>>();
        ids.into_iter()
            .filter(|(_, v)| dirty_set.contains(v))
            .filter_map(|(k, v)| {
                lane_buffers
                    .remove(&v)
                    .map(move |lane_buffer| (k, lane_buffer.buffer))
            })
            .collect()
    }
}

type RawReader = FramedRead<ByteReader, RawCommandMessageDecoder<BytesStr>>;

#[derive(Debug)]
enum LinksTaskEvent {
    Request(ExternalLinkRequest),
    Command(CommandMessage<BytesStr, BytesMut>),
    NewChannel(
        KeyOrId,
        Result<Result<ByteWriter, DownlinkRuntimeError>, oneshot::error::RecvError>,
    ),
    DownlinkResult {
        result: Result<Result<Io, DownlinkRuntimeError>, oneshot::error::RecvError>,
        request: DownlinkRequest,
        retry: RetryStrategy,
    },
    WriteDone(KeyOrId, Result<CmdChannelWriter, std::io::Error>),
    Timeout(KeyOrId),
}

/// The state of the external links task. This is public as it must be passed between from the
/// agent initialization task to the agent runtime task.
#[derive(Debug)]
pub struct LinksTaskState {
    reader: Option<RawReader>,
    outputs: HashMap<CommanderKey, CommandOutput>,
    link_requests: mpsc::Sender<LinkRequest>,
}

impl LinksTaskState {
    pub fn new(link_requests: mpsc::Sender<LinkRequest>) -> Self {
        LinksTaskState {
            reader: Default::default(),
            outputs: Default::default(),
            link_requests,
        }
    }
}

/// Configuration parameters for the external links task.
#[derive(Debug)]
pub struct LinksTaskConfig {
    /// Buffer size for the channel between the agent runtime and agent implementation.
    pub buffer_size: NonZeroUsize,
    /// Retry strategy for establishing remote connections to send commands.
    pub retry_strategy: RetryStrategy,
    /// If an output channel receives no new commands for this time period, it will be closed.
    pub timeout_delay: Duration,
}

pub trait ReportFailed {
    fn failed(&mut self, pending: PendingWrites);
}

#[derive(Default, Debug, Clone, Copy)]
pub struct NoReport;

impl ReportFailed for NoReport {
    fn failed(&mut self, _pending: PendingWrites) {}
}

/// A task that manages the external links opened by the agent, including the establishment of
/// downlinks and sending of ad hoc commands.
///
/// # Arguments
/// * `identity` - The unique ID of this agent instance.
/// * `open_requests` - Requests for the agent implementation to create a channel for sending ad-hoc commands.
/// * `state` - The state of the task. For agent initialization this should be empty. This is then passed from
///    the initialization task to the runtime task.
/// * `config` - Configuration parameters for the task.
/// * `report_failed` - Callback to report commands that were still pending when an output channel failed.
pub async fn external_links_task<F: ReportFailed>(
    identity: Uuid,
    mut open_requests: mpsc::Receiver<ExternalLinkRequest>,
    state: LinksTaskState,
    config: LinksTaskConfig,
    mut report_failed: Option<F>,
) -> LinksTaskState {
    let LinksTaskState {
        mut reader,
        mut outputs,
        link_requests,
    } = state;
    let LinksTaskConfig {
        buffer_size,
        retry_strategy,
        timeout_delay,
    } = config;
    let mut pending = FuturesUnordered::new();
    let mut commander_ids = CommanderIds::default();

    loop {
        let event = if let Some(rx) = reader.as_mut() {
            tokio::select! {
                biased;
                maybe_req = open_requests.recv() => {
                    if let Some(request) = maybe_req {
                        LinksTaskEvent::Request(request)
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
                        LinksTaskEvent::Command(msg)
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
                        LinksTaskEvent::Request(request)
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
            LinksTaskEvent::Request(ExternalLinkRequest::Command(CommandChannelRequest {
                promise,
            })) => {
                let (tx, rx) = byte_channel(buffer_size);
                if promise.send(Ok(tx)).is_ok() {
                    debug!(identity = %identity, "Attaching a new ad hoc command channel.");
                    reader = Some(FramedRead::new(rx, Default::default()));
                } else {
                    debug!(identity = %identity, "The agent dropped its request for an ad hoc command channel before it was completed.");
                }
            }
            LinksTaskEvent::Request(ExternalLinkRequest::Downlink(req)) => {
                pending.push(UnionFuture4::fourth(try_open_downlink(
                    None,
                    req,
                    link_requests.clone(),
                    retry_strategy,
                )));
            }
            LinksTaskEvent::Command(CommandMessage::Register { address, id }) => {
                trace!(identify = %identity, address = %address, "Handling a commander endpoint registration request for an agent.");
                let Address { host, node, lane } = address;
                let remote = match host
                    .as_ref()
                    .map(|h| h.as_ref().parse::<SchemeHostPort>())
                    .transpose()
                {
                    Ok(remote) => remote,
                    _ => {
                        error!(host = ?host, "Invalid host specified for ad-hoc message.");
                        continue;
                    }
                };

                let rel_addr =
                    RelativeAddress::new(Text::new(node.as_str()), Text::new(lane.as_str()));

                let endpoint = CommanderEndpoint::new(remote, rel_addr);
                commander_ids.set_id(endpoint, id);
            }
            LinksTaskEvent::Command(CommandMessage::Registered {
                target: id,
                command,
                overwrite_permitted,
            }) => {
                if let Some(endpoint) = commander_ids.endpoint_for(id) {
                    if let Some(output) = outputs.get_mut(endpoint.key()) {
                        output.append(endpoint.address(), &command, overwrite_permitted);
                        if let Some(fut) = output.write() {
                            pending.push(UnionFuture4::first(wrap_result(KeyOrId::Id(id), fut)));
                        } else {
                            pending.push(UnionFuture4::third(output_timeout(
                                KeyOrId::Id(id),
                                timeout_delay,
                            )))
                        }
                    } else {
                        let mut output = CommandOutput::new(identity, retry_strategy);
                        output.append(endpoint.address(), &command, overwrite_permitted);
                        outputs.insert(endpoint.key().clone(), output);
                        let fut = try_open_new(
                            identity,
                            endpoint.key().clone(),
                            Some(id),
                            link_requests.clone(),
                            None,
                        );
                        pending.push(UnionFuture4::second(fut));
                    }
                } else {
                    error!(
                        id,
                        "Received a message for an ID before its registration has been received."
                    );
                }
            }
            LinksTaskEvent::Command(CommandMessage::Addressed {
                target: address,
                command,
                overwrite_permitted,
            }) => {
                trace!(identify = %identity, address = %address, overwrite_permitted, "Handling an ad hoc command for an agent.");
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
                let addr = RelativeAddress::text(node.as_str(), lane.as_str());
                if let Some(output) = outputs.get_mut(&key) {
                    output.append(&addr, &command, overwrite_permitted);
                    if let Some(fut) = output.write() {
                        pending.push(UnionFuture4::first(wrap_result(KeyOrId::Key(key), fut)));
                    } else {
                        pending.push(UnionFuture4::third(output_timeout(
                            KeyOrId::Key(key),
                            timeout_delay,
                        )))
                    }
                } else {
                    let mut output = CommandOutput::new(identity, retry_strategy);
                    output.append(&addr, &command, overwrite_permitted);
                    outputs.insert(key.clone(), output);
                    let fut = try_open_new(identity, key, None, link_requests.clone(), None);
                    pending.push(UnionFuture4::second(fut));
                }
            }
            LinksTaskEvent::NewChannel(key_or_id, Ok(Ok(channel))) => {
                if let Some(output) = outputs.get_mut(commander_ids.key(&key_or_id)) {
                    debug!(identity = %identity, key = ?key_or_id, "Registered a new outgoing ad hoc command channel.");
                    output.replace_writer(CmdChannelWriter::new(channel));
                    if let Some(fut) = output.write() {
                        pending.push(UnionFuture4::first(wrap_result(key_or_id, fut)));
                    } else {
                        pending.push(UnionFuture4::third(output_timeout(
                            key_or_id,
                            timeout_delay,
                        )))
                    }
                }
            }
            LinksTaskEvent::NewChannel(key_or_id, Ok(Err(err))) => {
                if matches!(err, DownlinkRuntimeError::RuntimeError(_)) {
                    debug!(identity = %identity, "Stopping after the link request channel was dropped.");
                    break;
                }
                let key = commander_ids.key(&key_or_id);
                if let Some(output) = outputs.get_mut(key) {
                    if err.is_fatal() {
                        error!(error = %err, "Opening a new ad hoc command channel failed with a fatal error.");
                        if let (Some(output), Some(reporter)) =
                            (outputs.remove(key), report_failed.as_mut())
                        {
                            reporter.failed(output.into_pending());
                        }
                    } else {
                        match output.retry() {
                            RetryResult::Stop => {
                                error!(error = %err, "Opening a new ad hoc command channel failed after retry attempts exhausted.");
                                if let (Some(output), Some(reporter)) =
                                    (outputs.remove(key), report_failed.as_mut())
                                {
                                    reporter.failed(output.into_pending());
                                }
                            }
                            RetryResult::Immediate => {
                                let (key, id) = commander_ids.key_and_id(key_or_id);
                                error!(error = %err, "Opening a new ad hoc command channel failed. Retrying immediately.");
                                let fut =
                                    try_open_new(identity, key, id, link_requests.clone(), None);
                                pending.push(UnionFuture4::second(fut));
                            }
                            RetryResult::Delayed(t) => {
                                let (key, id) = commander_ids.key_and_id(key_or_id);
                                error!(error = %err, delay = ?t, "Opening a new ad hoc command channel failed. Retrying after a delay.");
                                let fut =
                                    try_open_new(identity, key, id, link_requests.clone(), Some(t));
                                pending.push(UnionFuture4::second(fut));
                            }
                        }
                    }
                }
            }
            LinksTaskEvent::NewChannel(key_or_id, _) => {
                outputs.remove(commander_ids.key(&key_or_id));
                debug!("The server dropped a request to open a command channel.");
            }
            LinksTaskEvent::WriteDone(key_or_id, result) => {
                let key = commander_ids.key(&key_or_id);
                if let Some(output) = outputs.get_mut(key) {
                    match result {
                        Ok(writer) => {
                            trace!(identify = %identity, key = ?key, "Completed writing an ad hoc command.");
                            output.replace_writer(writer);
                            if let Some(fut) = output.write() {
                                pending.push(UnionFuture4::first(wrap_result(key_or_id, fut)));
                            } else {
                                pending.push(UnionFuture4::third(output_timeout(
                                    key_or_id,
                                    timeout_delay,
                                )))
                            }
                        }
                        Err(err) => {
                            error!(error = %err, "Writing ad hoc command to channel failed.");
                            outputs.remove(key);
                        }
                    }
                }
            }
            LinksTaskEvent::Timeout(key_or_id) => {
                let key = commander_ids.key(&key_or_id);
                if let Some(output) = outputs.get(key) {
                    if output.timed_out(timeout_delay) {
                        debug!(identify = %identity, key = ?key, "Ad hoc output channel closed after a period of inactivity.");
                        outputs.remove(key);
                    }
                }
            }
            LinksTaskEvent::DownlinkResult {
                result: Err(_),
                request,
                ..
            } => {
                debug!("The server dropped a request to open a downlink.");
                if request
                    .promise
                    .send(Err(DownlinkRuntimeError::RuntimeError(
                        AgentRuntimeError::Stopping,
                    )))
                    .is_err()
                {
                    debug!("A request for a downlink was dropped.");
                }
            }
            LinksTaskEvent::DownlinkResult {
                result: Ok(result),
                request,
                mut retry,
            } => match result {
                Ok(_) => {
                    if request.promise.send(result).is_err() {
                        debug!("A request for a downlink was dropped.");
                    }
                }
                Err(DownlinkRuntimeError::RuntimeError(_)) => {
                    debug!(identity = %identity, "Stopping after the link request channel was dropped.");
                    break;
                }
                Err(e) => match retry.next() {
                    Some(delay) if !e.is_fatal() => {
                        pending.push(UnionFuture4::fourth(try_open_downlink(
                            delay,
                            request,
                            link_requests.clone(),
                            retry,
                        )));
                    }
                    _ => {
                        if request.promise.send(Err(e)).is_err() {
                            debug!("A request for a downlink was dropped.");
                        }
                    }
                },
            },
        }
    }
    LinksTaskState {
        reader,
        outputs,
        link_requests,
    }
}

/// Either the key identifying a command endpoint or and ID indicated that a key has been
/// registered.
#[derive(Debug)]
enum KeyOrId {
    /// Explicit key for an ad hoc command.
    Key(CommanderKey),
    /// ID for a registered point to point command channel.
    Id(u16),
}

async fn wrap_result<F>(key_or_id: KeyOrId, f: F) -> LinksTaskEvent
where
    F: Future<Output = Result<CmdChannelWriter, std::io::Error>>,
{
    let result = f.await;
    LinksTaskEvent::WriteDone(key_or_id, result)
}

async fn try_open_new(
    agent_id: Uuid,
    key: CommanderKey,
    id: Option<u16>,
    link_requests: mpsc::Sender<LinkRequest>,
    delay: Option<Duration>,
) -> LinksTaskEvent {
    if let Some(delay) = delay {
        trace!(delay = ?delay, "Waiting before next connection attempt.");
        tokio::time::sleep(delay).await;
    }
    let (tx, rx) = oneshot::channel();
    let req = CommanderRequest::new(agent_id, key.clone(), tx);
    let key_or_id = id
        .map(KeyOrId::Id)
        .unwrap_or_else(move || KeyOrId::Key(key));
    if link_requests
        .send(LinkRequest::Commander(req))
        .await
        .is_ok()
    {
        let result = rx.await;
        LinksTaskEvent::NewChannel(key_or_id, result)
    } else {
        LinksTaskEvent::NewChannel(
            key_or_id,
            Ok(Err(DownlinkRuntimeError::RuntimeError(
                AgentRuntimeError::Stopping,
            ))),
        )
    }
}

async fn output_timeout(key_or_id: KeyOrId, delay: Duration) -> LinksTaskEvent {
    tokio::time::sleep(delay).await;
    LinksTaskEvent::Timeout(key_or_id)
}

async fn try_open_downlink(
    delay: Option<Duration>,
    request: DownlinkRequest,
    link_requests: mpsc::Sender<LinkRequest>,
    retry: RetryStrategy,
) -> LinksTaskEvent {
    if let Some(delay) = delay {
        trace!(delay = ?delay, "Waiting before next attempt to establish downlink.");
        tokio::time::sleep(delay).await;
    }
    let (tx, rx) = oneshot::channel();
    let req = request.replace_promise(tx);
    let result = if link_requests
        .send(LinkRequest::Downlink(req))
        .await
        .is_err()
    {
        Ok(Err(DownlinkRuntimeError::RuntimeError(
            AgentRuntimeError::Stopping,
        )))
    } else {
        rx.await
    };
    LinksTaskEvent::DownlinkResult {
        result,
        request,
        retry,
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct CommanderEndpoint {
    key: CommanderKey,
    address: Option<RelativeAddress<Text>>,
}

impl CommanderEndpoint {
    fn new(remote: Option<SchemeHostPort>, address: RelativeAddress<Text>) -> Self {
        if let Some(shp) = remote {
            CommanderEndpoint {
                key: CommanderKey::Remote(shp),
                address: Some(address),
            }
        } else {
            CommanderEndpoint {
                key: CommanderKey::Local(address),
                address: None,
            }
        }
    }
}

impl CommanderEndpoint {
    fn key(&self) -> &CommanderKey {
        &self.key
    }

    fn address(&self) -> &RelativeAddress<Text> {
        match self {
            CommanderEndpoint {
                key: CommanderKey::Local(addr),
                ..
            } => addr,
            CommanderEndpoint {
                address: Some(addr),
                ..
            } => addr,
            _ => unreachable!("An endpoint always has an address."),
        }
    }
}

/// Assignment of integer IDs to commander endpoints.
#[derive(Default, Debug)]
struct CommanderIds {
    commander_ids: HashMap<CommanderEndpoint, u16>,
    commander_endpoints: HashMap<u16, CommanderEndpoint>,
}

impl CommanderIds {
    /// Set the ID associated with an endpoint.
    fn set_id(&mut self, endpoint: CommanderEndpoint, id: u16) {
        let CommanderIds {
            commander_ids,
            commander_endpoints,
            ..
        } = self;
        commander_endpoints.insert(id, endpoint.clone());
        commander_ids.insert(endpoint, id);
    }

    /// Get the endpoint associated with an ID.
    fn endpoint_for(&self, id: u16) -> Option<&CommanderEndpoint> {
        self.commander_endpoints.get(&id)
    }

    /// Find the key for an outgoing record.
    fn key<'a>(&'a self, key_or_id: &'a KeyOrId) -> &'a CommanderKey {
        match key_or_id {
            KeyOrId::Key(k) => k,
            KeyOrId::Id(id) => {
                &self
                    .commander_endpoints
                    .get(id)
                    .expect("ID not registered.")
                    .key
            }
        }
    }

    /// Get the key, and optionally registered ID for the associated request, for opening a new connection.
    fn key_and_id(&self, key_or_id: KeyOrId) -> (CommanderKey, Option<u16>) {
        match key_or_id {
            KeyOrId::Key(k) => (k, None),
            KeyOrId::Id(id) => (
                self.commander_endpoints
                    .get(&id)
                    .expect("ID not registered.")
                    .key
                    .clone(),
                Some(id),
            ),
        }
    }
}
