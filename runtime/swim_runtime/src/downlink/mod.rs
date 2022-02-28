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
use std::num::NonZeroUsize;
use std::time::Duration;

use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::{join, select, Either};
use futures::stream::SelectAll;
use futures::{Future, FutureExt, Sink, SinkExt, Stream, StreamExt};
use pin_utils::pin_mut;
use swim_api::protocol::downlink::{
    DownlinkNotification, DownlinkNotificationEncoder, DownlinkOperation, DownlinkOperationDecoder,
};
use swim_model::path::RelativePath;
use swim_utilities::future::{immediate_or_join, immediate_or_start, SecondaryResult};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};
use tracing::{error, info, info_span, trace, warn};
use tracing_futures::Instrument;

use crate::compat::{
    Notification, Operation, RawRequestMessage, RawRequestMessageEncoder, RawResponseMessage,
    RawResponseMessageDecoder,
};
use crate::routing::RoutingAddr;

mod key;
pub mod map_queue;
#[cfg(test)]
mod tests;

bitflags! {
    /// Flags that a downlink consumer can set to instruct the downlink runtime how it wishes
    /// to be driven.
    pub struct DownlinkOptions: u8 {
        /// The consumer needs to be synchronized with the remote lane.
        const SYNC = 0b01;
        /// If the connection fails, it should be restarted and the consumer passed to the new
        /// connection.
        const KEEP_LINKED = 0b10;
        const DEFAULT = Self::SYNC.bits | Self::KEEP_LINKED.bits;
    }
}

bitflags! {
    /// Internal flags for the downlink runtime write task.
    struct WriteTaskState: u8 {
        /// A new value has been received while a write was pending.
        const UPDATED = 0b001;
        /// The outgoing channel has been flushed.
        const FLUSHED = 0b010;
        /// A new consumer that needs to be synced has joined why a write was pending.
        const NEEDS_SYNC = 0b100;
        /// When the task starts it does not need to be flushed.
        const INIT = Self::FLUSHED.bits;
    }
}

impl WriteTaskState {
    /// If a new consumer needs to be synced, set the appropriate state bit.
    pub fn set_needs_sync(&mut self, options: DownlinkOptions) {
        if options.contains(DownlinkOptions::SYNC) {
            *self |= WriteTaskState::NEEDS_SYNC;
        }
    }
}

/// A request to attach a new consumer to the downlink runtime.
pub struct AttachAction {
    input: ByteReader,
    output: ByteWriter,
    options: DownlinkOptions,
}

impl AttachAction {
    pub fn new(input: ByteReader, output: ByteWriter, options: DownlinkOptions) -> Self {
        AttachAction {
            input,
            output,
            options,
        }
    }
}

/// Configuration parameters for the downlink runtime.
#[derive(Debug, Clone, Copy)]
pub struct DownlinkRuntimeConfig {
    /// If the runtime has no consumers for longer than this timeout, it will stop.
    pub empty_timeout: Duration,
    /// Size of the queue for accepting new subscribers to a downlink.
    pub attachment_queue_size: NonZeroUsize,
}

/// The runtime component for a value type downlink (i.e. value downlink, event downlink, etc.).
pub struct ValueDownlinkRuntime {
    requests: mpsc::Receiver<AttachAction>,
    input: ByteReader,
    output: ByteWriter,
    stopping: trigger::Receiver,
    identity: RoutingAddr,
    path: RelativePath,
    config: DownlinkRuntimeConfig,
}

impl ValueDownlinkRuntime {
    /// #Arguments
    /// * `requests` - The channel through which new consumers connect to the runtime.
    /// * `input` - Byte channel through which messages are received from the remote lane.
    /// * `output` - Byte channel through which messages are sent to the remote lane.
    /// * `stopping` - Trigger to instruct the runtime to stop.
    /// * `identity` - The routing ID of this runtime.
    /// * `path` - The path to the remote lane.
    /// * `config` - Configuration parameters for the runtime.
    pub fn new(
        requests: mpsc::Receiver<AttachAction>,
        input: ByteReader,
        output: ByteWriter,
        stopping: trigger::Receiver,
        identity: RoutingAddr,
        path: RelativePath,
        config: DownlinkRuntimeConfig,
    ) -> Self {
        ValueDownlinkRuntime {
            requests,
            input,
            output,
            stopping,
            identity,
            path,
            config,
        }
    }

    pub async fn run(self) {
        let ValueDownlinkRuntime {
            requests,
            input,
            output,
            stopping,
            identity,
            path,
            config,
        } = self;

        let (producer_tx, producer_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (consumer_tx, consumer_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (kill_switch_tx, kill_switch_rx) = trigger::trigger();
        let att = attach_task(
            requests,
            producer_tx,
            consumer_tx,
            stopping.clone(),
            kill_switch_rx,
        )
        .instrument(info_span!("Downlink Runtime Attachment Task", %path));
        let read = read_task(input, consumer_rx, stopping.clone(), config)
            .instrument(info_span!("Downlink Runtime Read Task", %path));
        let write = write_task(
            output,
            producer_rx,
            stopping,
            identity,
            path.clone(),
            config,
        )
        .instrument(info_span!("Downlink Runtime Write Task", %path));
        let io = async move {
            join(read, write).await;
            kill_switch_tx.trigger();
        };
        join(att, io).await;
    }
}

/// Communicates with the read and write tasks to add new consumers.
async fn attach_task(
    rx: mpsc::Receiver<AttachAction>,
    producer_tx: mpsc::Sender<(ByteReader, DownlinkOptions)>,
    consumer_tx: mpsc::Sender<(ByteWriter, DownlinkOptions)>,
    stopping: trigger::Receiver,
    kill_switch: trigger::Receiver,
) {
    let combined_stop = select(stopping, kill_switch);
    let mut stream = ReceiverStream::new(rx).take_until(combined_stop);
    while let Some(AttachAction {
        input,
        output,
        options,
    }) = stream.next().await
    {
        if consumer_tx.send((output, options)).await.is_err() {
            break;
        }
        if producer_tx.send((input, options)).await.is_err() {
            break;
        }
    }
    trace!("Attachment task stopping.");
}

#[derive(Debug, PartialEq, Eq)]
enum ReadTaskState {
    Init,
    Linked,
    Synced,
}

/// Sender to communicate with a subscriber to the downlink.
struct DownlinkSender {
    sender: FramedWrite<ByteWriter, DownlinkNotificationEncoder>,
    options: DownlinkOptions,
}

impl DownlinkSender {
    fn new(writer: ByteWriter, options: DownlinkOptions) -> Self {
        DownlinkSender {
            sender: FramedWrite::new(writer, DownlinkNotificationEncoder::default()),
            options,
        }
    }

    async fn feed(
        &mut self,
        mesage: DownlinkNotification<&BytesMut>,
    ) -> Result<(), std::io::Error> {
        self.sender.feed(mesage).await
    }

    async fn send(
        &mut self,
        mesage: DownlinkNotification<&BytesMut>,
    ) -> Result<(), std::io::Error> {
        self.sender.send(mesage).await
    }

    async fn flush(&mut self) -> Result<(), std::io::Error> {
        flush_sender_notification(&mut self.sender).await
    }
}

async fn flush_sender_notification<T>(
    sender: &mut FramedWrite<ByteWriter, T>,
) -> Result<(), T::Error>
where
    T: Encoder<DownlinkNotification<&'static BytesMut>>,
{
    sender.flush().await
}

async fn flush_sender_req<T>(sender: &mut FramedWrite<ByteWriter, T>) -> Result<(), T::Error>
where
    T: Encoder<RawRequestMessage<'static>>,
{
    sender.flush().await
}

/// Receiver to receive commands from downlink subscribers.
struct DownlinkReceiver {
    receiver: FramedRead<ByteReader, DownlinkOperationDecoder>,
    id: u64,
    terminated: bool,
}

impl DownlinkReceiver {
    fn new(reader: ByteReader, id: u64) -> Self {
        DownlinkReceiver {
            receiver: FramedRead::new(reader, DownlinkOperationDecoder),
            id,
            terminated: false,
        }
    }

    /// Stops a receiver so that it will be removed from a [`SelectAll`] collection.
    fn terminate(&mut self) {
        self.terminated = true;
    }
}

struct Failed(u64);

impl Stream for DownlinkReceiver {
    type Item = Result<DownlinkOperation<Bytes>, Failed>;

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
                .map_err(|_| Failed(this.id))
        }
    }
}

/// Consumes incoming messages from the remote lane and passes them to the consumers.
async fn read_task(
    input: ByteReader,
    consumers: mpsc::Receiver<(ByteWriter, DownlinkOptions)>,
    stopping: trigger::Receiver,
    config: DownlinkRuntimeConfig,
) {
    let mut messages = FramedRead::new(input, RawResponseMessageDecoder);

    let mut flushed = true;

    let mut consumer_stream = ReceiverStream::new(consumers).take_until(stopping);

    let mut state = ReadTaskState::Init;
    let mut current = BytesMut::new();
    let mut awaiting_synced: Vec<DownlinkSender> = vec![];
    let mut awaiting_linked: Vec<DownlinkSender> = vec![];
    let mut registered: Vec<DownlinkSender> = vec![];

    let mut empty_timestamp = Some(Instant::now() + config.empty_timeout);
    loop {
        let next_item = if let Some(timestamp) = empty_timestamp {
            if let Ok(result) = timeout_at(timestamp, consumer_stream.next()).await {
                result.map(Either::Right)
            } else {
                info!("No consumers connected within the timeout period.");
                // No consumers registered within the timeout so stop.
                break;
            }
        } else if flushed {
            trace!("Waiting without flush.");
            match select(consumer_stream.next(), messages.next()).await {
                Either::Left((consumer, _)) => consumer.map(Either::Right),
                Either::Right((msg, _)) => msg.map(Either::Left),
            }
        } else {
            trace!("Waiting with flush.");
            let get_next = select(consumer_stream.next(), messages.next());
            let flush = join(flush_all(&mut awaiting_synced), flush_all(&mut registered));
            let next_with_flush = immediate_or_join(get_next, flush);
            let (next, flush_result) = next_with_flush.await;
            if flush_result.is_some() {
                trace!("Flush completed.");
                if registered.is_empty() && awaiting_synced.is_empty() {
                    trace!("Number of subscribers dropped to 0.");
                    empty_timestamp = Some(Instant::now() + config.empty_timeout);
                }
                flushed = true;
            }
            match next {
                Either::Left((consumer, _)) => consumer.map(Either::Right),
                Either::Right((msg, _)) => msg.map(Either::Left),
            }
        };

        match next_item {
            Some(Either::Left(Ok(RawResponseMessage { envelope, .. }))) => match envelope {
                Notification::Linked => {
                    trace!("Entering Linked state.");
                    state = ReadTaskState::Linked;
                    link(&mut awaiting_linked, &mut awaiting_synced, &mut registered).await;
                    if awaiting_synced.is_empty() && registered.is_empty() {
                        empty_timestamp = Some(Instant::now() + config.empty_timeout);
                    }
                }
                Notification::Synced => {
                    trace!("Entering Synced state.");
                    state = ReadTaskState::Synced;
                    sync_current(&mut awaiting_synced, &mut registered, &current).await;
                    if registered.is_empty() {
                        empty_timestamp = Some(Instant::now() + config.empty_timeout);
                    }
                }
                Notification::Event(bytes) => {
                    trace!("Dispatching an event.");
                    current.clear();
                    current.reserve(bytes.len());
                    current.put(bytes);
                    send_current(&mut registered, &current).await;
                    if registered.is_empty() && awaiting_synced.is_empty() {
                        trace!("Number of subscribers dropped to 0.");
                        empty_timestamp = Some(Instant::now() + config.empty_timeout);
                        flushed = true;
                    } else {
                        flushed = false;
                    }
                }
                Notification::Unlinked(message) => {
                    trace!("Stopping after unlinked: {msg:?}", msg = message);
                    break;
                }
            },
            Some(Either::Right((writer, options))) => {
                let mut dl_writer = DownlinkSender::new(writer, options);
                if matches!(state, ReadTaskState::Init) {
                    trace!("Attaching a new subscriber to be linked.");
                    empty_timestamp = None;
                    awaiting_linked.push(dl_writer);
                } else if dl_writer.send(DownlinkNotification::Linked).await.is_ok() {
                    trace!("Attaching a new subscriber to be synced.");
                    empty_timestamp = None;
                    awaiting_synced.push(dl_writer);
                }
            }
            Some(Either::Left(Err(err))) => {
                error!(
                    "Failed to read a frame from the input: {error}",
                    error = err
                );
                break;
            }
            _ => {
                trace!("Stopping after being dropped by the runtime.");
                break;
            }
        }
    }
    trace!("Read task stopping and unlinked all subscribers");
    unlink(awaiting_linked).await;
    unlink(awaiting_synced).await;
    unlink(registered).await;
}

async fn sync_current(
    awaiting_synced: &mut Vec<DownlinkSender>,
    registered: &mut Vec<DownlinkSender>,
    current: &BytesMut,
) {
    let event = DownlinkNotification::Event { body: current };
    for mut tx in awaiting_synced.drain(..) {
        if tx.feed(event).await.is_ok() && tx.send(DownlinkNotification::Synced).await.is_ok() {
            registered.push(tx);
        }
    }
}

async fn send_current(senders: &mut Vec<DownlinkSender>, current: &BytesMut) {
    let event = DownlinkNotification::Event { body: current };
    let mut failed = HashSet::<usize>::default();
    for (i, tx) in senders.iter_mut().enumerate() {
        if tx.feed(event).await.is_err() {
            failed.insert(i);
        }
    }
    clear_failed(senders, &failed);
}

async fn link(
    awaiting_linked: &mut Vec<DownlinkSender>,
    awaiting_synced: &mut Vec<DownlinkSender>,
    registered: &mut Vec<DownlinkSender>,
) {
    let event = DownlinkNotification::Linked;

    for mut tx in awaiting_linked.drain(..) {
        if tx.send(event).await.is_ok() {
            if tx.options.contains(DownlinkOptions::SYNC) {
                awaiting_synced.push(tx);
            } else {
                registered.push(tx);
            }
        }
    }
}

async fn unlink(senders: Vec<DownlinkSender>) -> Vec<DownlinkSender> {
    let mut to_keep = vec![];
    for mut tx in senders.into_iter() {
        let still_active = tx.send(DownlinkNotification::Unlinked).await.is_ok();
        if tx.options.contains(DownlinkOptions::KEEP_LINKED) && still_active {
            to_keep.push(tx);
        }
    }
    to_keep
}

fn clear_failed(senders: &mut Vec<DownlinkSender>, failed: &HashSet<usize>) {
    if !failed.is_empty() {
        trace!(
            "Clearing {num_failed} failed subscribers.",
            num_failed = failed.len()
        );
        let mut i = 0;
        senders.retain(|_| {
            let keep = !failed.contains(&i);
            i += 1;
            keep
        });
    }
}

async fn flush_all(senders: &mut Vec<DownlinkSender>) {
    let mut failed = HashSet::<usize>::default();
    for (i, tx) in senders.iter_mut().enumerate() {
        if tx.flush().await.is_err() {
            failed.insert(i);
        }
    }
    clear_failed(senders, &failed);
}

#[derive(Debug)]
struct RequestSender {
    sender: FramedWrite<ByteWriter, RawRequestMessageEncoder>,
    identity: RoutingAddr,
    path: RelativePath,
}

impl RequestSender {
    fn new(writer: ByteWriter, identity: RoutingAddr, path: RelativePath) -> Self {
        RequestSender {
            sender: FramedWrite::new(writer, RawRequestMessageEncoder),
            identity,
            path,
        }
    }

    async fn send_link(&mut self) -> Result<(), std::io::Error> {
        let RequestSender {
            sender,
            identity,
            path,
        } = self;
        let message = RawRequestMessage {
            origin: *identity,
            path: path.clone(),
            envelope: Operation::Link,
        };
        sender.send(message).await
    }

    async fn send_sync(&mut self) -> Result<(), std::io::Error> {
        let RequestSender {
            sender,
            identity,
            path,
        } = self;
        let message = RawRequestMessage {
            origin: *identity,
            path: path.clone(),
            envelope: Operation::Sync,
        };
        sender.send(message).await
    }

    async fn feed_command(&mut self, body: &[u8]) -> Result<(), std::io::Error> {
        let RequestSender {
            sender,
            identity,
            path,
        } = self;
        let message = RawRequestMessage {
            origin: *identity,
            path: path.clone(),
            envelope: Operation::Command(body),
        };
        sender.feed(message).await
    }

    async fn flush(&mut self) -> Result<(), std::io::Error> {
        flush_sender_req(&mut self.sender).await
    }

    fn owning_flush(self) -> OwningFlush {
        OwningFlush::new(self)
    }
}

/// The internal state of the write task.
enum WriteState<F> {
    /// No writes are currently pending.
    Idle {
        message_writer: RequestSender,
        buffer: BytesMut,
    },
    /// A write to the outgoing channel is pending. In this state backpressure relief will
    /// cause updates the overwritten.
    Writing(F),
}

enum WriteKind {
    Sync,
    Data,
}

async fn do_flush(
    flush: OwningFlush,
    buffer: BytesMut,
) -> (Result<RequestSender, std::io::Error>, BytesMut) {
    let result = flush.await;
    (result, buffer)
}

/// Receives commands for the subscribers to the downlink and writes them to the outgoing channel.
/// If commands are received faster than the channel can send them, some records will be dropped.
async fn write_task(
    output: ByteWriter,
    producers: mpsc::Receiver<(ByteReader, DownlinkOptions)>,
    stopping: trigger::Receiver,
    identity: RoutingAddr,
    path: RelativePath,
    config: DownlinkRuntimeConfig,
) {
    let mut message_writer = RequestSender::new(output, identity, path);
    if message_writer.send_link().await.is_err() {
        return;
    }

    let mut state = WriteState::Idle {
        message_writer,
        buffer: BytesMut::new(),
    };
    let mut current = BytesMut::new();

    let mut registered: SelectAll<DownlinkReceiver> = SelectAll::new();
    let mut reg_requests = ReceiverStream::new(producers).take_until(stopping);
    // Consumers are given unique IDs to allow them to be removed when they fail.
    let mut id: u64 = 0;
    let mut next_id = move || {
        let i = id;
        id += 1;
        i
    };

    let mut task_state = WriteTaskState::INIT;

    let suspend_write = |mut message_writer: RequestSender, buffer: BytesMut, kind: WriteKind| async move {
        let result = match kind {
            WriteKind::Data => message_writer.feed_command(buffer.as_ref()).await,
            WriteKind::Sync => message_writer.send_sync().await,
        };
        (result.map(move |_| message_writer), buffer)
    };

    // The write task state machine has two states and three supplementary flags. The flags indicate the
    // following conditions:
    //
    // Flags
    // =====
    //
    // UPDATED:     While a write or flush was occurring, at least one new command has been received and
    //              another write needs to be scheduled.
    // FLUSHED:     Indicates that all data written to the output has been flushed.
    // NEEDS_SYNC:  While a write or flush was ocurring, a new subscriber was added that requested a SYNC
    //              message to be sent and this should be sent at the next opportunity.
    //
    // The state are as follows:
    //
    // States
    // ======
    //
    // Idle:    No writes are pending. In this state it will wait for new subscribers and outgoing commands
    //          from existing subscribers. If the FLUSHED flag is not set, it will attempt to flush the
    //          output channel simultaneously.
    //          1. If a new subscriber is received and:
    //              a. The flush did not start or completed successfully. Then subscriber is added and if it
    //                 requests a SYNC, we move to the Writing state for the SYNC message.
    //              b. The flush started and did not complete. The NEEDS_SYNC flag is set and we move to the
    //                 Flushing state.
    //          2. If a command is received and:
    //              a. The flush did not start or completed successfully. We move to the Writing state, writing
    //                 the command.
    //              b. The flush started and did not complete. The command is written into a buffer, the UPDATED
    //                 flag is set and we move into the Flushing state.
    // Writing: A write (of a command or SYNC message) is pending. In this state it will wait for the write to
    //          complete and for new subscribers and outgoing commands from existing subscribers.
    //          1. If the write completes and:
    //              a. The NEEDS_SYNC flag is set. A new write is scheduled for the SYNC and we remain in the
    //                 Writing state.
    //              b. The UPDATED flag is set. A new write is scheduled for the contents of the buffer and we
    //                 remain in the Writing state.
    //              c. Otherwise we move back to the Idle state.
    'outer: loop {
        match state {
            WriteState::Idle {
                mut message_writer,
                mut buffer,
            } => {
                if registered.is_empty() {
                    task_state.remove(WriteTaskState::NEEDS_SYNC);
                    let req_with_timeout = timeout(config.empty_timeout, reg_requests.next());
                    let req_result = if task_state.contains(WriteTaskState::FLUSHED) {
                        req_with_timeout.await
                    } else {
                        let (req_result, flush_result) =
                            join(req_with_timeout, message_writer.flush()).await;
                        if flush_result.is_err() {
                            warn!("Flushing the output failed.");
                            break 'outer;
                        } else {
                            task_state |= WriteTaskState::FLUSHED;
                        }
                        req_result
                    };
                    match req_result {
                        Ok(Some((reader, options))) => {
                            trace!("Registering new subscriber.");
                            let receiver = DownlinkReceiver::new(reader, next_id());
                            registered.push(receiver);
                            if options.contains(DownlinkOptions::SYNC) {
                                trace!("Sending a Sync message.");
                                let write = suspend_write(message_writer, buffer, WriteKind::Sync);
                                state = WriteState::Writing(Either::Left(write));
                            } else {
                                state = WriteState::Idle {
                                    message_writer,
                                    buffer,
                                };
                            }
                        }
                        Err(_) => {
                            info!("Stopping as no subscribers attached within the timeout.");
                            break;
                        }
                        _ => {
                            info!("Stopping after dropped by the runtime.");
                            break;
                        }
                    }
                } else {
                    let next = select(reg_requests.next(), registered.next());
                    let (next_op, flush_outcome) = if task_state.contains(WriteTaskState::FLUSHED) {
                        (discard(next.await), Either::Left(message_writer))
                    } else {
                        let (next_op, flush_result) =
                            immediate_or_start(next, message_writer.owning_flush()).await;
                        let flush_outcome = match flush_result {
                            SecondaryResult::NotStarted(of) => Either::Left(of.reclaim()),
                            SecondaryResult::Pending(of) => Either::Right(of),
                            SecondaryResult::Completed(Ok(sender)) => {
                                task_state |= WriteTaskState::FLUSHED;
                                Either::Left(sender)
                            }
                            SecondaryResult::Completed(Err(_)) => {
                                warn!("Flushing the output failed.");
                                break 'outer;
                            }
                        };
                        (discard(next_op), flush_outcome)
                    };
                    match next_op {
                        Either::Left(Some((reader, options))) => {
                            let receiver = DownlinkReceiver::new(reader, next_id());
                            registered.push(receiver);
                            match flush_outcome {
                                Either::Left(message_writer) => {
                                    if options.contains(DownlinkOptions::SYNC) {
                                        trace!("Sending a Sync message.");
                                        let write =
                                            suspend_write(message_writer, buffer, WriteKind::Sync);
                                        state = WriteState::Writing(Either::Left(write));
                                    } else {
                                        state = WriteState::Idle {
                                            message_writer,
                                            buffer,
                                        };
                                    }
                                }
                                Either::Right(flush) => {
                                    trace!("Waiting on the completion of a flush.");
                                    task_state.set_needs_sync(options);
                                    state =
                                        WriteState::Writing(Either::Right(do_flush(flush, buffer)));
                                }
                            }
                        }
                        Either::Left(_) => {
                            info!("Stopping after dropped by the runtime.");
                            break 'outer;
                        }
                        Either::Right(Some(Ok(DownlinkOperation { body }))) => {
                            match flush_outcome {
                                Either::Left(message_writer) => {
                                    trace!("Dispatching an event.");
                                    buffer.clear();
                                    buffer.reserve(body.len());
                                    buffer.put(body);
                                    let write_fut =
                                        suspend_write(message_writer, buffer, WriteKind::Data);
                                    task_state
                                        .remove(WriteTaskState::FLUSHED | WriteTaskState::UPDATED);
                                    state = WriteState::Writing(Either::Left(write_fut));
                                }
                                Either::Right(flush) => {
                                    trace!("Storing an event in the buffer and waiting on a flush to compelte.");
                                    current.clear();
                                    current.reserve(body.len());
                                    current.put(body);
                                    task_state |= WriteTaskState::UPDATED;
                                    state =
                                        WriteState::Writing(Either::Right(do_flush(flush, buffer)));
                                }
                            }
                        }
                        Either::Right(ow) => {
                            if let Some(Err(Failed(id))) = ow {
                                trace!("Removing a failed subscriber");
                                if let Some(rx) = registered.iter_mut().find(|rx| rx.id == id) {
                                    rx.terminate();
                                }
                            }
                            state = match flush_outcome {
                                Either::Left(message_writer) => WriteState::Idle {
                                    message_writer,
                                    buffer,
                                },
                                Either::Right(flush) => {
                                    trace!("Waiting on the completion of a flush.");
                                    WriteState::Writing(Either::Right(do_flush(flush, buffer)))
                                }
                            };
                        }
                    }
                }
            }
            WriteState::Writing(write_fut) => {
                // It is necessary for the write to be pinned. Rather than putting it into a heap
                // allocation, it is instead pinned to the stack and another, inner loop is started
                // until the write completes.
                pin_mut!(write_fut);
                'inner: loop {
                    let result = if registered.is_empty() {
                        task_state.remove(WriteTaskState::NEEDS_SYNC);
                        match select(&mut write_fut, reg_requests.next()).await {
                            Either::Left((write_result, _)) => {
                                SuspendedResult::SuspendedCompleted(write_result)
                            }
                            Either::Right((request, _)) => {
                                SuspendedResult::NewRegistration(request)
                            }
                        }
                    } else {
                        await_suspended(&mut write_fut, registered.next(), reg_requests.next())
                            .await
                    };

                    match result {
                        SuspendedResult::SuspendedCompleted((result, mut buffer)) => {
                            let message_writer = if let Ok(mw) = result {
                                mw
                            } else {
                                warn!("Writing to the output failed.");
                                break 'outer;
                            };
                            if task_state.contains(WriteTaskState::NEEDS_SYNC) {
                                trace!("Sending a Sync message.");
                                task_state
                                    .remove(WriteTaskState::FLUSHED | WriteTaskState::NEEDS_SYNC);
                                let write = suspend_write(message_writer, buffer, WriteKind::Sync);
                                state = WriteState::Writing(Either::Left(write));
                            } else if task_state.contains(WriteTaskState::UPDATED) {
                                trace!("Dispatching the updated buffer.");
                                std::mem::swap(&mut buffer, &mut current);
                                let write_fut =
                                    suspend_write(message_writer, buffer, WriteKind::Data);
                                task_state
                                    .remove(WriteTaskState::FLUSHED | WriteTaskState::UPDATED);
                                state = WriteState::Writing(Either::Left(write_fut));
                            } else {
                                trace!("Task has become idle.");
                                state = WriteState::Idle {
                                    message_writer,
                                    buffer,
                                };
                            }
                            // The write has completed so we can return to the outer loop.
                            break 'inner;
                        }
                        SuspendedResult::NextRecord(Some(Ok(DownlinkOperation { body }))) => {
                            // Writing is currently blocked so overwrite the next value to be sent.
                            trace!("Over-writing the current event buffer.");
                            current.clear();
                            current.reserve(body.len());
                            current.put(body);
                            task_state |= WriteTaskState::UPDATED;
                        }
                        SuspendedResult::NextRecord(Some(Err(Failed(id)))) => {
                            trace!("Removing a failed subscriber.");
                            if let Some(rx) = registered.iter_mut().find(|rx| rx.id == id) {
                                rx.terminate();
                            }
                        }
                        SuspendedResult::NewRegistration(Some((reader, options))) => {
                            trace!("Registering a new subscriber.");
                            let receiver = DownlinkReceiver::new(reader, next_id());
                            registered.push(receiver);
                            task_state.set_needs_sync(options);
                        }
                        SuspendedResult::NewRegistration(_) => {
                            info!("Stopping after dropped by the runtime.");
                            break 'outer;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

use futures::ready;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A future that flushes a sender and then returns it. This is necessary as we need
/// an [`Unpin`] future so an equivlanent async block would not work.
struct OwningFlush {
    inner: Option<RequestSender>,
}

impl OwningFlush {
    fn new(sender: RequestSender) -> Self {
        OwningFlush {
            inner: Some(sender),
        }
    }

    fn reclaim(self) -> RequestSender {
        if let Some(sender) = self.inner {
            sender
        } else {
            panic!("OwningFlush reclaimed after complete.");
        }
    }
}

impl Future for OwningFlush {
    type Output = Result<RequestSender, std::io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let OwningFlush { inner } = self.get_mut();
        let result = if let Some(tx) = inner {
            ready!(sender_poll_flush(&mut tx.sender, cx))
        } else {
            panic!("OwningFlush polled after complete.");
        };
        Poll::Ready(result.map(move |_| inner.take().unwrap()))
    }
}

fn sender_poll_flush<Snk>(sink: &mut Snk, cx: &mut Context<'_>) -> Poll<Result<(), Snk::Error>>
where
    Snk: Sink<RawRequestMessage<'static>> + Unpin,
{
    sink.poll_flush_unpin(cx)
}

fn discard<A1, A2, B1, B2>(either: Either<(A1, A2), (B1, B2)>) -> Either<A1, B1> {
    match either {
        Either::Left((a1, _)) => Either::Left(a1),
        Either::Right((b1, _)) => Either::Right(b1),
    }
}

/// This enum is for clarity only to avoid having nested [`Either`]s in matche statements
/// after nesting [`select`] calls.
enum SuspendedResult<A, B, C> {
    SuspendedCompleted(A),
    NextRecord(B),
    NewRegistration(C),
}

async fn await_suspended<F1, F2, F3>(
    suspended: F1,
    next_rec: F2,
    next_reg: F3,
) -> SuspendedResult<F1::Output, F2::Output, F3::Output>
where
    F1: Future + Unpin,
    F2: Future + Unpin,
    F3: Future + Unpin,
{
    let alternatives = select(next_rec, next_reg).map(discard);
    match select(suspended, alternatives).await {
        Either::Left((r, _)) => SuspendedResult::SuspendedCompleted(r),
        Either::Right((Either::Left(r), _)) => SuspendedResult::NextRecord(r),
        Either::Right((Either::Right(r), _)) => SuspendedResult::NewRegistration(r),
    }
}
