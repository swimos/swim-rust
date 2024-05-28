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

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::downlink::failure::BadFrameResponse;
use crate::timeout_coord::{VoteResult, Voter};
use backpressure::DownlinkBackpressure;
use bitflags::bitflags;
use bytes::{Bytes, BytesMut};
use futures::future::{join, select, Either};
use futures::stream::SelectAll;
use futures::{Future, FutureExt, Sink, SinkExt, Stream, StreamExt};
pub use interpretation::MapInterpretation;
pub use interpretation::NoInterpretation;
use swimos_agent_protocol::{
    encoding::downlink::DownlinkNotificationEncoder, DownlinkNotification,
};
use swimos_messages::protocol::{
    Notification, Operation, Path, RawRequestMessage, RawRequestMessageEncoder,
    RawResponseMessageDecoder, ResponseMessage,
};
use swimos_model::address::RelativeAddress;
use swimos_model::{BytesStr, Text};
use swimos_utilities::future::{immediate_or_join, immediate_or_start, SecondaryResult};
use swimos_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swimos_utilities::trigger;
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tracing::{error, info, info_span, trace, warn, Instrument};
use uuid::Uuid;

mod backpressure;
pub mod failure;
mod interpretation;
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
        /// The outgoing channel has been flushed.
        const FLUSHED = 0b01;
        /// A new consumer that needs to be synced has joined why a write was pending.
        const NEEDS_SYNC = 0b10;
        /// When the task starts it does not need to be flushed.
        const INIT = Self::FLUSHED.bits;
    }
}

pub type Io = (ByteWriter, ByteReader);

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
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct DownlinkRuntimeConfig {
    /// If the runtime has no consumers for longer than this timeout, it will stop.
    pub empty_timeout: Duration,
    /// Size of the queue for accepting new subscribers to a downlink.
    pub attachment_queue_size: NonZeroUsize,
    /// Abort the downlink on receiving invalid frames.
    pub abort_on_bad_frames: bool,
    /// Size of the buffers to communicated with the socket.
    pub remote_buffer_size: NonZeroUsize,
    /// Size of the buffers to communicate with the downlink implementation.
    pub downlink_buffer_size: NonZeroUsize,
}

impl Default for DownlinkRuntimeConfig {
    fn default() -> Self {
        DownlinkRuntimeConfig {
            empty_timeout: Duration::from_secs(30),
            attachment_queue_size: non_zero_usize!(16),
            abort_on_bad_frames: true,
            remote_buffer_size: non_zero_usize!(4096),
            downlink_buffer_size: non_zero_usize!(4096),
        }
    }
}

/// The runtime component for a value type downlink (i.e. value downlink, event downlink, etc.).
pub struct ValueDownlinkRuntime {
    requests: mpsc::Receiver<AttachAction>,
    input: ByteReader,
    output: ByteWriter,
    stopping: trigger::Receiver,
    identity: Uuid,
    path: RelativeAddress<Text>,
    config: DownlinkRuntimeConfig,
}

/// The runtime component for a map type downlink.
pub struct MapDownlinkRuntime<H, I> {
    requests: mpsc::Receiver<AttachAction>,
    input: ByteReader,
    output: ByteWriter,
    stopping: trigger::Receiver,
    identity: Uuid,
    path: RelativeAddress<Text>,
    config: DownlinkRuntimeConfig,
    failure_handler: H,
    interpretation: I,
}

async fn await_io_tasks<F1, F2, E>(
    read: F1,
    write: F2,
    kill_switch_tx: trigger::Sender,
) -> Result<(), E>
where
    F1: Future<Output = Result<(), E>>,
    F2: Future<Output = ()>,
{
    let read = pin!(read);
    let write = pin!(write);
    let first_finished = select(read, write).await;
    kill_switch_tx.trigger();
    match first_finished {
        Either::Left((read_res, write_fut)) => {
            write_fut.await;
            read_res
        }
        Either::Right((_, read_fut)) => read_fut.await,
    }
}

impl ValueDownlinkRuntime {
    /// #Arguments
    /// * `requests` - The channel through which new consumers connect to the runtime.
    /// * `io` - Byte channels through which messages are received from and sent to the remote lane.
    /// * `stopping` - Trigger to instruct the runtime to stop.
    /// * `identity` - The routing ID of this runtime.
    /// * `path` - The path to the remote lane.
    /// * `config` - Configuration parameters for the runtime.
    pub fn new(
        requests: mpsc::Receiver<AttachAction>,
        io: Io,
        stopping: trigger::Receiver,
        address: IdentifiedAddress,
        config: DownlinkRuntimeConfig,
    ) -> Self {
        let (output, input) = io;
        let IdentifiedAddress {
            identity,
            address: path,
        } = address;
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
        let (read_vote, write_vote, vote_rx) = crate::timeout_coord::downlink_timeout_coordinator();

        let combined_stop = select(stopping, select(kill_switch_rx, vote_rx));
        let att = attach_task(requests, producer_tx, consumer_tx, combined_stop)
            .instrument(info_span!("Value Downlink Runtime Attachment Task", %path));
        let read = read_task(
            input,
            consumer_rx,
            config,
            value_interpretation(),
            InfallibleStrategy,
            read_vote,
        )
        .instrument(info_span!("Value Downlink Runtime Read Task", %path));
        let write = write_task(
            output,
            producer_rx,
            identity,
            Path::new(path.node.clone(), path.lane.clone()),
            config,
            ValueBackpressure::default(),
            write_vote,
        )
        .instrument(info_span!("Value Downlink Runtime Write Task", %path));
        let io = async move {
            let read_res = await_io_tasks(read, write, kill_switch_tx).await;
            if let Err(e) = read_res {
                match e {}
            }
        };
        join(att, io).await;
    }
}

impl<H> MapDownlinkRuntime<H, MapInterpretation> {
    /// #Arguments
    /// * `requests` - The channel through which new consumers connect to the runtime.
    /// * `io` - Byte channels through which messages are received from and sent to the remote lane.
    /// * `stopping` - Trigger to instruct the runtime to stop.
    /// * `identity` - The routing ID of this runtime.
    /// * `path` - The path to the remote lane.
    /// * `config` - Configuration parameters for the runtime.
    /// * `failure_handler` - Handler for event frames that do no contain valid map
    /// messages.
    pub fn new(
        requests: mpsc::Receiver<AttachAction>,
        io: Io,
        stopping: trigger::Receiver,
        address: IdentifiedAddress,
        config: DownlinkRuntimeConfig,
        failure_handler: H,
    ) -> Self {
        let (output, input) = io;
        let IdentifiedAddress {
            identity,
            address: path,
        } = address;
        MapDownlinkRuntime {
            requests,
            input,
            output,
            stopping,
            identity,
            path,
            config,
            failure_handler,
            interpretation: MapInterpretation::default(),
        }
    }
}

impl<I, H> MapDownlinkRuntime<H, I> {
    /// #Arguments
    /// * `requests` - The channel through which new consumers connect to the runtime.
    /// * `io` - Byte channels through which messages are received from and sent to the remote lane.
    /// * `stopping` - Trigger to instruct the runtime to stop.
    /// * `identity` - The routing ID of this runtime.
    /// * `path` - The path to the remote lane.
    /// * `config` - Configuration parameters for the runtime.
    /// * `failure_handler` - Handler for event frames that do no contain valid map
    /// messages.
    /// * `interpretation` - A transformation to apply to an incoming event body, before passing it
    /// on to the downlink implementation.
    pub fn with_interpretation(
        requests: mpsc::Receiver<AttachAction>,
        io: Io,
        stopping: trigger::Receiver,
        address: IdentifiedAddress,
        config: DownlinkRuntimeConfig,
        failure_handler: H,
        interpretation: I,
    ) -> Self {
        let (output, input) = io;
        let IdentifiedAddress {
            identity,
            address: path,
        } = address;
        MapDownlinkRuntime {
            requests,
            input,
            output,
            stopping,
            identity,
            path,
            config,
            failure_handler,
            interpretation,
        }
    }
}

pub struct IdentifiedAddress {
    pub identity: Uuid,
    pub address: RelativeAddress<Text>,
}

impl<I, H> MapDownlinkRuntime<H, I>
where
    I: DownlinkInterpretation,
    H: BadFrameStrategy<I::Error>,
{
    pub async fn run(self) {
        let MapDownlinkRuntime {
            requests,
            input,
            output,
            stopping,
            identity,
            path,
            config,
            failure_handler,
            interpretation,
        } = self;

        let (producer_tx, producer_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (consumer_tx, consumer_rx) = mpsc::channel(config.attachment_queue_size.get());
        let (read_vote, write_vote, vote_rx) = crate::timeout_coord::downlink_timeout_coordinator();
        let (kill_switch_tx, kill_switch_rx) = trigger::trigger();

        let combined_stop = select(stopping, select(kill_switch_rx, vote_rx));
        let att = attach_task(requests, producer_tx, consumer_tx, combined_stop)
            .instrument(info_span!("Map Downlink Runtime Attachment Task", %path));
        let read = read_task(
            input,
            consumer_rx,
            config,
            interpretation,
            failure_handler,
            read_vote,
        )
        .instrument(info_span!("Map Downlink Runtime Read Task", %path));
        let write = write_task(
            output,
            producer_rx,
            identity,
            Path::new(path.node.clone(), path.lane.clone()),
            config,
            MapBackpressure::default(),
            write_vote,
        )
        .instrument(info_span!("Map Downlink Runtime Write Task", %path));
        let io = async move {
            let read_res = await_io_tasks(read, write, kill_switch_tx).await;
            if let Err(e) = read_res {
                error!(
                    "Map downlink received invalid event messages: {problems}",
                    problems = e
                );
            }
        };
        join(att, io).await;
    }
}

/// Communicates with the read and write tasks to add new consumers.
async fn attach_task<F>(
    rx: mpsc::Receiver<AttachAction>,
    producer_tx: mpsc::Sender<(ByteReader, DownlinkOptions)>,
    consumer_tx: mpsc::Sender<(ByteWriter, DownlinkOptions)>,
    combined_stop: F,
) where
    F: Future + Unpin,
{
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
enum ReadTaskDlState {
    Init,
    Linked,
    Synced,
}

/// Sender to communicate with a subscriber to the downlink.
#[derive(Debug)]
struct DownlinkSender {
    sender: FramedWrite<ByteWriter, DownlinkNotificationEncoder>,
    options: DownlinkOptions,
}

impl DownlinkSender {
    fn new(writer: ByteWriter, options: DownlinkOptions) -> Self {
        DownlinkSender {
            sender: FramedWrite::new(writer, DownlinkNotificationEncoder),
            options,
        }
    }

    async fn feed(
        &mut self,
        message: DownlinkNotification<&BytesMut>,
    ) -> Result<(), std::io::Error> {
        self.sender.feed(message).await
    }

    async fn send(
        &mut self,
        message: DownlinkNotification<&BytesMut>,
    ) -> Result<(), std::io::Error> {
        self.sender.send(message).await
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
    T: Encoder<RawRequestMessage<'static, &'static str>>,
{
    sender.flush().await
}

/// Receiver to receive commands from downlink subscribers.
struct DownlinkReceiver<D> {
    receiver: FramedRead<ByteReader, D>,
    id: u64,
    terminated: bool,
}

impl<D: Decoder> DownlinkReceiver<D> {
    fn new(reader: ByteReader, id: u64, decoder: D) -> Self {
        DownlinkReceiver {
            receiver: FramedRead::new(reader, decoder),
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

impl<D: Decoder> Stream for DownlinkReceiver<D> {
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
                .map_err(|_| Failed(this.id))
        }
    }
}

enum ReadTaskEvent {
    Message(ResponseMessage<BytesStr, Bytes, Bytes>),
    ReadFailed(std::io::Error),
    MessagesStopped,
    NewConsumer(ByteWriter, DownlinkOptions),
    ConsumerChannelStopped,
    ConsumersTimedOut,
}

/// Consumes incoming messages from the remote lane and passes them to the consumers.
async fn read_task<I, H>(
    input: ByteReader,
    consumers: mpsc::Receiver<(ByteWriter, DownlinkOptions)>,
    config: DownlinkRuntimeConfig,
    mut interpretation: I,
    mut failure_handler: H,
    stop_voter: Voter,
) -> Result<(), H::Report>
where
    I: DownlinkInterpretation,
    H: BadFrameStrategy<I::Error>,
{
    let mut messages = FramedRead::new(input, RawResponseMessageDecoder);

    let mut flushed = true;
    let mut voted = false;

    let mut consumer_stream = ReceiverStream::new(consumers);

    let make_timeout = || tokio::time::sleep_until(Instant::now() + config.empty_timeout);
    let mut task_state = pin!(Some(make_timeout()));
    let mut dl_state = ReadTaskDlState::Init;
    let mut current = BytesMut::new();
    let mut awaiting_synced: Vec<DownlinkSender> = vec![];
    let mut awaiting_linked: Vec<DownlinkSender> = vec![];
    let mut registered: Vec<DownlinkSender> = vec![];
    // Track whether any events have been received while syncing the downlink. While this isn't the
    // nicest thing to have, it's required to distinguish between communicating with a stateless
    // lane and a downlink syncing with a lane that has a type which may be optional.
    let mut sync_event = false;

    let result: Result<(), H::Report> = loop {
        let (event, is_active) = match task_state.as_mut().as_pin_mut() {
            Some(sleep) if !voted => (
                tokio::select! {
                    biased;
                    _ = sleep => {
                        task_state.set(None);
                        ReadTaskEvent::ConsumersTimedOut
                    },
                    maybe_consumer = consumer_stream.next() => {
                        if let Some((consumer, options)) = maybe_consumer {
                            ReadTaskEvent::NewConsumer(consumer, options)
                        } else {
                            ReadTaskEvent::ConsumerChannelStopped
                        }
                    },
                    maybe_message = messages.next() => {
                        match maybe_message {
                            Some(Ok(msg)) => ReadTaskEvent::Message(msg),
                            Some(Err(err)) => ReadTaskEvent::ReadFailed(err),
                            _ => ReadTaskEvent::MessagesStopped,
                        }
                    }
                },
                false,
            ),
            _ => {
                let get_next = async {
                    tokio::select! {
                        maybe_consumer = consumer_stream.next() => {
                            if let Some((consumer, options)) = maybe_consumer {
                                ReadTaskEvent::NewConsumer(consumer, options)
                            } else {
                                ReadTaskEvent::ConsumerChannelStopped
                            }
                        },
                        maybe_message = messages.next() => {
                            match maybe_message {
                                Some(Ok(msg)) => ReadTaskEvent::Message(msg),
                                Some(Err(err)) => ReadTaskEvent::ReadFailed(err),
                                _ => ReadTaskEvent::MessagesStopped,
                            }
                        }
                    }
                };
                if flushed {
                    trace!("Waiting without flush.");
                    (get_next.await, true)
                } else {
                    trace!("Waiting with flush.");
                    let flush = join(flush_all(&mut awaiting_synced), flush_all(&mut registered));
                    let next_with_flush = immediate_or_join(get_next, flush);
                    let (next, flush_result) = next_with_flush.await;
                    let is_active = if flush_result.is_some() {
                        trace!("Flush completed.");
                        flushed = true;
                        if registered.is_empty() && awaiting_synced.is_empty() {
                            trace!("Number of subscribers dropped to 0.");
                            task_state.set(Some(make_timeout()));
                            false
                        } else {
                            true
                        }
                    } else {
                        true
                    };
                    (next, is_active)
                }
            }
        };

        match event {
            ReadTaskEvent::ConsumersTimedOut => {
                info!("No consumers connected within the timeout period. Voting to stop.");
                if stop_voter.vote() == VoteResult::Unanimous {
                    // No consumers registered within the timeout and the write task has voted to stop.
                    break Ok(());
                } else {
                    voted = true;
                }
            }
            ReadTaskEvent::Message(ResponseMessage { envelope, .. }) => match envelope {
                Notification::Linked => {
                    trace!("Entering Linked state.");
                    dl_state = ReadTaskDlState::Linked;
                    if is_active {
                        link(&mut awaiting_linked, &mut awaiting_synced, &mut registered).await;
                        if awaiting_synced.is_empty() && registered.is_empty() {
                            trace!("Number of subscribers dropped to 0.");
                            task_state.set(Some(make_timeout()));
                        }
                    }
                }
                Notification::Synced => {
                    trace!("Entering Synced state.");
                    dl_state = ReadTaskDlState::Synced;
                    if is_active {
                        // `sync_event` will be false if we're communicating with a stateless lane
                        // as no event envelope will have been sent. However, it's valid Recon for
                        // an empty event envelope to be sent (consider Option::None) and this must
                        // still be sent to the downlink task.
                        //
                        // If we're linked to a stateless lane, then `sync_current` cannot be used
                        // as we will not have received an event envelope as it will dispatch one
                        // with the empty buffer and this may cause the downlink task's decoder to
                        // fail due to reading an extant read event. Therefore, delegate the operation to
                        // `sync_only` which will not send an event notification.
                        if I::SINGLE_FRAME_STATE && sync_event {
                            sync_current(&mut awaiting_synced, &mut registered, &current).await;
                        } else {
                            sync_only(&mut awaiting_synced, &mut registered).await;
                        }
                        if registered.is_empty() {
                            trace!("Number of subscribers dropped to 0.");
                            task_state.set(Some(make_timeout()));
                        }
                    }
                }
                Notification::Unlinked(message) => {
                    trace!("Stopping after unlinked: {msg:?}", msg = message);
                    break Ok(());
                }
                Notification::Event(bytes) => {
                    sync_event = true;

                    trace!("Updating the current value.");
                    current.clear();

                    if let Err(e) = interpretation.interpret_frame_data(bytes, &mut current) {
                        if let BadFrameResponse::Abort(report) = failure_handler.failed_with(e) {
                            break Err(report);
                        }
                    }
                    if is_active {
                        send_current(&mut registered, &current).await;
                        if !I::SINGLE_FRAME_STATE {
                            send_current(&mut awaiting_synced, &current).await;
                        }
                        if registered.is_empty() && awaiting_synced.is_empty() {
                            trace!("Number of subscribers dropped to 0.");
                            task_state.set(Some(make_timeout()));
                            flushed = true;
                        } else {
                            flushed = false;
                        }
                    }
                }
            },
            ReadTaskEvent::ReadFailed(err) => {
                error!(
                    "Failed to read a frame from the input: {error}",
                    error = err
                );
                break Ok(());
            }
            ReadTaskEvent::NewConsumer(writer, options) => {
                let mut dl_writer = DownlinkSender::new(writer, options);
                let added = if matches!(dl_state, ReadTaskDlState::Init) {
                    trace!("Attaching a new subscriber to be linked.");
                    awaiting_linked.push(dl_writer);
                    true
                } else if dl_writer.send(DownlinkNotification::Linked).await.is_ok() {
                    trace!("Attaching a new subscriber to be synced.");
                    awaiting_synced.push(dl_writer);
                    true
                } else {
                    false
                };
                if added {
                    task_state.set(None);
                    if voted {
                        if stop_voter.rescind() == VoteResult::Unanimous {
                            info!(
                                "Attempted to rescind stop vote but shutdown had already started."
                            );
                            break Ok(());
                        } else {
                            voted = false;
                        }
                    }
                }
            }
            _ => {
                trace!("Instructed to stop.");
                break Ok(());
            }
        }
    };

    trace!("Read task stopping and unlinked all subscribers");
    unlink(awaiting_linked).await;
    unlink(awaiting_synced).await;
    unlink(registered).await;
    result
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

async fn sync_only(
    awaiting_synced: &mut Vec<DownlinkSender>,
    registered: &mut Vec<DownlinkSender>,
) {
    for mut tx in awaiting_synced.drain(..) {
        if tx.send(DownlinkNotification::Synced).await.is_ok() {
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
    identity: Uuid,
    path: Path<Text>,
}

impl RequestSender {
    fn new(writer: ByteWriter, identity: Uuid, path: Path<Text>) -> Self {
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
async fn write_task<B: DownlinkBackpressure>(
    output: ByteWriter,
    producers: mpsc::Receiver<(ByteReader, DownlinkOptions)>,
    identity: Uuid,
    path: Path<Text>,
    config: DownlinkRuntimeConfig,
    mut backpressure: B,
    stop_voter: Voter,
) {
    let mut message_writer = RequestSender::new(output, identity, path);
    if message_writer.send_link().await.is_err() {
        return;
    }

    let mut state = WriteState::Idle {
        message_writer,
        buffer: BytesMut::new(),
    };

    let mut registered: SelectAll<DownlinkReceiver<B::Dec>> = SelectAll::new();
    let mut reg_requests = ReceiverStream::new(producers);
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

    let mut voted = false;

    // The write task state machine has two states and three supplementary flags. The flags indicate the
    // following conditions:
    //
    // Flags
    // =====
    //
    // HAS_DATA:    While a write or flush was occurring, at least one new command has been received and
    //              another write needs to be scheduled. This is stored implicitly in the bakpressure
    //              implementation.
    // FLUSHED:     Indicates that all data written to the output has been flushed.
    // NEEDS_SYNC:  While a write or flush was occurring, a new subscriber was added that requested a SYNC
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
    //              b. The flush started and did not complete. The command is written into a buffer, the HAS_DATA
    //                 flag is set and we move into the Flushing state.
    // Writing: A write (of a command or SYNC message) is pending. In this state it will wait for the write to
    //          complete and for new subscribers and outgoing commands from existing subscribers.
    //          1. If the write completes and:
    //              a. The NEEDS_SYNC flag is set. A new write is scheduled for the SYNC and we remain in the
    //                 Writing state.
    //              b. The HAS_DATA flag is set. A new write is scheduled for the contents of the buffer and we
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
                    let req_with_timeout = async {
                        if voted {
                            Ok(reg_requests.next().await)
                        } else {
                            timeout(config.empty_timeout, reg_requests.next()).await
                        }
                    };
                    let req_result = if task_state.contains(WriteTaskState::FLUSHED) {
                        req_with_timeout.await
                    } else {
                        let (req_result, flush_result) =
                            join(req_with_timeout, message_writer.flush()).await;
                        if let Err(err) = flush_result {
                            warn!(error = %err, "Flushing the output failed.");
                            break 'outer;
                        } else {
                            task_state |= WriteTaskState::FLUSHED;
                        }
                        req_result
                    };
                    match req_result {
                        Ok(Some((reader, options))) => {
                            if voted {
                                if stop_voter.rescind() == VoteResult::Unanimous {
                                    info!("Attempted to rescind vote to stop but shutdown had already started.");
                                    break 'outer;
                                } else {
                                    voted = false;
                                }
                            }
                            trace!("Registering new subscriber.");
                            let receiver =
                                DownlinkReceiver::new(reader, next_id(), B::make_decoder());
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
                            if stop_voter.vote() == VoteResult::Unanimous {
                                info!("Stopping as no subscribers attached within the timeout and read task voted to stop.");
                                break 'outer;
                            } else {
                                voted = true;
                                state = WriteState::Idle {
                                    message_writer,
                                    buffer,
                                };
                            }
                        }
                        _ => {
                            info!("Instructed to stop.");
                            break 'outer;
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
                            let receiver =
                                DownlinkReceiver::new(reader, next_id(), B::make_decoder());
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
                            info!("Instructed to stop.");
                            break 'outer;
                        }
                        Either::Right(Some(Ok(op))) => match flush_outcome {
                            Either::Left(message_writer) => {
                                trace!("Dispatching an event.");
                                backpressure.write_direct(op, &mut buffer);
                                let write_fut =
                                    suspend_write(message_writer, buffer, WriteKind::Data);
                                task_state.remove(WriteTaskState::FLUSHED);
                                state = WriteState::Writing(Either::Left(write_fut));
                            }
                            Either::Right(flush) => {
                                trace!("Storing an event in the buffer and waiting on a flush to compelte.");
                                if let Err(err) = backpressure.push_operation(op) {
                                    error!(
                                        "Failed to process downlink operaton: {error}",
                                        error = err
                                    );
                                };
                                state = WriteState::Writing(Either::Right(do_flush(flush, buffer)));
                            }
                        },
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
                let mut write_fut = pin!(write_fut);
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
                            } else if backpressure.has_data() {
                                trace!("Dispatching the updated buffer.");
                                backpressure.prepare_write(&mut buffer);
                                let write_fut =
                                    suspend_write(message_writer, buffer, WriteKind::Data);
                                task_state.remove(WriteTaskState::FLUSHED);
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
                        SuspendedResult::NextRecord(Some(Ok(op))) => {
                            // Writing is currently blocked so overwrite the next value to be sent.
                            trace!("Over-writing the current event buffer.");
                            if let Err(err) = backpressure.push_operation(op) {
                                error!("Failed to process downlink operaton: {error}", error = err);
                            };
                        }
                        SuspendedResult::NextRecord(Some(Err(Failed(id)))) => {
                            trace!("Removing a failed subscriber.");
                            if let Some(rx) = registered.iter_mut().find(|rx| rx.id == id) {
                                rx.terminate();
                            }
                        }
                        SuspendedResult::NewRegistration(Some((reader, options))) => {
                            trace!("Registering a new subscriber.");
                            let receiver =
                                DownlinkReceiver::new(reader, next_id(), B::make_decoder());
                            registered.push(receiver);
                            task_state.set_needs_sync(options);
                        }
                        SuspendedResult::NewRegistration(_) => {
                            info!("Instructed to stop.");
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
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use swimos_utilities::non_zero_usize;

use self::failure::{BadFrameStrategy, InfallibleStrategy};
use self::interpretation::{value_interpretation, DownlinkInterpretation};
use crate::backpressure::{MapBackpressure, ValueBackpressure};

/// A future that flushes a sender and then returns it. This is necessary as we need
/// an [`Unpin`] future so an equivalent async block would not work.
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
    Snk: Sink<RawRequestMessage<'static, &'static str>> + Unpin,
{
    sink.poll_flush_unpin(cx)
}

fn discard<A1, A2, B1, B2>(either: Either<(A1, A2), (B1, B2)>) -> Either<A1, B1> {
    match either {
        Either::Left((a1, _)) => Either::Left(a1),
        Either::Right((b1, _)) => Either::Right(b1),
    }
}

/// This enum is for clarity only to avoid having nested [`Either`]s in match statements
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
