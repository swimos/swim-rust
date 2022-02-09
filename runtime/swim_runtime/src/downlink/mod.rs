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
use std::time::Duration;

use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::{join, join3, select, Either};
use futures::stream::SelectAll;
use futures::{Future, FutureExt, Sink, SinkExt, Stream, StreamExt};
use pin_utils::pin_mut;
use swim_api::protocol::{
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

use crate::compat::{
    Notification, Operation, RawRequestMessage, RawRequestMessageEncoder, RawResponseMessage,
    RawResponseMessageDecoder,
};
use crate::routing::RoutingAddr;

#[cfg(test)]
mod tests;

bitflags! {
    pub struct DownlinkOptions: u32 {
        const SYNC = 0b00000001;
        const KEEP_LINKED = 0b00000010;
        const DEFAULT = Self::SYNC.bits | Self::KEEP_LINKED.bits;
    }
}

bitflags! {
    pub struct WriteTaskState: u32 {
        const UPDATED = 0b00000001;
        const FLUSHED = 0b00000010;
        const NEEDS_SYNC = 0b00000100;
        const INIT = Self::FLUSHED.bits;
    }
}

impl WriteTaskState {
    pub fn set_needs_sync(&mut self, options: DownlinkOptions) {
        if options.contains(DownlinkOptions::SYNC) {
            *self |= WriteTaskState::NEEDS_SYNC;
        }
    }
}

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

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub empty_timeout: Duration,
}

pub struct ValueDownlinkManagementTask {
    requests: mpsc::Receiver<AttachAction>,
    input: ByteReader,
    output: ByteWriter,
    stopping: trigger::Receiver,
    identity: RoutingAddr,
    path: RelativePath,
    config: Config,
}

impl ValueDownlinkManagementTask {
    pub fn new(
        requests: mpsc::Receiver<AttachAction>,
        input: ByteReader,
        output: ByteWriter,
        stopping: trigger::Receiver,
        identity: RoutingAddr,
        path: RelativePath,
        config: Config,
    ) -> Self {
        ValueDownlinkManagementTask {
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
        let ValueDownlinkManagementTask {
            requests,
            input,
            output,
            stopping,
            identity,
            path,
            config,
        } = self;

        let (producer_tx, producer_rx) = mpsc::channel(8);
        let (consumer_tx, consumer_rx) = mpsc::channel(8);
        let att = attach_task(requests, producer_tx, consumer_tx, stopping.clone());
        let read = read_task(input, consumer_rx, stopping.clone(), config);
        let write = write_task(output, producer_rx, stopping, identity, path, config);
        join3(att, read, write).await;
    }
}

async fn attach_task(
    rx: mpsc::Receiver<AttachAction>,
    producer_tx: mpsc::Sender<(ByteReader, DownlinkOptions)>,
    consumer_tx: mpsc::Sender<(ByteWriter, DownlinkOptions)>,
    stopping: trigger::Receiver,
) {
    let mut stream = ReceiverStream::new(rx).take_until(stopping);
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
}
#[derive(Debug, PartialEq, Eq)]
enum ReadTaskState {
    Init,
    Linked,
    Synced,
}

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

struct DownlinkReceiver {
    receiver: FramedRead<ByteReader, DownlinkOperationDecoder>,
    _options: DownlinkOptions,
    id: u64,
    terminated: bool,
}

impl DownlinkReceiver {
    fn new(reader: ByteReader, options: DownlinkOptions, id: u64) -> Self {
        DownlinkReceiver {
            receiver: FramedRead::new(reader, DownlinkOperationDecoder),
            _options: options,
            id,
            terminated: false,
        }
    }

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

async fn read_task(
    input: ByteReader,
    consumers: mpsc::Receiver<(ByteWriter, DownlinkOptions)>,
    stopping: trigger::Receiver,
    config: Config,
) {
    let mut messages = FramedRead::new(input, RawResponseMessageDecoder);

    let mut flushed = true;

    let mut consumer_stream = ReceiverStream::new(consumers).take_until(stopping);

    let mut state = ReadTaskState::Init;
    let mut current = BytesMut::new();
    let mut awaiting_synced: Vec<DownlinkSender> = vec![];
    let mut awaiting_linked: Vec<DownlinkSender> = vec![];
    let mut registered: Vec<DownlinkSender> = vec![];

    let mut empty_timestamp = Some(Instant::now());
    loop {
        let next_item = if let Some(timestamp) = empty_timestamp {
            if let Ok(result) = timeout_at(timestamp, consumer_stream.next()).await {
                result.map(Either::Right)
            } else {
                // No consumers registered within the timeout so stop.
                break;
            }
        } else if flushed {
            match select(consumer_stream.next(), messages.next()).await {
                Either::Left((consumer, _)) => consumer.map(Either::Right),
                Either::Right((msg, _)) => msg.map(Either::Left),
            }
        } else {
            let get_next = select(consumer_stream.next(), messages.next());
            let flush = join(flush_all(&mut awaiting_synced), flush_all(&mut registered));
            let next_with_flush = immediate_or_join(get_next, flush);
            let (next, flush_result) = next_with_flush.await;
            if flush_result.is_some() {
                if registered.is_empty() && awaiting_synced.is_empty() {
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
                    state = ReadTaskState::Linked;
                    link(&mut awaiting_linked, &mut awaiting_synced, &mut registered).await;
                    if awaiting_synced.is_empty() && registered.is_empty() {
                        empty_timestamp = Some(Instant::now() + config.empty_timeout);
                    }
                }
                Notification::Synced => {
                    state = ReadTaskState::Synced;
                    sync_current(&mut awaiting_synced, &mut registered, &current).await;
                    if registered.is_empty() {
                        empty_timestamp = Some(Instant::now() + config.empty_timeout);
                    }
                }
                Notification::Event(bytes) => {
                    current.clear();
                    current.reserve(bytes.len());
                    current.put(bytes);
                    send_current(&mut registered, &current).await;
                    if registered.is_empty() && awaiting_synced.is_empty() {
                        empty_timestamp = Some(Instant::now() + config.empty_timeout);
                        flushed = true;
                    } else {
                        flushed = false;
                    }
                }
                Notification::Unlinked(_) => {
                    break;
                }
            },
            Some(Either::Right((writer, options))) => {
                let mut dl_writer = DownlinkSender::new(writer, options);
                if matches!(state, ReadTaskState::Init) {
                    empty_timestamp = None;
                    awaiting_linked.push(dl_writer);
                } else if dl_writer.send(DownlinkNotification::Linked).await.is_ok() {
                    empty_timestamp = None;
                    awaiting_synced.push(dl_writer);
                }
            }
            _ => {
                break;
            }
        }
    }
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

enum WriteState<F> {
    Idle {
        message_writer: RequestSender,
        buffer: BytesMut,
    },
    Writing(F),
    Flushing {
        flush: OwningFlush,
        buffer: BytesMut,
    },
}

enum WriteKind {
    Sync,
    Data,
}

async fn write_task(
    output: ByteWriter,
    producers: mpsc::Receiver<(ByteReader, DownlinkOptions)>,
    stopping: trigger::Receiver,
    identity: RoutingAddr,
    path: RelativePath,
    config: Config,
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
        (result, message_writer, buffer)
    };

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
                            break 'outer;
                        } else {
                            task_state |= WriteTaskState::FLUSHED;
                        }
                        req_result
                    };
                    if let Ok(Some((reader, options))) = req_result {
                        let receiver = DownlinkReceiver::new(reader, options, next_id());
                        registered.push(receiver);
                        if options.contains(DownlinkOptions::SYNC) {
                            let write = suspend_write(message_writer, buffer, WriteKind::Sync);
                            state = WriteState::Writing(write);
                        } else {
                            state = WriteState::Idle {
                                message_writer,
                                buffer,
                            };
                        }
                    } else {
                        // Either we are stopping or no one registered within the timeout.
                        break;
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
                                break 'outer;
                            }
                        };
                        (discard(next_op), flush_outcome)
                    };
                    match next_op {
                        Either::Left(Some((reader, options))) => {
                            let receiver = DownlinkReceiver::new(reader, options, next_id());
                            registered.push(receiver);
                            match flush_outcome {
                                Either::Left(message_writer) => {
                                    if options.contains(DownlinkOptions::SYNC) {
                                        let write =
                                            suspend_write(message_writer, buffer, WriteKind::Sync);
                                        state = WriteState::Writing(write);
                                    } else {
                                        state = WriteState::Idle {
                                            message_writer,
                                            buffer,
                                        };
                                    }
                                }
                                Either::Right(flush) => {
                                    task_state.set_needs_sync(options);
                                    state = WriteState::Flushing { flush, buffer };
                                }
                            }
                        }
                        Either::Left(_) => {
                            break 'outer;
                        }
                        Either::Right(Some(Ok(DownlinkOperation { body }))) => {
                            match flush_outcome {
                                Either::Left(message_writer) => {
                                    buffer.clear();
                                    buffer.reserve(body.len());
                                    buffer.put(body);
                                    let write_fut =
                                        suspend_write(message_writer, buffer, WriteKind::Data);
                                    task_state
                                        .remove(WriteTaskState::FLUSHED | WriteTaskState::UPDATED);
                                    state = WriteState::Writing(write_fut);
                                }
                                Either::Right(flush) => {
                                    current.clear();
                                    current.reserve(body.len());
                                    current.put(body);
                                    task_state |= WriteTaskState::UPDATED;
                                    state = WriteState::Flushing { flush, buffer };
                                }
                            }
                        }
                        Either::Right(Some(Err(Failed(id)))) => {
                            if let Some(rx) = registered.iter_mut().find(|rx| rx.id == id) {
                                rx.terminate();
                            }
                            state = update_write_state(flush_outcome, buffer);
                        }
                        Either::Right(_) => {
                            state = update_write_state(flush_outcome, buffer);
                        }
                    }
                }
            }
            WriteState::Writing(write_fut) => {
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
                        SuspendedResult::SuspendedCompleted((
                            result,
                            message_writer,
                            mut buffer,
                        )) => {
                            if result.is_err() {
                                break 'outer;
                            }
                            if task_state.contains(WriteTaskState::NEEDS_SYNC) {
                                task_state
                                    .remove(WriteTaskState::FLUSHED | WriteTaskState::NEEDS_SYNC);
                                let write = suspend_write(message_writer, buffer, WriteKind::Sync);
                                state = WriteState::Writing(write);
                            } else if task_state.contains(WriteTaskState::UPDATED) {
                                std::mem::swap(&mut buffer, &mut current);
                                let write_fut =
                                    suspend_write(message_writer, buffer, WriteKind::Data);
                                task_state
                                    .remove(WriteTaskState::FLUSHED | WriteTaskState::UPDATED);
                                state = WriteState::Writing(write_fut);
                            } else {
                                state = WriteState::Idle {
                                    message_writer,
                                    buffer,
                                };
                            }
                            break 'inner;
                        }
                        SuspendedResult::NextRecord(Some(Ok(DownlinkOperation { body }))) => {
                            current.clear();
                            current.reserve(body.len());
                            current.put(body);
                            task_state |= WriteTaskState::UPDATED;
                        }
                        SuspendedResult::NextRecord(Some(Err(Failed(id)))) => {
                            if let Some(rx) = registered.iter_mut().find(|rx| rx.id == id) {
                                rx.terminate();
                            }
                        }
                        SuspendedResult::NewRegistration(Some((reader, options))) => {
                            let receiver = DownlinkReceiver::new(reader, options, next_id());
                            registered.push(receiver);
                            task_state.set_needs_sync(options);
                        }
                        SuspendedResult::NewRegistration(_) => {
                            break 'outer;
                        }
                        _ => {}
                    }
                }
            }
            WriteState::Flushing {
                mut flush,
                mut buffer,
            } => {
                let result = if registered.is_empty() {
                    task_state.remove(WriteTaskState::NEEDS_SYNC);
                    match select(&mut flush, reg_requests.next()).await {
                        Either::Left((flush_result, _)) => {
                            SuspendedResult::SuspendedCompleted(flush_result)
                        }
                        Either::Right((request, _)) => SuspendedResult::NewRegistration(request),
                    }
                } else {
                    await_suspended(&mut flush, registered.next(), reg_requests.next()).await
                };

                match result {
                    SuspendedResult::SuspendedCompleted(Ok(message_writer)) => {
                        if task_state.contains(WriteTaskState::NEEDS_SYNC) {
                            task_state.remove(WriteTaskState::FLUSHED | WriteTaskState::NEEDS_SYNC);
                            let write = suspend_write(message_writer, buffer, WriteKind::Sync);
                            state = WriteState::Writing(write);
                        } else if task_state.contains(WriteTaskState::UPDATED) {
                            std::mem::swap(&mut buffer, &mut current);
                            let write_fut = suspend_write(message_writer, buffer, WriteKind::Data);
                            task_state.remove(WriteTaskState::FLUSHED | WriteTaskState::UPDATED);
                            state = WriteState::Writing(write_fut);
                        } else {
                            state = WriteState::Idle {
                                message_writer,
                                buffer,
                            };
                        }
                    }
                    SuspendedResult::NextRecord(result) => {
                        match result {
                            Some(Ok(DownlinkOperation { body })) => {
                                current.clear();
                                current.reserve(body.len());
                                current.put(body);
                                task_state |= WriteTaskState::UPDATED;
                            }
                            Some(Err(Failed(id))) => {
                                if let Some(rx) = registered.iter_mut().find(|rx| rx.id == id) {
                                    rx.terminate();
                                }
                            }
                            _ => {}
                        }
                        state = WriteState::Flushing { flush, buffer };
                    }
                    SuspendedResult::NewRegistration(Some((reader, options))) => {
                        let receiver = DownlinkReceiver::new(reader, options, next_id());
                        registered.push(receiver);
                        task_state.set_needs_sync(options);
                        state = WriteState::Flushing { flush, buffer };
                    }
                    _ => {
                        break 'outer;
                    }
                }
            }
        }
    }
}

fn update_write_state<F>(
    flush_outcome: Either<RequestSender, OwningFlush>,
    buffer: BytesMut,
) -> WriteState<F> {
    match flush_outcome {
        Either::Left(message_writer) => WriteState::Idle {
            message_writer,
            buffer,
        },
        Either::Right(flush) => WriteState::Flushing { flush, buffer },
    }
}

use futures::ready;
use std::pin::Pin;
use std::task::{Context, Poll};

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
