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

use std::cell::Cell;
use std::collections::HashSet;
use std::time::Duration;

use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::{join, join3, select as select_fut, Either};
use futures::stream::{select, unfold, SelectAll};
use futures::{SinkExt, Stream, StreamExt};
use pin_utils::pin_mut;
use swim_api::protocol::{
    DownlinkNotification, DownlinkNotificationEncoder, DownlinkOperation, DownlinkOperationDecoder,
};
use swim_model::path::RelativePath;
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
    pub flush_timout: Duration,
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

fn stream_timeout<'a, S: 'a>(
    stream: S,
    duration: Duration,
    flushed: &'a Cell<bool>,
) -> impl Stream<Item = Result<S::Item, ()>> + 'a
where
    S: Stream + Unpin,
{
    unfold(stream, move |mut s| async move {
        if !flushed.get() {
            match timeout(duration, s.next()).await {
                Ok(Some(v)) => Some((Ok(v), s)),
                Ok(_) => None,
                Err(_) => Some((Err(()), s)),
            }
        } else {
            s.next().await.map(move |v| (Ok(v), s))
        }
    })
}

async fn read_task(
    input: ByteReader,
    consumers: mpsc::Receiver<(ByteWriter, DownlinkOptions)>,
    stopping: trigger::Receiver,
    config: Config,
) {
    let messages = FramedRead::new(input, RawResponseMessageDecoder);

    let flushed = Cell::new(true);

    let messages_with_timeout =
        stream_timeout(messages, config.flush_timout, &flushed).map(Either::Left);

    pin_mut!(messages_with_timeout);
    let mut stream = select(
        messages_with_timeout,
        ReceiverStream::new(consumers).map(Either::Right),
    )
    .take_until(stopping);

    let mut state = ReadTaskState::Init;
    let mut current = BytesMut::new();
    let mut pending: Vec<DownlinkSender> = vec![];
    let mut registered: Vec<DownlinkSender> = vec![];

    let mut empty_timestamp = Some(Instant::now());
    loop {
        let next_item = if let Some(timestamp) = empty_timestamp {
            if let Ok(result) = timeout_at(timestamp, stream.next()).await {
                result
            } else {
                // No consumers registered within the timeout so stop.
                break;
            }
        } else {
            stream.next().await
        };

        match next_item {
            Some(Either::Left(Ok(Ok(RawResponseMessage { envelope, .. })))) => match envelope {
                Notification::Linked => {
                    state = ReadTaskState::Linked;
                }
                Notification::Synced => {
                    state = ReadTaskState::Synced;
                    sync_current(&mut pending, &current).await;
                    registered.extend(pending.drain(..));
                    if registered.is_empty() {
                        empty_timestamp = Some(Instant::now() + config.empty_timeout);
                    }
                }
                Notification::Event(bytes) => {
                    current.clear();
                    current.reserve(bytes.len());
                    current.put(bytes);
                    send_current(&mut registered, &current).await;
                    if registered.is_empty() && pending.is_empty() {
                        empty_timestamp = Some(Instant::now() + config.empty_timeout);
                        flushed.set(true);
                    } else {
                        flushed.set(false);
                    }
                }
                Notification::Unlinked(_) => {
                    break;
                }
            },
            Some(Either::Left(Err(_))) => {
                join(flush_all(&mut pending), flush_all(&mut registered)).await;
                if registered.is_empty() && pending.is_empty() {
                    empty_timestamp = Some(Instant::now() + config.empty_timeout);
                }
                flushed.set(true);
            }
            Some(Either::Right((writer, options))) => {
                let mut dl_writer = DownlinkSender::new(writer, options);
                if dl_writer.send(DownlinkNotification::Linked).await.is_ok() {
                    empty_timestamp = None;
                    if options.contains(DownlinkOptions::SYNC) && state != ReadTaskState::Synced {
                        pending.push(dl_writer);
                    } else {
                        registered.push(dl_writer);
                    }
                }
            }
            _ => {
                break;
            }
        }
    }
    unlink(pending).await;
    unlink(registered).await;
}

async fn sync_current(senders: &mut Vec<DownlinkSender>, current: &BytesMut) {
    let event = DownlinkNotification::Event { body: current };
    let mut failed = HashSet::<usize>::default();
    for (i, tx) in senders.iter_mut().enumerate() {
        if tx.feed(event).await.is_err() {
            failed.insert(i);
        } else {
            if tx.send(DownlinkNotification::Synced).await.is_err() {
                failed.insert(i);
            }
        }
    }
    clear_failed(senders, &failed);
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
}

enum WriteState<F> {
    Idle {
        message_writer: RequestSender,
        buffer: BytesMut,
    },
    Writing(F),
}

enum Suspended {
    Write,
    Flush,
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
    let mut updated = false;
    let mut registered: SelectAll<DownlinkReceiver> = SelectAll::new();
    let mut reg_requests = ReceiverStream::new(producers).take_until(stopping);
    let mut id: u64 = 0;
    let mut next_id = move || {
        let i = id;
        id += 1;
        i
    };
    let mut flushed = true;

    let suspend_write = |mut message_writer: RequestSender, buffer: BytesMut, option: Suspended| async move {
        let result = match option {
            Suspended::Write => message_writer
                .feed_command(buffer.as_ref())
                .await
                .map(|_| false),
            Suspended::Flush => message_writer.flush().await.map(|_| true),
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
                    if !flushed {
                        let write_fut = suspend_write(message_writer, buffer, Suspended::Flush);
                        state = WriteState::Writing(write_fut);
                    } else if let Ok(Some((reader, options))) =
                        timeout(config.empty_timeout, reg_requests.next()).await
                    {
                        let receiver = DownlinkReceiver::new(reader, options, next_id());
                        registered.push(receiver);
                        if options.contains(DownlinkOptions::SYNC) {
                            if message_writer.send_sync().await.is_err() {
                                break;
                            }
                        }
                        state = WriteState::Idle {
                            message_writer,
                            buffer,
                        };
                    } else {
                        // Either we are stopping or no one registered within the timeout.
                        break;
                    }
                } else {
                    let req_or_op = {
                        let next_op = async {
                            if flushed {
                                Ok(registered.next().await)
                            } else {
                                timeout(config.flush_timout, registered.next()).await
                            }
                        };
                        pin_mut!(next_op);
                        match select_fut(reg_requests.next(), next_op).await {
                            Either::Left((r, _)) => Either::Left(r),
                            Either::Right((r, _)) => Either::Right(r),
                        }
                    };

                    match req_or_op {
                        Either::Left(Some((reader, options))) => {
                            let receiver = DownlinkReceiver::new(reader, options, next_id());
                            registered.push(receiver);
                            if options.contains(DownlinkOptions::SYNC) {
                                if message_writer.send_sync().await.is_err() {
                                    break;
                                }
                            }
                            state = WriteState::Idle {
                                message_writer,
                                buffer,
                            };
                        }
                        Either::Left(_) => {
                            break;
                        }
                        Either::Right(Ok(Some(Ok(DownlinkOperation { body })))) => {
                            buffer.clear();
                            buffer.reserve(body.len());
                            buffer.put(body);
                            let write_fut = suspend_write(message_writer, buffer, Suspended::Write);
                            updated = false;
                            flushed = false;
                            state = WriteState::Writing(write_fut);
                        }
                        Either::Right(Ok(Some(Err(Failed(id))))) => {
                            if let Some(rx) = registered.iter_mut().find(|rx| rx.id == id) {
                                rx.terminate();
                            }
                            state = WriteState::Idle {
                                message_writer,
                                buffer,
                            };
                        }
                        Either::Right(Err(_)) => {
                            let write_fut = suspend_write(message_writer, buffer, Suspended::Flush);
                            state = WriteState::Writing(write_fut);
                        }
                        Either::Right(_) => {
                            state = WriteState::Idle {
                                message_writer,
                                buffer,
                            };
                        }
                    }
                }
            }
            WriteState::Writing(write_fut) => {
                pin_mut!(write_fut);
                'inner: loop {
                    if registered.is_empty() {
                        let (result, message_writer, mut buffer) = write_fut.await;
                        match result {
                            Err(_) => {
                                break 'outer;
                            }
                            Ok(did_flush) => {
                                flushed = did_flush;
                            }
                        }
                        if updated {
                            std::mem::swap(&mut buffer, &mut current);
                            let write_fut = suspend_write(message_writer, buffer, Suspended::Write);
                            updated = false;
                            flushed = false;
                            state = WriteState::Writing(write_fut);
                        } else {
                            state = WriteState::Idle {
                                message_writer,
                                buffer,
                            };
                        }
                        break 'inner;
                    } else {
                        match select_fut(&mut write_fut, registered.next()).await {
                            Either::Left(((result, message_writer, mut buffer), _)) => {
                                match result {
                                    Err(_) => {
                                        break 'outer;
                                    }
                                    Ok(did_flush) => {
                                        flushed = did_flush;
                                    }
                                }
                                if updated {
                                    std::mem::swap(&mut buffer, &mut current);
                                    let write_fut =
                                        suspend_write(message_writer, buffer, Suspended::Write);
                                    updated = false;
                                    flushed = false;
                                    state = WriteState::Writing(write_fut);
                                } else {
                                    state = WriteState::Idle {
                                        message_writer,
                                        buffer,
                                    };
                                }
                                break 'inner;
                            }
                            Either::Right((Some(Ok(DownlinkOperation { body })), _)) => {
                                current.clear();
                                current.reserve(body.len());
                                current.put(body);
                                updated = true;
                            }
                            Either::Right((Some(Err(Failed(id))), _)) => {
                                if let Some(rx) = registered.iter_mut().find(|rx| rx.id == id) {
                                    rx.terminate();
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}
