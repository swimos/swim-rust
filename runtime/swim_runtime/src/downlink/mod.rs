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
use bytes::{BytesMut, BufMut};
use futures::{StreamExt, SinkExt, Stream};
use futures::future::{join, join3, Either};
use futures::stream::{select, unfold};
use swim_api::error::DownlinkTaskError;
use swim_api::protocol::{DownlinkNotification, DownlinkNotificationEncoder, DownlinkOperationDecoder};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio::time::{timeout_at, timeout, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite, Encoder};
use pin_utils::pin_mut;

use crate::compat::{RawResponseMessageDecoder, RawResponseMessage, Notification};

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
    empty_timeout: Duration,
    flush_timout: Duration,
}

pub struct ValueDownlinkManagementTask {
    requests: mpsc::Receiver<AttachAction>,
    input: ByteReader,
    output: ByteWriter,
    stopping: trigger::Receiver,
    config: Config,
}

impl ValueDownlinkManagementTask {
    pub fn new(
        requests: mpsc::Receiver<AttachAction>,
        input: ByteReader,
        output: ByteWriter,
        stopping: trigger::Receiver,
        config: Config,
    ) -> Self {
        ValueDownlinkManagementTask {
            requests,
            input,
            output,
            stopping,
            config,
        }
    }

    pub async fn run(self) -> Result<(), DownlinkTaskError> {
        let ValueDownlinkManagementTask {
            requests,
            input,
            output,
            stopping,
            config
        } = self;
        
        let (producer_tx, producer_rx) = mpsc::channel(8);
        let (consumer_tx, consumer_rx) = mpsc::channel(8);
        let att = attach_task(requests, producer_tx, consumer_tx, stopping.clone());
        let read = read_task(input, consumer_rx, stopping.clone(), config);
        let write = write_task(output, producer_rx, stopping);
        let (_, read_result, write_result) = join3(att, read, write).await;
        read_result.and(write_result)
    }
}

async fn attach_task(
    rx: mpsc::Receiver<AttachAction>,
    producer_tx: mpsc::Sender<(ByteReader, DownlinkOptions)>,
    consumer_tx: mpsc::Sender<(ByteWriter, DownlinkOptions)>,
    stopping: trigger::Receiver,
) {
    let mut stream = ReceiverStream::new(rx).take_until(stopping);
    while let Some(AttachAction { input, output, options }) = stream.next().await {
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

    async fn send(&mut self, mesage: DownlinkNotification<&BytesMut>) -> Result<(), std::io::Error> {
        self.sender.send(mesage).await
    }

    async fn flush(&mut self) -> Result<(), std::io::Error> {
        flush_sender(&mut self.sender).await
    }
}

async fn flush_sender<T>(sender: &mut FramedWrite<ByteWriter, T>) -> Result<(), T::Error>
where
    T: Encoder<DownlinkNotification<&'static BytesMut>> 
{
    sender.flush().await
}

type DownlinkReceiver = FramedRead<ByteReader, DownlinkOperationDecoder>;

fn stream_timeout<'a, S: 'a>(stream: S, 
    duration: Duration,
    flushed: &'a Cell<bool>) -> impl Stream<Item = Result<S::Item, ()>> + 'a
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

async fn read_task(input: ByteReader, 
    consumers: mpsc::Receiver<(ByteWriter, DownlinkOptions)>, 
    stopping: trigger::Receiver,
    config: Config) -> Result<(), DownlinkTaskError> {
    let messages = FramedRead::new(input, RawResponseMessageDecoder);
    
    let flushed = Cell::new(true);

    let messages_with_timeout = stream_timeout(messages, config.flush_timout, &flushed).map(Either::Left);

    pin_mut!(messages_with_timeout);
    let mut stream = select(messages_with_timeout, ReceiverStream::new(consumers).map(Either::Right)).take_until(stopping);
    
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
            Some(Either::Left(Ok(Ok(RawResponseMessage { envelope, ..})))) => {
                match envelope {
                    Notification::Linked => {
                        state = ReadTaskState::Linked;
                    },
                    Notification::Synced => {
                        state = ReadTaskState::Synced;
                        sync_current(&mut pending, &current).await;
                        registered.extend(pending.drain(..));
                        if registered.is_empty() {
                            empty_timestamp = Some(Instant::now() + config.empty_timeout);
                        }
                    },
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
                    },
                    Notification::Unlinked(_) => {
                        break;
                    },
                }
            },
            Some(Either::Left(Err(_))) => {
                join(flush_all(&mut pending), flush_all(&mut registered)).await;
                if registered.is_empty() && pending.is_empty() {
                    empty_timestamp = Some(Instant::now() + config.empty_timeout);  
                }
                flushed.set(true);
                todo!()
            },
            Some(Either::Right((writer, options))) => {
                let dl_writer = DownlinkSender::new(writer, options);
                empty_timestamp = None;
                if options.contains(DownlinkOptions::SYNC) && state != ReadTaskState::Synced {
                    pending.push(dl_writer);
                } else {
                    registered.push(dl_writer);
                }
            },
            _ => {
                break;
            }
        }
    }
    Ok(())
}

async fn sync_current(senders: &mut Vec<DownlinkSender>, current: &BytesMut) {
    let event = DownlinkNotification::Event { body: current };
    let mut failed = HashSet::<usize>::default();
    for (i, tx) in senders.iter_mut().enumerate() {
        if tx.send(event).await.is_err() {
            failed.insert(i);
        } else {
            if tx.send(DownlinkNotification::Synced).await.is_err() {
                failed.insert(i);
            } else {
                if tx.flush().await.is_err() {
                    failed.insert(i);
                }
            }
        }
    }
    clear_failed(senders, &failed);
}

async fn send_current(senders: &mut Vec<DownlinkSender>, current: &BytesMut) {
    let event = DownlinkNotification::Event { body: current };
    let mut failed = HashSet::<usize>::default();
    for (i, tx) in senders.iter_mut().enumerate() {
        if tx.send(event).await.is_err() {
            failed.insert(i);
        }
    }
    clear_failed(senders, &failed);
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

async fn write_task(output: ByteWriter, producers: mpsc::Receiver<(ByteReader, DownlinkOptions)>, stopping: trigger::Receiver) -> Result<(), DownlinkTaskError> {

    todo!()
}
