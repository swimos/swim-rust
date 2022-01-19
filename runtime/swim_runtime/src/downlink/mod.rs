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

use bitflags::bitflags;
use bytes::BytesMut;
use futures::StreamExt;
use futures::future::{join3, Either};
use futures::stream::select;
use swim_api::error::DownlinkTaskError;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedRead;

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

pub struct ValueDownlinkManagementTask {
    requests: mpsc::Receiver<AttachAction>,
    input: ByteReader,
    output: ByteWriter,
    stopping: trigger::Receiver,
}

impl ValueDownlinkManagementTask {
    pub fn new(
        requests: mpsc::Receiver<AttachAction>,
        input: ByteReader,
        output: ByteWriter,
        stopping: trigger::Receiver,
    ) -> Self {
        ValueDownlinkManagementTask {
            requests,
            input,
            output,
            stopping,
        }
    }

    pub async fn run(self) -> Result<(), DownlinkTaskError> {
        let ValueDownlinkManagementTask {
            requests,
            input,
            output,
            stopping,
        } = self;
        
        let (producer_tx, producer_rx) = mpsc::channel(8);
        let (consumer_tx, consumer_rx) = mpsc::channel(8);
        let att = attach_task(requests, producer_tx, consumer_tx, stopping.clone());
        let read = read_task(input, consumer_rx, stopping.clone());
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

async fn read_task(input: ByteReader, consumers: mpsc::Receiver<(ByteWriter, DownlinkOptions)>, stopping: trigger::Receiver) -> Result<(), DownlinkTaskError> {
    let messages = FramedRead::new(input, RawResponseMessageDecoder).map(Either::Left);
    let mut stream = select(messages, ReceiverStream::new(consumers).map(Either::Right)).take_until(stopping);
    
    let mut state = ReadTaskState::Init;
    let mut current = BytesMut::new();
    let mut pending: Vec<ByteWriter> = vec![];
    let mut registered: Vec<ByteWriter> = vec![];
    loop {
        match stream.next().await {
            Some(Either::Left(Ok(RawResponseMessage { envelope, ..}))) => {
                match envelope {
                    Notification::Linked => {
                        state = ReadTaskState::Linked;
                    },
                    Notification::Synced => {
                        state = ReadTaskState::Synced;

                    },
                    Notification::Event(bytes) => {
                        
                    },
                    Notification::Unlinked(_) => {
                        break;
                    },
                }
            },
            Some(Either::Right((writer, options))) => {
                if options.contains(DownlinkOptions::SYNC) && state != ReadTaskState::Synced {
                    pending.push(writer);
                } else {
                    registered.push(writer);
                }
            },
            _ => {
                break;
            }
        }
    }
    todo!()
}

async fn send_current(senders: Vec<ByteWriter>, current: &BytesMut) -> Vec<ByteWriter> {
    todo!()
}

async fn write_task(output: ByteWriter, producers: mpsc::Receiver<(ByteReader, DownlinkOptions)>, stopping: trigger::Receiver) -> Result<(), DownlinkTaskError> {

    todo!()
}
