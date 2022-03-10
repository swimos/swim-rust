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

use futures::{SinkExt, StreamExt, future::join3};
use swim_api::{protocol::{map::{MapOperation, MapMessage, MapOperationEncoder}, downlink::{MapNotificationDecoder, DownlinkNotification}}, error::DownlinkTaskError};
use swim_form::{Form, structural::read::recognizer::RecognizerReadable};
use swim_model::{path::RelativePath, Text};
use swim_utilities::{trigger, io::byte_channel::{self, ByteWriter, ByteReader}};
use tokio::{sync::mpsc, io::AsyncWriteExt, time::timeout};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use std::future::Future;

use crate::{downlink::{AttachAction, DownlinkOptions, DownlinkRuntimeConfig, MapDownlinkRuntime, failure::AlwaysAbortStrategy}, compat::{ResponseMessageEncoder, AgentMessageDecoder, ResponseMessage, RequestMessage, MessageDecodeError}};

use super::*;

const SEND_INSERTION: i32 = -1;

#[derive(Debug, PartialEq, Eq, Form, Clone, Copy)]
struct Record {
    a: i32,
    b: i32,
}

type Event = (State, DownlinkNotification<MapMessage<i32, Record>>);

async fn run_fake_downlink(
    sub: mpsc::Sender<AttachAction>,
    options: DownlinkOptions,
    start: trigger::Receiver,
    event_tx: mpsc::UnboundedSender<Event>,
) -> Result<(), DownlinkTaskError> {
    if start.await.is_err() {
        return Err(DownlinkTaskError::FailedToStart);
    }

    let (tx_in, rx_in) = byte_channel::byte_channel(BUFFER_SIZE);
    let (tx_out, rx_out) = byte_channel::byte_channel(BUFFER_SIZE);
    if sub
        .send(AttachAction::new(rx_out, tx_in, options))
        .await
        .is_err()
    {
        return Err(DownlinkTaskError::FailedToStart);
    }
    let mut state = State::Unlinked;
    let mut read = FramedRead::new(
        rx_in,
        MapNotificationDecoder::<i32, Record>::default(),
    );

    let mut write = FramedWrite::new(tx_out, MapOperationEncoder);

    while let Some(message) = read.next().await.transpose()? {
        assert!(event_tx.send((state, message.clone())).is_ok());
        match state {
            State::Unlinked => match message {
                DownlinkNotification::Linked => {
                    state = State::Linked;
                }
                DownlinkNotification::Synced => {
                    state = State::Synced;
                }
                _ => {}
            },
            State::Linked => match message {
                DownlinkNotification::Synced => {
                    state = State::Synced;
                }
                DownlinkNotification::Unlinked => {
                    break;
                }
                _ => {}
            },
            State::Synced => match message {
                DownlinkNotification::Unlinked => {
                    break;
                }
                DownlinkNotification::Event { body } => {
                    if let MapMessage::Remove {key: SEND_INSERTION } = body {
                        if write
                            .send(MapOperation::Update { key: 42, value: Record { a: 1, b : 2}})
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                _ => {}
            },
        }
    }
    Ok(())
}

struct TestContext {
    tx: TestSender,
    rx: TestReceiver<i32, Record>,
    start_client: trigger::Sender,
    stop: trigger::Sender,
    events: UnboundedReceiverStream<Event>,
}


struct TestSender(FramedWrite<ByteWriter, ResponseMessageEncoder>);

type MsgDecoder<K, V> = AgentMessageDecoder<MapOperation<K, V>, <MapOperation<K, V> as RecognizerReadable>::Rec>;
struct TestReceiver<K: RecognizerReadable, V: RecognizerReadable>(FramedRead<ByteReader, MsgDecoder<K, V>>);

impl TestSender {
    fn new(writer: ByteWriter) -> Self {
        TestSender(FramedWrite::new(writer, ResponseMessageEncoder))
    }

    async fn link(&mut self) {
        self.send(ResponseMessage::linked(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
        ))
        .await;
    }

    async fn sync(&mut self) {
        self.send(ResponseMessage::synced(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
        ))
        .await;
    }

    async fn send(&mut self, message: ResponseMessage<MapMessage<i32, Record>, &[u8]>) {
        assert!(self.0.send(message).await.is_ok());
    }

    async fn update(&mut self, key: i32, value: Record) {
        let message = ResponseMessage::event(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
            MapMessage::Update { key, value },
        );
        self.send(message).await;
    }

    async fn remove(&mut self, key: i32) {
        let message = ResponseMessage::event(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
            MapMessage::Remove { key },
        );
        self.send(message).await;
    }

    async fn clear(&mut self) {
        let message = ResponseMessage::event(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
            MapMessage::Clear,
        );
        self.send(message).await;
    }

    async fn take(&mut self, n: u64) {
        let message = ResponseMessage::event(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
            MapMessage::Take(n),
        );
        self.send(message).await;
    }

    async fn drop(&mut self, n: u64) {
        let message = ResponseMessage::event(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
            MapMessage::Drop(n),
        );
        self.send(message).await;
    }

    async fn update_text(&mut self, message: Text) {
        let message: ResponseMessage<Text, &[u8]> = ResponseMessage::event(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
            message,
        );
        assert!(self.0.send(message).await.is_ok());
    }

    async fn corrupted_frame(&mut self) {
        let inner = self.0.get_mut();
        assert!(inner.write_u128(REMOTE_ADDR.uuid().as_u128()).await.is_ok());
        assert!(inner.write_u32(REMOTE_NODE.len() as u32).await.is_ok());
        assert!(inner.write_u32(REMOTE_LANE.len() as u32).await.is_ok());
        assert!(inner.write_u64(0).await.is_ok());
        //Replacing the node name with invalid UTF8 will cause the decoder to fail.
        assert!(inner.write(BAD_UTF8).await.is_ok());
        assert!(inner.write(REMOTE_LANE.as_bytes()).await.is_ok());
    } 
}


const BAD_UTF8: &[u8] = &[0xf0, 0x28, 0x8c, 0x28, 0x00, 0x00, 0x00];

impl<K: RecognizerReadable, V: RecognizerReadable> TestReceiver<K, V> {
    fn new(reader: ByteReader) -> Self {
        TestReceiver(FramedRead::new(
            reader,
            MsgDecoder::new(MapOperation::<K, V>::make_recognizer()),
        ))
    }

    async fn recv(&mut self) -> Option<Result<RequestMessage<MapOperation<K, V>>, MessageDecodeError>> {
        self.0.next().await
    }
}

async fn run_test<F, Fut>(
    options: DownlinkOptions,
    test_block: F,
) -> (Fut::Output, Result<(), DownlinkTaskError>)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future + Send + 'static,
{
    run_test_with_config(
        options,
        DownlinkRuntimeConfig {
            empty_timeout: EMPTY_TIMEOUT,
            attachment_queue_size: ATT_QUEUE_SIZE,
        },
        test_block,
    )
    .await
}

async fn run_test_with_config<F, Fut>(
    options: DownlinkOptions,
    config: DownlinkRuntimeConfig,
    test_block: F,
) -> (Fut::Output, Result<(), DownlinkTaskError>)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future + Send + 'static,
{
    let (attach_tx, attach_rx) = mpsc::channel(CHANNEL_SIZE);
    let (start_tx, start_rx) = trigger::trigger();
    let (stop_tx, stop_rx) = trigger::trigger();
    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let downlink = run_fake_downlink(attach_tx.clone(), options, start_rx, event_tx);

    let (in_tx, in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let path = RelativePath::new("/node", "lane");

    let management_task = MapDownlinkRuntime::new(
        attach_rx,
        (out_tx, in_rx),
        stop_rx,
        RoutingAddr::client(1),
        path,
        config,
        AlwaysAbortStrategy
    )
    .run();

    let test_task = test_block(TestContext {
        tx: TestSender::new(in_tx),
        rx: TestReceiver::new(out_rx),
        start_client: start_tx,
        stop: stop_tx,
        events: UnboundedReceiverStream::new(event_rx),
    });

    let (_, task_res, result) = timeout(TEST_TIMEOUT, join3(management_task, test_task, downlink))
        .await
        .unwrap();
    (task_res, result)
}