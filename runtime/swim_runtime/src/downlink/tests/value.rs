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

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use crate::downlink::DownlinkRuntimeConfig;
use crate::routing::RoutingAddr;

use super::super::{AttachAction, DownlinkOptions, ValueDownlinkRuntime};
use super::*;
use futures::future::{join3, join4};
use futures::{SinkExt, StreamExt};
use swim_api::error::{DownlinkTaskError, FrameIoError, InvalidFrame};
use swim_api::protocol::downlink::{
    DownlinkNotification, DownlinkOperation, DownlinkOperationEncoder, ValueNotificationDecoder,
};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_messages::protocol::{
    AgentMessageDecoder, MessageDecodeError, Operation, Path, RequestMessage, ResponseMessage,
    ResponseMessageEncoder,
};
use swim_model::path::RelativePath;
use swim_model::Text;
use swim_utilities::io::byte_channel::{self, ByteReader, ByteWriter};
use swim_utilities::trigger;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug, Form, PartialEq, Eq, Clone)]
#[form_root(::swim_form)]
enum Message {
    Ping,
    Fail,
    CurrentValue(Text),
}

type Event = (State, DownlinkNotification<Message>);

struct TestContext {
    tx: TestSender,
    rx: TestReceiver<Message>,
    start_client: trigger::Sender,
    stop: trigger::Sender,
    events: UnboundedReceiverStream<Event>,
}

struct SyncedTestContext {
    tx: TestSender,
    rx: TestReceiver<Message>,
    stop: trigger::Sender,
    events: UnboundedReceiverStream<Event>,
}

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
    let mut read = FramedRead::new(rx_in, ValueNotificationDecoder::default());

    let mut write = FramedWrite::new(tx_out, DownlinkOperationEncoder);

    let mut current = Text::default();

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
                DownlinkNotification::Event {
                    body: Message::CurrentValue(v),
                } => {
                    current = v;
                }
                _ => {}
            },
            State::Synced => match message {
                DownlinkNotification::Unlinked => {
                    break;
                }
                DownlinkNotification::Event {
                    body: Message::CurrentValue(v),
                } => {
                    current = v;
                }
                DownlinkNotification::Event {
                    body: Message::Ping,
                } => {
                    let response = Message::CurrentValue(current.clone());
                    if write
                        .send(DownlinkOperation { body: response })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                DownlinkNotification::Event {
                    body: Message::Fail,
                } => {
                    return Err(DownlinkTaskError::BadFrame(FrameIoError::BadFrame(
                        InvalidFrame::Incomplete,
                    )));
                }
                _ => {}
            },
        }
    }
    Ok(())
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

    let management_task = ValueDownlinkRuntime::new(
        attach_rx,
        (out_tx, in_rx),
        stop_rx,
        *RoutingAddr::client(1).uuid(),
        path,
        config,
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

struct TestSender(FramedWrite<ByteWriter, ResponseMessageEncoder>);

type MsgDecoder<T> = AgentMessageDecoder<T, <T as RecognizerReadable>::Rec>;
struct TestReceiver<M: RecognizerReadable>(FramedRead<ByteReader, MsgDecoder<M>>);

impl TestSender {
    fn new(writer: ByteWriter) -> Self {
        TestSender(FramedWrite::new(writer, ResponseMessageEncoder))
    }

    async fn link(&mut self) {
        self.send(ResponseMessage::linked(
            REMOTE_ADDR,
            Path::new(REMOTE_NODE, REMOTE_LANE),
        ))
        .await;
    }

    async fn sync(&mut self) {
        self.send(ResponseMessage::synced(
            REMOTE_ADDR,
            Path::new(REMOTE_NODE, REMOTE_LANE),
        ))
        .await;
    }

    async fn send(&mut self, message: ResponseMessage<&str, Message, &[u8]>) {
        assert!(self.0.send(message).await.is_ok());
    }

    async fn update(&mut self, message: Message) {
        let message =
            ResponseMessage::event(REMOTE_ADDR, Path::new(REMOTE_NODE, REMOTE_LANE), message);
        self.send(message).await;
    }

    async fn update_text(&mut self, message: Text) {
        let message: ResponseMessage<&str, Text, &[u8]> =
            ResponseMessage::event(REMOTE_ADDR, Path::new(REMOTE_NODE, REMOTE_LANE), message);
        assert!(self.0.send(message).await.is_ok());
    }

    async fn corrupted_frame(&mut self) {
        let inner = self.0.get_mut();
        assert!(inner.write_u128(REMOTE_ADDR.as_u128()).await.is_ok());
        assert!(inner.write_u32(REMOTE_NODE.len() as u32).await.is_ok());
        assert!(inner.write_u32(REMOTE_LANE.len() as u32).await.is_ok());
        assert!(inner.write_u64(0).await.is_ok());
        //Replacing the node name with invalid UTF8 will cause the decoder to fail.
        assert!(inner.write(BAD_UTF8).await.is_ok());
        assert!(inner.write(REMOTE_LANE.as_bytes()).await.is_ok());
    }
}

const BAD_UTF8: &[u8] = &[0xf0, 0x28, 0x8c, 0x28, 0x00, 0x00, 0x00];

impl<M: RecognizerReadable> TestReceiver<M> {
    fn new(reader: ByteReader) -> Self {
        TestReceiver(FramedRead::new(
            reader,
            MsgDecoder::new(M::make_recognizer()),
        ))
    }

    async fn recv(&mut self) -> Option<Result<RequestMessage<Text, M>, MessageDecodeError>> {
        self.0.next().await
    }
}

fn expect_message<M: Eq + Debug>(
    result: Option<Result<RequestMessage<Text, M>, MessageDecodeError>>,
    message: Operation<M>,
) {
    match result {
        Some(Ok(m)) => {
            assert_eq!(m.envelope, message);
        }
        Some(Err(e)) => {
            panic!("Unexpected error: {}", e);
        }
        _ => {
            panic!("Unexpected termination.")
        }
    }
}

fn expect_event(result: Option<Event>, state: State, notification: DownlinkNotification<Message>) {
    if let Some(ev) = result {
        assert_eq!(ev, (state, notification));
    } else {
        panic!("Client stopped unexpectedly.");
    }
}

fn expect_text_event(
    result: Option<DownlinkNotification<Text>>,
    notification: DownlinkNotification<Text>,
) {
    if let Some(ev) = result {
        assert_eq!(ev, notification);
    } else {
        panic!("Client stopped unexpectedly.");
    }
}

#[tokio::test]
async fn shutdowm_none_attached() {
    let (events, result) = run_test(
        DownlinkOptions::empty(),
        |TestContext {
             mut rx,
             stop,
             events,
             ..
         }| async move {
            expect_message(rx.recv().await, Operation::Link);
            stop.trigger();
            events.collect::<Vec<_>>().await
        },
    )
    .await;
    assert!(matches!(result, Err(DownlinkTaskError::FailedToStart)));
    assert!(events.is_empty());
}

#[tokio::test]
async fn shutdowm_after_attached() {
    let (events, result) = run_test(
        DownlinkOptions::empty(),
        |TestContext {
             mut tx,
             mut rx,
             start_client,
             stop,
             mut events,
             ..
         }| async move {
            expect_message(rx.recv().await, Operation::Link);

            start_client.trigger();
            tx.link().await;

            expect_event(
                events.next().await,
                State::Unlinked,
                DownlinkNotification::Linked,
            );

            stop.trigger();
            events.collect::<Vec<_>>().await
        },
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(
        events,
        vec![(State::Linked, DownlinkNotification::Unlinked)]
    );
}

#[tokio::test]
async fn shutdowm_after_corrupted_frame() {
    let (events, result) = run_test(DownlinkOptions::empty(), |context| async move {
        let TestContext {
            mut tx,
            mut rx,
            start_client,
            stop: _stop,
            mut events,
            ..
        } = context;
        expect_message(rx.recv().await, Operation::Link);

        start_client.trigger();
        tx.link().await;

        expect_event(
            events.next().await,
            State::Unlinked,
            DownlinkNotification::Linked,
        );

        tx.corrupted_frame().await;
        events.collect::<Vec<_>>().await
    })
    .await;
    assert!(result.is_ok());
    assert_eq!(
        events,
        vec![(State::Linked, DownlinkNotification::Unlinked)]
    );
}

#[tokio::test]
async fn sync_from_nothing() {
    let (events, result) = run_test(
        DownlinkOptions::SYNC,
        |TestContext {
             mut tx,
             mut rx,
             start_client,
             stop,
             mut events,
             ..
         }| async move {
            expect_message(rx.recv().await, Operation::Link);

            start_client.trigger();
            tx.link().await;

            expect_event(
                events.next().await,
                State::Unlinked,
                DownlinkNotification::Linked,
            );
            expect_message(rx.recv().await, Operation::Sync);

            let message = Message::CurrentValue(Text::new("A"));
            tx.update(message.clone()).await;
            tx.sync().await;

            expect_event(
                events.next().await,
                State::Linked,
                DownlinkNotification::Event { body: message },
            );
            expect_event(
                events.next().await,
                State::Linked,
                DownlinkNotification::Synced,
            );

            stop.trigger();
            events.collect::<Vec<_>>().await
        },
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(
        events,
        vec![(State::Synced, DownlinkNotification::Unlinked)]
    );
}

#[tokio::test]
async fn sync_after_value() {
    let (events, result) = run_test(
        DownlinkOptions::SYNC,
        |TestContext {
             mut tx,
             mut rx,
             start_client,
             stop,
             mut events,
             ..
         }| async move {
            expect_message(rx.recv().await, Operation::Link);

            tx.link().await;

            let message1 = Message::CurrentValue(Text::new("A"));
            tx.update(message1).await;

            start_client.trigger();

            expect_event(
                events.next().await,
                State::Unlinked,
                DownlinkNotification::Linked,
            );
            expect_message(rx.recv().await, Operation::Sync);

            let message2 = Message::CurrentValue(Text::new("B"));
            tx.update(message2.clone()).await;
            tx.sync().await;

            expect_event(
                events.next().await,
                State::Linked,
                DownlinkNotification::Event { body: message2 },
            );
            expect_event(
                events.next().await,
                State::Linked,
                DownlinkNotification::Synced,
            );

            stop.trigger();
            events.collect::<Vec<_>>().await
        },
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(
        events,
        vec![(State::Synced, DownlinkNotification::Unlinked)]
    );
}

async fn sync_client_then<F, Fut>(context: TestContext, f: F) -> Vec<Event>
where
    F: FnOnce(SyncedTestContext) -> Fut,
    Fut: Future<Output = UnboundedReceiverStream<Event>> + Send + 'static,
{
    let TestContext {
        mut tx,
        mut rx,
        start_client,
        stop,
        mut events,
    } = context;
    expect_message(rx.recv().await, Operation::Link);

    start_client.trigger();
    tx.link().await;

    expect_event(
        events.next().await,
        State::Unlinked,
        DownlinkNotification::Linked,
    );
    expect_message(rx.recv().await, Operation::Sync);

    let message = Message::CurrentValue(Text::new("A"));
    tx.update(message.clone()).await;
    tx.sync().await;

    expect_event(
        events.next().await,
        State::Linked,
        DownlinkNotification::Event { body: message },
    );
    expect_event(
        events.next().await,
        State::Linked,
        DownlinkNotification::Synced,
    );
    let events = f(SyncedTestContext {
        tx,
        rx,
        stop,
        events,
    })
    .await;
    events.collect::<Vec<_>>().await
}

#[tokio::test]
async fn receive_commands() {
    let (events, result) = run_test(DownlinkOptions::SYNC, |context| {
        sync_client_then(
            context,
            |SyncedTestContext {
                 mut tx,
                 mut rx,
                 stop,
                 mut events,
             }| async move {
                tx.update(Message::Ping).await;
                expect_event(
                    events.next().await,
                    State::Synced,
                    DownlinkNotification::Event {
                        body: Message::Ping,
                    },
                );
                expect_message(
                    rx.recv().await,
                    Operation::Command(Message::CurrentValue(Text::new("A"))),
                );

                tx.update(Message::CurrentValue(Text::new("B"))).await;
                expect_event(
                    events.next().await,
                    State::Synced,
                    DownlinkNotification::Event {
                        body: Message::CurrentValue(Text::new("B")),
                    },
                );
                tx.update(Message::Ping).await;
                expect_event(
                    events.next().await,
                    State::Synced,
                    DownlinkNotification::Event {
                        body: Message::Ping,
                    },
                );
                expect_message(
                    rx.recv().await,
                    Operation::Command(Message::CurrentValue(Text::new("B"))),
                );
                stop.trigger();

                events
            },
        )
    })
    .await;

    assert!(result.is_ok());
    assert_eq!(
        events,
        vec![(State::Synced, DownlinkNotification::Unlinked)]
    );
}

#[tokio::test]
async fn handle_failed_consumer() {
    let (events, result) = run_test(DownlinkOptions::SYNC, |context| {
        sync_client_then(context, |context| async move {
            let SyncedTestContext {
                mut tx,
                rx: _rx,
                stop: _stop, //No explicit stop, shutdown should ocurr becase there are no remaining consumers.
                mut events,
            } = context;
            tx.update(Message::Fail).await; //Cause the consumer to fail.
            expect_event(
                events.next().await,
                State::Synced,
                DownlinkNotification::Event {
                    body: Message::Fail,
                },
            );
            tx.update(Message::Ping).await; //Sending another messasge should cause the write task to notice the failure.
            events
        })
    })
    .await;

    assert!(matches!(result, Err(DownlinkTaskError::BadFrame(_))));
    assert!(events.is_empty());
}

const LIMIT: usize = 150;

#[tokio::test]
async fn exhaust_output_buffer() {
    let final_body = LIMIT.to_string();
    let (events, result) = run_test(DownlinkOptions::SYNC, move |context| {
        sync_client_then(
            context,
            |SyncedTestContext {
                 mut tx,
                 mut rx,
                 stop,
                 mut events,
             }| async move {
                for i in 0..(LIMIT + 1) {
                    let i_text = Text::from(format!("{}", i));
                    tx.update(Message::CurrentValue(i_text.clone())).await;
                    expect_event(
                        events.next().await,
                        State::Synced,
                        DownlinkNotification::Event {
                            body: Message::CurrentValue(i_text),
                        },
                    );
                    tx.update(Message::Ping).await;
                    expect_event(
                        events.next().await,
                        State::Synced,
                        DownlinkNotification::Event {
                            body: Message::Ping,
                        },
                    );
                }
                let mut messages = vec![];
                loop {
                    let result = rx.recv().await;
                    match result {
                        Some(Ok(RequestMessage {
                            envelope: Operation::Command(Message::CurrentValue(body)),
                            ..
                        })) => {
                            let fin = body == final_body;
                            messages.push(body);
                            if fin {
                                break;
                            }
                        }
                        ow => panic!("Unexpected result: {:?}", ow),
                    }
                }
                assert!(messages.len() < LIMIT);
                let mut prev = None;
                for message in messages.into_iter() {
                    let j = message.as_str().parse::<usize>().unwrap();
                    if let Some(i) = prev {
                        assert!(i < j);
                    }
                    prev = Some(j);
                }
                stop.trigger();
                events
            },
        )
    })
    .await;

    assert!(result.is_ok());
    assert_eq!(
        events,
        vec![(State::Synced, DownlinkNotification::Unlinked)]
    );
}

#[tokio::test]
async fn shutdowm_after_timeout_with_no_subscribers() {
    let ((_stop, events), result) = run_test_with_config(
        DownlinkOptions::empty(),
        DownlinkRuntimeConfig {
            empty_timeout: Duration::from_millis(100),
            attachment_queue_size: ATT_QUEUE_SIZE,
        },
        |TestContext {
             tx: _tx,
             mut rx,
             stop,
             events,
             start_client,
         }| async move {
            expect_message(rx.recv().await, Operation::Link);
            drop(start_client);
            (stop, events)
        },
    )
    .await;
    assert!(matches!(result, Err(DownlinkTaskError::FailedToStart)));
    assert!(events.collect::<Vec<_>>().await.is_empty());
}

struct ConsumerContext {
    start_client: Option<trigger::Sender>,
    events: UnboundedReceiverStream<DownlinkNotification<Text>>,
}

struct TwoConsumerTestContext {
    tx: TestSender,
    rx: TestReceiver<Text>,
    stop: trigger::Sender,
    first_consumer: ConsumerContext,
    second_consumer: ConsumerContext,
}

async fn run_simple_fake_downlink(
    tag: &'static str,
    sub: mpsc::Sender<AttachAction>,
    options: DownlinkOptions,
    start: trigger::Receiver,
    event_tx: mpsc::UnboundedSender<DownlinkNotification<Text>>,
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
    let mut read = FramedRead::new(rx_in, ValueNotificationDecoder::default());

    let mut write = FramedWrite::new(tx_out, DownlinkOperationEncoder);

    while let Some(message) = read.next().await.transpose()? {
        assert!(event_tx.send(message.clone()).is_ok());
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
                    if body == tag {
                        let response = Text::from(format!("Response from {}.", tag));
                        if write
                            .send(DownlinkOperation { body: response })
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

const FIRST_TAG: &str = "A";
const SECOND_TAG: &str = "B";

async fn run_test_with_two_consumers<F, Fut>(
    options: DownlinkOptions,
    config: DownlinkRuntimeConfig,
    test_block: F,
) -> (
    Fut::Output,
    Result<(), DownlinkTaskError>,
    Result<(), DownlinkTaskError>,
)
where
    F: FnOnce(TwoConsumerTestContext) -> Fut,
    Fut: Future + Send + 'static,
{
    let (attach_tx, attach_rx) = mpsc::channel(CHANNEL_SIZE);
    let (start_tx1, start_rx1) = trigger::trigger();
    let (start_tx2, start_rx2) = trigger::trigger();
    let (stop_tx, stop_rx) = trigger::trigger();
    let (event_tx1, event_rx1) = mpsc::unbounded_channel();
    let (event_tx2, event_rx2) = mpsc::unbounded_channel();

    let downlink1 =
        run_simple_fake_downlink(FIRST_TAG, attach_tx.clone(), options, start_rx1, event_tx1);
    let downlink2 =
        run_simple_fake_downlink(SECOND_TAG, attach_tx.clone(), options, start_rx2, event_tx2);

    let (in_tx, in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let path = RelativePath::new("/node", "lane");

    let management_task = ValueDownlinkRuntime::new(
        attach_rx,
        (out_tx, in_rx),
        stop_rx,
        *RoutingAddr::client(1).uuid(),
        path,
        config,
    )
    .run();

    let context = TwoConsumerTestContext {
        tx: TestSender::new(in_tx),
        rx: TestReceiver::new(out_rx),
        stop: stop_tx,
        first_consumer: ConsumerContext {
            start_client: Some(start_tx1),
            events: UnboundedReceiverStream::new(event_rx1),
        },
        second_consumer: ConsumerContext {
            start_client: Some(start_tx2),
            events: UnboundedReceiverStream::new(event_rx2),
        },
    };

    let test_task = test_block(context);

    let (_, task_res, result1, result2) = timeout(
        TEST_TIMEOUT,
        join4(management_task, test_task, downlink1, downlink2),
    )
    .await
    .unwrap();
    (task_res, result1, result2)
}

async fn sync_both(context: &mut TwoConsumerTestContext) {
    let TwoConsumerTestContext {
        tx,
        rx,
        first_consumer,
        second_consumer,
        ..
    } = context;
    let ConsumerContext {
        start_client: start_first,
        events: first_events,
    } = first_consumer;

    let ConsumerContext {
        start_client: start_second,
        events: second_events,
    } = second_consumer;

    expect_message(rx.recv().await, Operation::Link);

    if let Some(start) = start_first.take() {
        start.trigger();
    }
    if let Some(start) = start_second.take() {
        start.trigger();
    }
    tx.link().await;

    expect_text_event(first_events.next().await, DownlinkNotification::Linked);
    expect_text_event(second_events.next().await, DownlinkNotification::Linked);
    expect_message(rx.recv().await, Operation::Sync);
    expect_message(rx.recv().await, Operation::Sync);

    let content = Text::new("Content");
    tx.update_text(content.clone()).await;
    tx.sync().await;

    expect_text_event(
        first_events.next().await,
        DownlinkNotification::Event {
            body: content.clone(),
        },
    );
    expect_text_event(
        second_events.next().await,
        DownlinkNotification::Event { body: content },
    );
    expect_text_event(first_events.next().await, DownlinkNotification::Synced);
    expect_text_event(second_events.next().await, DownlinkNotification::Synced);
}

#[tokio::test]
async fn sync_two_consumers() {
    let ((first_events, second_events), first_result, second_result) = run_test_with_two_consumers(
        DownlinkOptions::SYNC,
        DownlinkRuntimeConfig {
            empty_timeout: EMPTY_TIMEOUT,
            attachment_queue_size: ATT_QUEUE_SIZE,
        },
        |mut context| async move {
            sync_both(&mut context).await;
            let TwoConsumerTestContext {
                tx: _tx,
                rx: _rx,
                stop,
                first_consumer,
                second_consumer,
            } = context;
            stop.trigger();
            (
                first_consumer.events.collect::<Vec<_>>().await,
                second_consumer.events.collect::<Vec<_>>().await,
            )
        },
    )
    .await;
    assert!(first_result.is_ok());
    assert!(second_result.is_ok());
    assert_eq!(first_events, vec![DownlinkNotification::Unlinked]);
    assert_eq!(second_events, vec![DownlinkNotification::Unlinked]);
}

#[tokio::test]
async fn receive_from_two_consumers() {
    let ((first_events, second_events), first_result, second_result) = run_test_with_two_consumers(
        DownlinkOptions::SYNC,
        DownlinkRuntimeConfig {
            empty_timeout: EMPTY_TIMEOUT,
            attachment_queue_size: ATT_QUEUE_SIZE,
        },
        |mut context| async move {
            sync_both(&mut context).await;

            let TwoConsumerTestContext {
                mut tx,
                mut rx,
                stop,
                first_consumer,
                second_consumer,
            } = context;

            let ConsumerContext {
                events: mut first_events,
                ..
            } = first_consumer;

            let ConsumerContext {
                events: mut second_events,
                ..
            } = second_consumer;

            tx.update_text(Text::new(FIRST_TAG)).await;

            expect_text_event(
                first_events.next().await,
                DownlinkNotification::Event {
                    body: Text::new(FIRST_TAG),
                },
            );
            expect_text_event(
                second_events.next().await,
                DownlinkNotification::Event {
                    body: Text::new(FIRST_TAG),
                },
            );

            expect_message(
                rx.recv().await,
                Operation::Command(format!("Response from {}.", FIRST_TAG).into()),
            );

            tx.update_text(Text::new(SECOND_TAG)).await;
            expect_text_event(
                first_events.next().await,
                DownlinkNotification::Event {
                    body: Text::new(SECOND_TAG),
                },
            );
            expect_text_event(
                second_events.next().await,
                DownlinkNotification::Event {
                    body: Text::new(SECOND_TAG),
                },
            );

            expect_message(
                rx.recv().await,
                Operation::Command(format!("Response from {}.", SECOND_TAG).into()),
            );

            stop.trigger();
            (
                first_events.collect::<Vec<_>>().await,
                second_events.collect::<Vec<_>>().await,
            )
        },
    )
    .await;
    assert!(first_result.is_ok());
    assert!(second_result.is_ok());
    assert_eq!(first_events, vec![DownlinkNotification::Unlinked]);
    assert_eq!(second_events, vec![DownlinkNotification::Unlinked]);
}
