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

use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::compat::{
    AgentMessageDecoder, MessageDecodeError, Operation, RequestMessage, ResponseMessage,
    ResponseMessageEncoder,
};
use crate::routing::RoutingAddr;

use super::{AttachAction, DownlinkOptions, ValueDownlinkManagementTask};
use futures::future::join3;
use futures::{SinkExt, StreamExt};
use swim_api::error::DownlinkTaskError;
use swim_api::protocol::{
    DownlinkNotifiationDecoder, DownlinkNotification, DownlinkOperation, DownlinkOperationEncoder,
};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::path::RelativePath;
use swim_model::Text;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::io::byte_channel::{self, ByteReader, ByteWriter};
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug, Form, PartialEq, Eq, Clone)]
enum Message {
    Ping,
    CurrentValue(Text),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Unlinked,
    Linked,
    Synced,
}

type Event = (State, DownlinkNotification<Message>);

struct TestContext {
    tx: TestSender,
    rx: TestReceiver,
    start_client: trigger::Sender,
    stop: trigger::Sender,
    events: UnboundedReceiverStream<Event>,
}

struct SyncedTestContext {
    tx: TestSender,
    rx: TestReceiver,
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
    let mut read = FramedRead::new(
        rx_in,
        DownlinkNotifiationDecoder::new(Message::make_recognizer()),
    );

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
                _ => {}
            },
        }
    }
    Ok(())
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);
const CHANNEL_SIZE: usize = 16;
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const REMOTE_ADDR: RoutingAddr = RoutingAddr::remote(1);
const REMOTE_NODE: &str = "/remote";
const REMOTE_LANE: &str = "remote_lane";
const EMPTY_TIMEOUT: Duration = Duration::from_secs(2);

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
        super::Config {
            empty_timeout: EMPTY_TIMEOUT,
        },
        test_block,
    )
    .await
}

async fn run_test_with_config<F, Fut>(
    options: DownlinkOptions,
    config: super::Config,
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

    let downlink = run_fake_downlink(attach_tx, options, start_rx, event_tx);

    let (in_tx, in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let path = RelativePath::new("/node", "lane");

    let management_task = ValueDownlinkManagementTask::new(
        attach_rx,
        in_rx,
        out_tx,
        stop_rx,
        RoutingAddr::client(1),
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
struct TestReceiver(FramedRead<ByteReader, MsgDecoder<Message>>);

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

    async fn send(&mut self, message: ResponseMessage<Message, &[u8]>) {
        assert!(self.0.send(message).await.is_ok());
    }

    async fn update(&mut self, message: Message) {
        let message = ResponseMessage::event(
            REMOTE_ADDR,
            RelativePath::new(REMOTE_NODE, REMOTE_LANE),
            message,
        );
        self.send(message).await;
    }
}

impl TestReceiver {
    fn new(reader: ByteReader) -> Self {
        TestReceiver(FramedRead::new(
            reader,
            MsgDecoder::new(Message::make_recognizer()),
        ))
    }

    async fn recv(&mut self) -> Option<Result<RequestMessage<Message>, MessageDecodeError>> {
        self.0.next().await
    }
}

fn expect_message(
    result: Option<Result<RequestMessage<Message>, MessageDecodeError>>,
    message: Operation<Message>,
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
    let events = f(SyncedTestContext { tx, rx, events }).await;
    stop.trigger();
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
        super::Config {
            empty_timeout: Duration::from_millis(100),
        },
        |TestContext {
             mut rx,
             stop,
             events,
             ..
         }| async move {
            expect_message(rx.recv().await, Operation::Link);
            (stop, events)
        },
    )
    .await;
    assert!(matches!(result, Err(DownlinkTaskError::FailedToStart)));
    assert!(events.collect::<Vec<_>>().await.is_empty());
}
