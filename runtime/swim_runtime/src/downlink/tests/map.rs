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

use futures::{
    future::{join3, select, Either},
    SinkExt, StreamExt,
};
use std::fmt::Debug;
use std::future::Future;
use swim_api::{
    error::{DownlinkTaskError, FrameIoError, InvalidFrame},
    protocol::{
        downlink::{DownlinkNotification, MapNotificationDecoder},
        map::{MapMessage, MapOperation, MapOperationEncoder},
    },
};
use swim_form::{structural::read::recognizer::RecognizerReadable, Form};
use swim_model::{path::RelativePath, Text};
use swim_utilities::{
    io::byte_channel::{self, ByteReader, ByteWriter},
    trigger::{self, promise},
};
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{self, UnboundedSender},
    time::timeout,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    compat::{
        AgentMessageDecoder, MessageDecodeError, Operation, RequestMessage, ResponseMessage,
        ResponseMessageEncoder,
    },
    downlink::{
        failure::{AlwaysAbortStrategy, BadFrameResponse, BadFrameStrategy},
        interpretation::{DownlinkInterpretation, MapInterpretation},
        AttachAction, DownlinkOptions, DownlinkRuntimeConfig, MapDownlinkRuntime,
    },
};

use super::*;

#[derive(Debug, PartialEq, Eq, Form, Clone, Copy)]
struct Record {
    a: i32,
    b: i32,
}

type Event = (State, DownlinkNotification<MapMessage<i32, Record>>);

const FAIL_KEY: i32 = -2;

async fn run_fake_downlink(
    sub: mpsc::Sender<AttachAction>,
    options: DownlinkOptions,
    start: trigger::Receiver,
    event_tx: mpsc::UnboundedSender<Event>,
    send_rx: mpsc::UnboundedReceiver<MapOperation<i32, Record>>,
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
    let mut read = FramedRead::new(rx_in, MapNotificationDecoder::<i32, Record>::default());

    let mut write = FramedWrite::new(tx_out, MapOperationEncoder);

    let mut send_rx = UnboundedReceiverStream::new(send_rx);

    let mut sender_active = true;

    loop {
        let next = if sender_active {
            match select(read.next(), send_rx.next()).await {
                Either::Left((l, _)) => Either::Left(l),
                Either::Right((r, _)) => Either::Right(r),
            }
        } else {
            Either::Left(read.next().await)
        };
        match next {
            Either::Left(Some(result)) => {
                let message = result?;
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
                        DownlinkNotification::Event {
                            body: MapMessage::Update { key, ..} | MapMessage::Remove { key},
                        } if key == FAIL_KEY => {
                            return Err(DownlinkTaskError::BadFrame(FrameIoError::BadFrame(
                                InvalidFrame::Incomplete,
                            )));
                        }
                        _ => {}
                    },
                }
            }
            Either::Right(Some(op)) => {
                if matches!(state, State::Synced) {
                    assert!(write.send(op).await.is_ok());
                } else {
                    panic!("Sending when not synced.");
                }
            }
            Either::Right(_) => {
                sender_active = false;
            }
            _ => {
                break;
            }
        }
    }

    Ok(())
}

struct CommandSender(UnboundedSender<MapOperation<i32, Record>>);

impl CommandSender {
    fn update(&mut self, key: i32, value: Record) {
        assert!(self.0.send(MapOperation::Update { key, value }).is_ok());
    }

    fn remove(&mut self, key: i32) {
        assert!(self.0.send(MapOperation::Remove { key }).is_ok());
    }

    fn clear(&mut self) {
        assert!(self.0.send(MapOperation::Clear).is_ok());
    }
}

struct TestContext {
    tx: TestSender,
    rx: TestReceiver<i32, Record>,
    start_client: trigger::Sender,
    stop: trigger::Sender,
    events: UnboundedReceiverStream<Event>,
    send_tx: CommandSender,
}

struct SyncedTestContext {
    tx: TestSender,
    rx: TestReceiver<i32, Record>,
    stop: trigger::Sender,
    events: UnboundedReceiverStream<Event>,
    send_tx: CommandSender,
}

#[derive(Debug)]
struct TestSender(FramedWrite<ByteWriter, ResponseMessageEncoder>);

type MsgDecoder<K, V> =
    AgentMessageDecoder<MapOperation<K, V>, <MapOperation<K, V> as RecognizerReadable>::Rec>;
struct TestReceiver<K: RecognizerReadable, V: RecognizerReadable>(
    FramedRead<ByteReader, MsgDecoder<K, V>>,
);

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

impl<K, V> TestReceiver<K, V>
where
    K: RecognizerReadable + Debug,
    V: RecognizerReadable + Debug,
{
    fn new(reader: ByteReader) -> Self {
        TestReceiver(FramedRead::new(
            reader,
            MsgDecoder::new(MapOperation::<K, V>::make_recognizer()),
        ))
    }

    async fn recv(
        &mut self,
    ) -> Option<Result<RequestMessage<MapOperation<K, V>>, MessageDecodeError>> {
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
        AlwaysAbortStrategy,
        test_block,
    )
    .await
}

async fn run_test_with_config<F, Fut, H>(
    options: DownlinkOptions,
    config: DownlinkRuntimeConfig,
    failure_strategy: H,
    test_block: F,
) -> (Fut::Output, Result<(), DownlinkTaskError>)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future + Send + 'static,
    H: BadFrameStrategy<<MapInterpretation as DownlinkInterpretation>::Error>,
{
    let (attach_tx, attach_rx) = mpsc::channel(CHANNEL_SIZE);
    let (start_tx, start_rx) = trigger::trigger();
    let (stop_tx, stop_rx) = trigger::trigger();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (send_tx, send_rx) = mpsc::unbounded_channel();

    let downlink = run_fake_downlink(attach_tx.clone(), options, start_rx, event_tx, send_rx);

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
        failure_strategy,
    )
    .run();

    let test_task = test_block(TestContext {
        tx: TestSender::new(in_tx),
        rx: TestReceiver::new(out_rx),
        start_client: start_tx,
        stop: stop_tx,
        events: UnboundedReceiverStream::new(event_rx),
        send_tx: CommandSender(send_tx),
    });

    let (_, task_res, result) = timeout(TEST_TIMEOUT, join3(management_task, test_task, downlink))
        .await
        .unwrap();
    (task_res, result)
}

fn expect_message<K: Eq + Debug, V: Eq + Debug>(
    result: Option<Result<RequestMessage<MapOperation<K, V>>, MessageDecodeError>>,
    message: Operation<MapOperation<K, V>>,
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

fn expect_event(
    result: Option<Event>,
    state: State,
    notification: DownlinkNotification<MapMessage<i32, Record>>,
) {
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

fn rec(a: i32, b: i32) -> Record {
    Record { a, b }
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

            tx.update(2, rec(1, 2)).await;
            tx.update(6, rec(3, 4)).await;
            tx.sync().await;

            expect_event(
                events.next().await,
                State::Linked,
                DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 2,
                        value: rec(1, 2),
                    },
                },
            );
            expect_event(
                events.next().await,
                State::Linked,
                DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 6,
                        value: rec(3, 4),
                    },
                },
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
        send_tx,
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

    tx.sync().await;

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
        send_tx,
    })
    .await;
    events.collect::<Vec<_>>().await
}

#[tokio::test]
async fn receive_commands() {
    let (events, result) = run_test(DownlinkOptions::SYNC, |context| {
        sync_client_then(context, |context| async move {
            let SyncedTestContext {
                tx: _tx,
                mut rx,
                stop,
                events,
                mut send_tx,
            } = context;
            send_tx.update(4, rec(7, 8));

            expect_message(
                rx.recv().await,
                Operation::Command(MapOperation::Update {
                    key: 4,
                    value: rec(7, 8),
                }),
            );

            send_tx.remove(4);

            expect_message(
                rx.recv().await,
                Operation::Command(MapOperation::Remove { key: 4 }),
            );

            send_tx.clear();

            expect_message(rx.recv().await, Operation::Command(MapOperation::Clear));

            stop.trigger();

            events
        })
    })
    .await;

    assert!(result.is_ok());
    assert_eq!(
        events,
        vec![(State::Synced, DownlinkNotification::Unlinked)]
    );
}

#[tokio::test]
async fn send_all_message_kinds() {
    let (events, result) = run_test(DownlinkOptions::SYNC, |context| {
        sync_client_then(context, |context| async move {
            let SyncedTestContext {
                mut tx,
                rx: _rx,
                stop,
                mut events,
                send_tx: _,
            } = context;

            tx.update(1, rec(0, 0)).await;
            tx.remove(1).await;
            tx.clear().await;
            tx.take(0).await;
            tx.drop(0).await;

            expect_event(
                events.next().await,
                State::Synced,
                DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 1,
                        value: rec(0, 0),
                    },
                },
            );

            expect_event(
                events.next().await,
                State::Synced,
                DownlinkNotification::Event {
                    body: MapMessage::Remove { key: 1 },
                },
            );

            expect_event(
                events.next().await,
                State::Synced,
                DownlinkNotification::Event {
                    body: MapMessage::Clear,
                },
            );

            expect_event(
                events.next().await,
                State::Synced,
                DownlinkNotification::Event {
                    body: MapMessage::Take(0),
                },
            );

            expect_event(
                events.next().await,
                State::Synced,
                DownlinkNotification::Event {
                    body: MapMessage::Drop(0),
                },
            );

            stop.trigger();

            events
        })
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
        AlwaysAbortStrategy,
        |TestContext {
             tx: _tx,
             mut rx,
             stop,
             events,
             start_client,
             send_tx: _,
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

struct TestStrategy(Option<promise::Sender<String>>);

impl TestStrategy {
    fn make() -> (Self, promise::Receiver<String>) {
        let (tx, rx) = promise::promise();
        (TestStrategy(Some(tx)), rx)
    }
}

impl<E: std::error::Error> BadFrameStrategy<E> for TestStrategy {
    type Report = E;

    fn failed_with(&mut self, error: E) -> BadFrameResponse<E> {
        let TestStrategy(inner) = self;
        if let Some(tx) = inner.take() {
            assert!(tx.provide(format!("{}", error)).is_ok());
        }
        BadFrameResponse::Abort(error)
    }
}

#[tokio::test]
async fn use_bad_message_strategy() {
    let (test_strategy, bad_message_rx) = TestStrategy::make();
    let ((_stop, events), result) = run_test_with_config(
        DownlinkOptions::SYNC,
        DownlinkRuntimeConfig {
            empty_timeout: EMPTY_TIMEOUT,
            attachment_queue_size: ATT_QUEUE_SIZE,
        },
        test_strategy,
        move |TestContext {
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

            tx.update_text(Text::new("invalid")).await;

            let result = bad_message_rx.await;
            assert!(result.is_ok());

            (stop, events.collect::<Vec<_>>().await)
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
async fn handle_failed_consumer() {
    let (events, result) = run_test(DownlinkOptions::SYNC, |context| {
        sync_client_then(context, |context| async move {
            let SyncedTestContext {
                mut tx,
                rx: _rx,
                stop: _stop, //No explicit stop, shutdown should ocurr becase there are no remaining consumers.
                mut events,
                send_tx: _,
            } = context;
            tx.update(-2, rec(1, 2)).await; //Cause the consumer to fail.
            expect_event(
                events.next().await,
                State::Synced,
                DownlinkNotification::Event {
                    body: MapMessage::Update { key: -2, value: rec(1, 2)},
                },
            );
            tx.clear().await; //Sending another messasge should cause the write task to notice the failure.
            events
        })
    })
    .await;

    assert!(matches!(result, Err(DownlinkTaskError::BadFrame(_))));
    assert!(events.is_empty());
}

const LIMIT: i32 = 150;
const KEY: i32 = 1;

#[tokio::test]
async fn exhaust_output_buffer() {
    let (events, result) = run_test(DownlinkOptions::SYNC, move |context| {
        sync_client_then(
            context,
            |context| async move {
                let SyncedTestContext {
                    tx: _tx,
                    mut rx,
                    stop,
                    events,
                    mut send_tx,
                } = context;
                for i in 0..(LIMIT + 1) {
                    send_tx.update(KEY, rec(i, i + 1));
                }
                let mut messages = vec![];
                loop {
                    let result = rx.recv().await;
                    match result {
                        Some(Ok(RequestMessage {
                            envelope: Operation::Command(MapOperation::Update { key, value}),
                            ..
                        })) => {
                            assert_eq!(key, KEY);
                            let fin = value.a == LIMIT;
                            messages.push(value);
                            if fin {
                                break;
                            }
                        }
                        ow => panic!("Unexpected result: {:?}", ow),
                    }
                }
                assert!((messages.len() as i32) < LIMIT);
                let mut prev = None;
                for message in messages.into_iter() {
                    let Record { a, .. } = message;
                    if let Some(i) = prev {
                        assert!(i < a);
                    }
                    prev = Some(a);
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
