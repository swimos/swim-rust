use std::{cell::RefCell, collections::HashMap, io::ErrorKind, sync::Arc, time::Duration};

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

use bytes::{Bytes, BytesMut};
use futures::{
    future::{join, ready, BoxFuture},
    Future, FutureExt, SinkExt, StreamExt,
};
use parking_lot::Mutex;
use swim_api::{
    agent::{AgentConfig, AgentContext, AgentTask, HttpLaneRequest},
    downlink::DownlinkKind,
    error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError},
    lane::WarpLaneKind,
    protocol::{
        agent::{AdHocCommand, AdHocCommandDecoder},
        downlink::{DownlinkNotification, DownlinkNotificationEncoder},
        map::{MapMessage, MapOperation},
        WithLenRecognizerDecoder, WithLengthBytesCodec,
    },
};
use swim_form::structural::read::recognizer::{primitive::TextRecognizer, RecognizerReadable};
use swim_model::{
    address::Address,
    http::{HttpRequest, Method, StatusCode, Version},
    BytesStr, Text,
};
use swim_utilities::{
    future::retryable::RetryStrategy,
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    non_zero_usize,
    routing::route_uri::RouteUri,
    sync::circular_buffer,
    trigger::trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::{
    agent_model::{downlink::hosted::HostedValueDownlinkFactory, HostedDownlinkEvent},
    config::SimpleDownlinkConfig,
    downlink_lifecycle::{
        on_failed::OnFailed,
        on_linked::OnLinked,
        on_synced::OnSynced,
        on_unlinked::OnUnlinked,
        value::{on_event::OnDownlinkEvent, on_set::OnDownlinkSet},
    },
    event_handler::{
        BoxEventHandler, HandlerActionExt, SideEffect, StepResult, UnitHandler, WriteStream,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

use self::{
    fake_agent::TestAgent,
    fake_context::TestAgentContext,
    fake_lifecycle::{LifecycleEvent, TestLifecycle},
    lane_io::{MapLaneReceiver, MapLaneSender, ValueLaneReceiver, ValueLaneSender},
};

use super::{
    downlink::handlers::{DownlinkChannel, DownlinkChannelError, DownlinkChannelEvent},
    AgentModel, HostedDownlink, ItemModelFactory,
};

mod fake_agent;
mod fake_context;
mod fake_lifecycle;
mod lane_io;
mod run_handler;

const TIMEOUT: Duration = Duration::from_secs(5);

async fn with_timeout<F>(f: F) -> F::Output
where
    F: Future,
{
    tokio::time::timeout(TIMEOUT, f)
        .await
        .expect("Test timed out.")
}

#[derive(Debug, Clone)]
pub enum TestEvent {
    Value { body: i32 },
    Cmd { body: i32 },
    Map { body: MapMessage<i32, i32> },
    Sync { id: Uuid },
}

const VAL_ID: u64 = 0;
const MAP_ID: u64 = 1;
const CMD_ID: u64 = 2;
const HTTP_ID: u64 = 3;

const VAL_LANE: &str = "first";
const MAP_LANE: &str = "second";
const CMD_LANE: &str = "third";
const HTTP_LANE: &str = "fourth";
const HTTP_LANE_URI: &str = "http://example/node?lane=fourth";

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

const SYNC_VALUE: i32 = -1;

const AD_HOC_HOST: &str = "localhost:8080";
const AD_HOC_NODE: &str = "/node";
const AD_HOC_LANE: &str = "lane";

const REMOTE_HOST: &str = "remote:8080";
const REMOTE_NODE: &str = "/node";
const REMOTE_LANE: &str = "lane";

//Event values cause the mock command lane to suspend a future.
const SUSPEND_VALUE: i32 = 456;
//Odd values cause the mock command lane to send an ad ho command.
const AD_HOC_CMD_VALUE: i32 = 891;

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

type CommandReceiver =
    FramedRead<ByteReader, AdHocCommandDecoder<BytesStr, WithLenRecognizerDecoder<TextRecognizer>>>;

struct TestContext {
    test_event_rx: UnboundedReceiverStream<TestEvent>,
    http_request_rx: UnboundedReceiverStream<HttpRequest<Bytes>>,
    lc_event_rx: UnboundedReceiverStream<LifecycleEvent>,
    val_lane_io: (ValueLaneSender, ValueLaneReceiver),
    map_lane_io: (MapLaneSender, MapLaneReceiver),
    cmd_lane_io: (ValueLaneSender, ValueLaneReceiver),
    http_lane_tx: mpsc::Sender<HttpLaneRequest>,
}

#[derive(Clone)]
pub struct Fac {
    rx: Arc<Mutex<Option<TestAgent>>>,
}

impl ItemModelFactory for Fac {
    type ItemModel = TestAgent;

    fn create(&self) -> Self::ItemModel {
        let mut guard = self.rx.lock();
        guard.take().expect("Agent created twice.")
    }
}

impl Fac {
    fn new(agent: TestAgent) -> Self {
        Fac {
            rx: Arc::new(Mutex::new(Some(agent))),
        }
    }
}

async fn init_agent(context: Box<TestAgentContext>) -> (AgentTask, TestContext) {
    let mut agent = TestAgent::default();
    let test_event_rx = agent.take_receiver();
    let http_req_rx = agent.take_http_receiver();
    let lane_model_fac = Fac::new(agent);

    let (lc_event_tx, lc_event_rx) = mpsc::unbounded_channel();
    let lifecycle = TestLifecycle::new(lc_event_tx);

    let model = AgentModel::<TestAgent, TestLifecycle>::new(lane_model_fac, lifecycle);

    let task = model
        .initialize_agent(make_uri(), HashMap::new(), CONFIG, context.clone())
        .await
        .expect("Initialization failed.");

    let (val_lane_io, map_lane_io, cmd_lane_io) = context.take_lane_io();
    let http_tx = context.take_http_io();

    let (val_tx, val_rx) = val_lane_io.expect("Value lane not registered.");
    let val_sender = ValueLaneSender::new(val_tx);
    let val_receiver = ValueLaneReceiver::new(val_rx);

    let (map_tx, map_rx) = map_lane_io.expect("Map lane not registered.");

    let map_sender = MapLaneSender::new(map_tx);
    let map_receiver = MapLaneReceiver::new(map_rx);

    let (cmd_tx, cmd_rx) = cmd_lane_io.expect("Command lane not registered.");
    let cmd_sender = ValueLaneSender::new(cmd_tx);
    let cmd_receiver = ValueLaneReceiver::new(cmd_rx);

    let http_tx = http_tx.expect("HTTP lane not registered.");
    (
        task,
        TestContext {
            test_event_rx: UnboundedReceiverStream::new(test_event_rx),
            http_request_rx: UnboundedReceiverStream::new(http_req_rx),
            lc_event_rx: UnboundedReceiverStream::new(lc_event_rx),
            val_lane_io: (val_sender, val_receiver),
            map_lane_io: (map_sender, map_receiver),
            cmd_lane_io: (cmd_sender, cmd_receiver),
            http_lane_tx: http_tx,
        },
    )
}

#[tokio::test]
async fn run_agent_init_task() {
    with_timeout(async {
        let context = Box::<TestAgentContext>::default();
        let (
            _,
            TestContext {
                test_event_rx,
                lc_event_rx,
                ..
            },
        ) = init_agent(context).await;

        //We expect the `on_start` event to have fired and the two lanes to have been attached.

        let events = lc_event_rx.collect::<Vec<_>>().await;

        assert!(matches!(
            events.as_slice(),
            [LifecycleEvent::Init, LifecycleEvent::Start]
        ));

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

#[tokio::test]
async fn stops_if_all_lanes_stop() {
    with_timeout(async move {
        let context = Box::<TestAgentContext>::default();
        let (
            task,
            TestContext {
                test_event_rx,
                http_request_rx: _http_request_rx,
                mut lc_event_rx,
                val_lane_io,
                map_lane_io,
                cmd_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        let test_case = async move {
            assert_eq!(
                lc_event_rx.next().await.expect("Expected init event."),
                LifecycleEvent::Init
            );
            assert_eq!(
                lc_event_rx.next().await.expect("Expected start event."),
                LifecycleEvent::Start
            );

            let (vtx, vrx) = val_lane_io;
            let (mtx, mrx) = map_lane_io;
            let (ctx, crx) = cmd_lane_io;

            //Dropping both lane senders should cause the agent to stop.
            drop(vtx);
            drop(mtx);
            drop(ctx);
            drop(http_lane_tx);

            (lc_event_rx, vrx, mrx, crx)
        };

        let (result, (lc_event_rx, _vrx, _mrx, _crx)) = join(task, test_case).await;
        assert!(result.is_ok());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        //Check that the `on_stop` event fired.
        assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

#[tokio::test]
async fn command_to_value_lane() {
    with_timeout(async {
        let context = Box::<TestAgentContext>::default();
        let (
            task,
            TestContext {
                mut test_event_rx,
                http_request_rx: _http_request_rx,
                mut lc_event_rx,
                val_lane_io,
                map_lane_io,
                cmd_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        let test_case = async move {
            assert_eq!(
                lc_event_rx.next().await.expect("Expected init event."),
                LifecycleEvent::Init
            );
            assert_eq!(
                lc_event_rx.next().await.expect("Expected start event."),
                LifecycleEvent::Start
            );
            let (mut sender, mut receiver) = val_lane_io;

            sender.command(56).await;

            // The agent should receive the command...
            assert!(matches!(
                test_event_rx.next().await.expect("Expected command event."),
                TestEvent::Value { body: 56 }
            ));

            //... ,trigger the `on_command` event...
            assert_eq!(
                lc_event_rx.next().await.expect("Expected command event."),
                LifecycleEvent::Lane(Text::new(VAL_LANE))
            );

            //... and then generate an outgoing event.
            receiver.expect_event(56).await;

            drop(sender);
            drop(map_lane_io);
            drop(cmd_lane_io);
            drop(http_lane_tx);
            (test_event_rx, lc_event_rx)
        };

        let (result, (test_event_rx, lc_event_rx)) = join(task, test_case).await;
        assert!(result.is_ok());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        //Check that the `on_stop` event fired.
        assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

#[tokio::test]
async fn request_to_http_lane() {
    with_timeout(async move {
        let context = Box::<TestAgentContext>::default();
        let (
            task,
            TestContext {
                test_event_rx,
                mut http_request_rx,
                mut lc_event_rx,
                val_lane_io,
                map_lane_io,
                cmd_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        let test_case = async move {
            assert_eq!(
                lc_event_rx.next().await.expect("Expected init event."),
                LifecycleEvent::Init
            );
            assert_eq!(
                lc_event_rx.next().await.expect("Expected start event."),
                LifecycleEvent::Start
            );

            let req = HttpRequest {
                method: Method::GET,
                version: Version::HTTP_1_1,
                uri: HTTP_LANE_URI.parse().unwrap(),
                headers: vec![],
                payload: Bytes::new(),
            };
            let (lane_request, response_rx) = HttpLaneRequest::new(req.clone());

            http_lane_tx
                .send(lane_request)
                .await
                .expect("HTTP lane stopped.");

            // The agent should receive the request...
            assert_eq!(
                http_request_rx
                    .next()
                    .await
                    .expect("Expected HTTP request."),
                req
            );

            //... ,trigger the lane event...
            assert_eq!(
                lc_event_rx.next().await.expect("Expected HTTP lane event."),
                LifecycleEvent::Lane(Text::new(HTTP_LANE))
            );

            //... and then satisfy the request.
            let response = response_rx.await.expect("Request not satisfied.");
            assert_eq!(response.status_code, StatusCode::OK);

            drop(val_lane_io);
            drop(map_lane_io);
            drop(cmd_lane_io);
            drop(http_lane_tx);
            (test_event_rx, lc_event_rx)
        };

        let (result, (test_event_rx, lc_event_rx)) =
            tokio::time::timeout(TEST_TIMEOUT, join(task, test_case))
                .await
                .expect("Timed out");
        assert!(result.is_ok());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        //Check that the `on_stop` event fired.
        assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

const SYNC_ID: Uuid = Uuid::from_u128(393883);

#[tokio::test]
async fn sync_with_lane() {
    with_timeout(async {
        let context = Box::<TestAgentContext>::default();
        let (
            task,
            TestContext {
                mut test_event_rx,
                http_request_rx: _http_request_rx,
                mut lc_event_rx,
                val_lane_io,
                map_lane_io,
                cmd_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        let test_case = async move {
            assert_eq!(
                lc_event_rx.next().await.expect("Expected init event."),
                LifecycleEvent::Init
            );
            assert_eq!(
                lc_event_rx.next().await.expect("Expected start event."),
                LifecycleEvent::Start
            );
            let (mut sender, mut receiver) = val_lane_io;

            sender.sync(SYNC_ID).await;

            // The agent should receive the sync request..
            assert!(matches!(
                test_event_rx.next().await.expect("Expected sync event."),
                TestEvent::Sync { id: SYNC_ID }
            ));

            // ... and send out the response.
            receiver.expect_sync_event(SYNC_ID, SYNC_VALUE).await;

            drop(sender);
            drop(map_lane_io);
            drop(cmd_lane_io);
            drop(http_lane_tx);
            (test_event_rx, lc_event_rx)
        };

        let (result, (test_event_rx, lc_event_rx)) = join(task, test_case).await;
        assert!(result.is_ok());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        //Check that the `on_stop` event fired.
        assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

#[tokio::test]
async fn command_to_map_lane() {
    with_timeout(async {
        let context = Box::<TestAgentContext>::default();
        let (
            task,
            TestContext {
                mut test_event_rx,
                http_request_rx: _http_request_rx,
                mut lc_event_rx,
                val_lane_io,
                map_lane_io,
                cmd_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        let test_case = async move {
            assert_eq!(
                lc_event_rx.next().await.expect("Expected init event."),
                LifecycleEvent::Init
            );
            assert_eq!(
                lc_event_rx.next().await.expect("Expected start event."),
                LifecycleEvent::Start
            );
            let (mut sender, mut receiver) = map_lane_io;

            sender.command(83, 9282).await;

            // The agent should receive the command...
            assert!(matches!(
                test_event_rx.next().await.expect("Expected command event."),
                TestEvent::Map {
                    body: MapMessage::Update {
                        key: 83,
                        value: 9282
                    }
                }
            ));

            //... ,trigger the `on_command` event...
            assert_eq!(
                lc_event_rx.next().await.expect("Expected command event."),
                LifecycleEvent::Lane(Text::new(MAP_LANE))
            );

            //... and then generate an outgoing event.
            receiver
                .expect_event(MapOperation::Update {
                    key: 83,
                    value: 9282,
                })
                .await;

            drop(sender);
            drop(val_lane_io);
            drop(cmd_lane_io);
            drop(http_lane_tx);
            (test_event_rx, lc_event_rx)
        };

        let (result, (test_event_rx, lc_event_rx)) = join(task, test_case).await;
        assert!(result.is_ok());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        //Check that the `on_stop` event fired.
        assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

#[tokio::test]
async fn suspend_future() {
    with_timeout(async {
        let context = Box::<TestAgentContext>::default();
        let (
            task,
            TestContext {
                mut test_event_rx,
                http_request_rx: _http_request_rx,
                mut lc_event_rx,
                val_lane_io,
                map_lane_io,
                cmd_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        let test_case = async move {
            assert_eq!(
                lc_event_rx.next().await.expect("Expected init event."),
                LifecycleEvent::Init
            );
            assert_eq!(
                lc_event_rx.next().await.expect("Expected start event."),
                LifecycleEvent::Start
            );
            let (mut sender, receiver) = cmd_lane_io;

            let n = SUSPEND_VALUE;

            sender.command(n).await;

            // The agent should receive the command...
            assert!(matches!(
                test_event_rx.next().await.expect("Expected command event."),
                TestEvent::Cmd { body } if body == n
            ));

            //... ,trigger the `on_command` event...
            assert_eq!(
                lc_event_rx.next().await.expect("Expected command event."),
                LifecycleEvent::Lane(Text::new(CMD_LANE))
            );

            //... and then run the suspended future.
            assert_eq!(
                lc_event_rx
                    .next()
                    .await
                    .expect("Expected suspended future."),
                LifecycleEvent::RanSuspended(n)
            );

            //... and then run the consequence.
            assert_eq!(
                lc_event_rx
                    .next()
                    .await
                    .expect("Expected suspended future consequence."),
                LifecycleEvent::RanSuspendedConsequence
            );

            drop(sender);
            drop(receiver);
            drop(val_lane_io);
            drop(map_lane_io);
            drop(http_lane_tx);
            (test_event_rx, lc_event_rx)
        };

        let (result, (test_event_rx, lc_event_rx)) = join(task, test_case).await;
        assert!(result.is_ok());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        //Check that the `on_stop` event fired.
        assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

#[tokio::test]
async fn trigger_ad_hoc_command() {
    let (ad_hoc_tx, ad_hoc_rx) = oneshot::channel();
    let context = Box::new(TestAgentContext::new(ad_hoc_tx));
    let (
        task,
        TestContext {
            mut test_event_rx,
            http_request_rx: _http_request_rx,
            mut lc_event_rx,
            val_lane_io,
            map_lane_io,
            cmd_lane_io,
            http_lane_tx,
        },
    ) = init_agent(context).await;

    let test_case = async move {
        assert_eq!(
            lc_event_rx.next().await.expect("Expected init event."),
            LifecycleEvent::Init
        );
        assert_eq!(
            lc_event_rx.next().await.expect("Expected start event."),
            LifecycleEvent::Start
        );
        let (mut sender, receiver) = cmd_lane_io;

        let mut cmd_receiver = CommandReceiver::new(
            ad_hoc_rx
                .await
                .expect("Ad hoc command channel not registered."),
            AdHocCommandDecoder::new(WithLenRecognizerDecoder::new(Text::make_recognizer())),
        );

        let n = AD_HOC_CMD_VALUE;

        sender.command(n).await;

        // The agent should receive the command...
        assert!(matches!(
            test_event_rx.next().await.expect("Expected command event."),
            TestEvent::Cmd { body } if body == n
        ));

        //... ,trigger the `on_command` event...
        assert_eq!(
            lc_event_rx.next().await.expect("Expected command event."),
            LifecycleEvent::Lane(Text::new(CMD_LANE))
        );

        //... and then issue an outgoing command.

        let expected = AdHocCommand::new(
            Address::new(
                Some(BytesStr::from_static_str(AD_HOC_HOST)),
                BytesStr::from_static_str(AD_HOC_NODE),
                BytesStr::from_static_str(AD_HOC_LANE),
            ),
            Text::new("content"),
            true,
        );

        let received = cmd_receiver
            .next()
            .await
            .expect("Command sender dropped.")
            .expect("Command channel failed");
        assert_eq!(received, expected);

        drop(sender);
        drop(receiver);
        drop(val_lane_io);
        drop(map_lane_io);
        drop(http_lane_tx);
        (test_event_rx, lc_event_rx)
    };

    let (result, (test_event_rx, lc_event_rx)) = join(task, test_case).await;
    assert!(result.is_ok());

    let events = lc_event_rx.collect::<Vec<_>>().await;

    //Check that the `on_stop` event fired.
    assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

    let lane_events = test_event_rx.collect::<Vec<_>>().await;
    assert!(lane_events.is_empty());
}

struct TestDownlinkChannel {
    address: Address<Text>,
    rx: mpsc::UnboundedReceiver<Result<DownlinkChannelEvent, DownlinkChannelError>>,
    ready: bool,
    writes: Option<WriteStream>,
}

struct TestDlAgent;

impl DownlinkChannel<TestDlAgent> for TestDownlinkChannel {
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Event
    }

    fn address(&self) -> &Address<Text> {
        &self.address
    }

    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>> {
        let TestDownlinkChannel {
            rx, ready, writes, ..
        } = self;
        async move {
            if let Some(w) = writes.as_mut() {
                tokio::select! {
                    biased;
                    result = rx.recv() => {
                        *ready = true;
                        result
                    },
                    write_result = w.next() => {
                        match write_result {
                            Some(Ok(_)) => Some(Ok(DownlinkChannelEvent::WriteCompleted)),
                            Some(Err(err)) => Some(Err(DownlinkChannelError::WriteFailed(err))),
                            _ => {
                                *writes = None;
                                Some(Ok(DownlinkChannelEvent::WriteStreamTerminated))
                            }
                        }
                    }
                }
            } else {
                let result = rx.recv().await;
                *ready = true;
                result
            }
        }
        .boxed()
    }

    fn next_event(&mut self, _context: &TestDlAgent) -> Option<BoxEventHandler<'_, TestDlAgent>> {
        if self.ready {
            self.ready = false;
            Some(UnitHandler::default().boxed())
        } else {
            None
        }
    }

    fn connect(&mut self, _context: &TestDlAgent, _output: ByteWriter, _input: ByteReader) {
        panic!("Unexpected reconnection.");
    }

    fn can_restart(&self) -> bool {
        false
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), std::io::Error>> {
        ready(Ok(())).boxed()
    }
}

fn make_dl_out(rx: mpsc::UnboundedReceiver<Result<(), std::io::Error>>) -> WriteStream {
    UnboundedReceiverStream::new(rx).boxed()
}

fn make_test_hosted_downlink(
    in_rx: mpsc::UnboundedReceiver<Result<DownlinkChannelEvent, DownlinkChannelError>>,
    out_rx: mpsc::UnboundedReceiver<Result<(), std::io::Error>>,
) -> HostedDownlink<TestDlAgent> {
    let address = Address::text(Some(REMOTE_HOST), REMOTE_NODE, REMOTE_LANE);
    let channel = TestDownlinkChannel {
        address,
        rx: in_rx,
        ready: false,
        writes: Some(make_dl_out(out_rx)),
    };

    HostedDownlink::new(Box::new(channel))
}

#[tokio::test]
async fn hosted_downlink_incoming() {
    with_timeout(async {
        let agent = TestDlAgent;

        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (_out_tx, out_rx) = mpsc::unbounded_channel();
        let hosted = make_test_hosted_downlink(in_rx, out_rx);

        assert!(in_tx.send(Ok(DownlinkChannelEvent::HandlerReady)).is_ok());
        let (mut hosted, event) = hosted.wait_on_downlink().await;
        assert!(matches!(
            event,
            HostedDownlinkEvent::HandlerReady { failed: false }
        ));
        assert!(hosted.next_event(&agent).is_some());
    })
    .await;
}

#[tokio::test]
async fn hosted_downlink_incoming_error() {
    with_timeout(async {
        let agent = TestDlAgent;

        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (_out_tx, out_rx) = mpsc::unbounded_channel();
        let hosted = make_test_hosted_downlink(in_rx, out_rx);

        assert!(in_tx.send(Err(DownlinkChannelError::ReadFailed)).is_ok());
        let (mut hosted, event) = hosted.wait_on_downlink().await;

        assert!(matches!(
            event,
            HostedDownlinkEvent::HandlerReady { failed: true }
        ));

        assert!(hosted.next_event(&agent).is_some());

        assert!(matches!(
            hosted.wait_on_downlink().await,
            (_, HostedDownlinkEvent::Stopped)
        ));
    })
    .await;
}

#[tokio::test]
async fn hosted_downlink_outgoing() {
    with_timeout(async {
        let agent = TestDlAgent;

        let (_in_tx, in_rx) = mpsc::unbounded_channel();
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let hosted = make_test_hosted_downlink(in_rx, out_rx);

        assert!(out_tx.send(Ok(())).is_ok());
        let (mut hosted, event) = hosted.wait_on_downlink().await;
        assert!(matches!(event, HostedDownlinkEvent::Written));
        assert!(hosted.next_event(&agent).is_none());
    })
    .await;
}

#[tokio::test]
async fn hosted_downlink_write_terminated() {
    with_timeout(async {
        let agent = TestDlAgent;

        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let hosted = make_test_hosted_downlink(in_rx, out_rx);

        drop(out_tx);
        let (mut hosted, event) = hosted.wait_on_downlink().await;
        assert!(matches!(event, HostedDownlinkEvent::WriterTerminated));
        assert!(hosted.next_event(&agent).is_none());

        assert!(in_tx.send(Ok(DownlinkChannelEvent::HandlerReady)).is_ok());
        let (mut hosted, event) = hosted.wait_on_downlink().await;
        assert!(matches!(
            event,
            HostedDownlinkEvent::HandlerReady { failed: false }
        ));
        assert!(hosted.next_event(&agent).is_some());
    })
    .await;
}

#[tokio::test]
async fn hosted_downlink_write_failed() {
    with_timeout(async {
        let agent = TestDlAgent;

        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let hosted = make_test_hosted_downlink(in_rx, out_rx);

        let err = std::io::Error::from(ErrorKind::BrokenPipe);

        assert!(out_tx.send(Err(err)).is_ok());
        let (mut hosted, event) = hosted.wait_on_downlink().await;
        match event {
            HostedDownlinkEvent::WriterFailed(err) => {
                assert_eq!(err.kind(), ErrorKind::BrokenPipe);
            }
            ow => {
                panic!("Unexpected event: {:?}", ow);
            }
        }
        assert!(hosted.next_event(&agent).is_none());

        assert!(in_tx.send(Ok(DownlinkChannelEvent::HandlerReady)).is_ok());
        let (mut hosted, event) = hosted.wait_on_downlink().await;
        assert!(matches!(
            event,
            HostedDownlinkEvent::HandlerReady { failed: false }
        ));
        assert!(hosted.next_event(&agent).is_some());
    })
    .await;
}

#[tokio::test]
async fn hosted_downlink_incoming_terminated() {
    with_timeout(async {
        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (_out_tx, out_rx) = mpsc::unbounded_channel();
        let hosted = make_test_hosted_downlink(in_rx, out_rx);

        drop(in_tx);
        assert!(matches!(
            hosted.wait_on_downlink().await,
            (_, HostedDownlinkEvent::Stopped)
        ));
    })
    .await;
}

#[tokio::test]
async fn hosted_downlink_in_out_interleaved() {
    let agent = TestDlAgent;

    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let hosted = make_test_hosted_downlink(in_rx, out_rx);

    assert!(in_tx.send(Ok(DownlinkChannelEvent::HandlerReady)).is_ok());
    let (mut hosted, event) = hosted.wait_on_downlink().await;
    assert!(matches!(
        event,
        HostedDownlinkEvent::HandlerReady { failed: false }
    ));
    assert!(hosted.next_event(&agent).is_some());

    assert!(out_tx.send(Ok(())).is_ok());
    let (mut hosted, event) = hosted.wait_on_downlink().await;
    assert!(matches!(event, HostedDownlinkEvent::Written));
    assert!(hosted.next_event(&agent).is_none());

    assert!(in_tx.send(Ok(DownlinkChannelEvent::HandlerReady)).is_ok());
    let (mut hosted, event) = hosted.wait_on_downlink().await;
    assert!(matches!(
        event,
        HostedDownlinkEvent::HandlerReady { failed: false }
    ));
    assert!(hosted.next_event(&agent).is_some());
}

#[derive(Debug, PartialEq, Eq, Default)]
enum DlState {
    Linked,
    Synced,
    #[default]
    Unlinked,
}

#[derive(PartialEq, Eq, Default)]
struct Inner {
    dl_state: DlState,
    value: Option<i32>,
}

#[derive(Default, Clone)]
struct TestState(Arc<Mutex<Inner>>);

impl TestState {
    fn check(&self, state: DlState, v: Option<i32>) {
        let guard = self.0.lock();
        assert_eq!(guard.dl_state, state);
        assert_eq!(guard.value, v);
    }
}

struct FakeAgent;

impl OnLinked<FakeAgent> for TestState {
    type OnLinkedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        SideEffect::from(move || {
            let mut guard = self.0.lock();
            guard.dl_state = DlState::Linked;
        })
        .boxed()
    }
}

impl OnUnlinked<FakeAgent> for TestState {
    type OnUnlinkedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        SideEffect::from(move || {
            let mut guard = self.0.lock();
            guard.dl_state = DlState::Unlinked;
        })
        .boxed()
    }
}

impl OnFailed<FakeAgent> for TestState {
    type OnFailedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        panic!("Downlink failed.");
    }
}

impl OnSynced<i32, FakeAgent> for TestState {
    type OnSyncedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &i32) -> Self::OnSyncedHandler<'a> {
        let n = *value;
        SideEffect::from(move || {
            let mut guard = self.0.lock();
            guard.value = Some(n);
            guard.dl_state = DlState::Synced;
        })
        .boxed()
    }
}

impl OnDownlinkEvent<i32, FakeAgent> for TestState {
    type OnEventHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_event(&self, value: &i32) -> Self::OnEventHandler<'_> {
        let n = *value;
        SideEffect::from(move || {
            let mut guard = self.0.lock();
            guard.value = Some(n);
        })
        .boxed()
    }
}

impl OnDownlinkSet<i32, FakeAgent> for TestState {
    type OnSetHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn on_set<'a>(&'a self, _previous: Option<i32>, _new_value: &i32) -> Self::OnSetHandler<'a> {
        UnitHandler::default()
    }
}

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn run_value_downlink() {
    let test_case = async {
        let (in_tx, in_rx) = byte_channel(non_zero_usize!(1024));
        let (out_tx, out_rx) = byte_channel(non_zero_usize!(1024));
        let (_stop_tx, stop_rx) = trigger();

        let (mut write_tx, write_rx) = circular_buffer::channel::<i32>(non_zero_usize!(8));

        let config = SimpleDownlinkConfig {
            events_when_not_synced: false,
            terminate_on_unlinked: true,
        };
        let state: RefCell<Option<i32>> = Default::default();

        let lc = TestState::default();
        let fac = HostedValueDownlinkFactory::<i32, _, _>::new(
            Address::text(None, "/node", "lane"),
            lc.clone(),
            state,
            config,
            stop_rx,
            write_rx,
        );
        let agent = FakeAgent;
        let dl: HostedDownlink<FakeAgent> = HostedDownlink::new(fac.create(&agent, out_tx, in_rx));

        let mut in_writer = FramedWrite::new(in_tx, DownlinkNotificationEncoder);
        let mut out_reader = FramedRead::new(out_rx, WithLengthBytesCodec);

        in_writer
            .send(DownlinkNotification::<&[u8]>::Linked)
            .await
            .unwrap();
        in_writer
            .send(DownlinkNotification::Event { body: b"1" })
            .await
            .unwrap();
        in_writer
            .send(DownlinkNotification::<&[u8]>::Synced)
            .await
            .unwrap();
        in_writer
            .send(DownlinkNotification::Event { body: b"2" })
            .await
            .unwrap();

        let dl = expect_event(dl, true).await;
        lc.check(DlState::Linked, None);
        let dl = expect_event(dl, false).await;
        let dl = expect_event(dl, true).await;
        lc.check(DlState::Synced, Some(1));
        let dl = expect_event(dl, true).await;
        lc.check(DlState::Synced, Some(2));

        write_tx.try_send(3).unwrap();
        let (mut dl, event) = dl.wait_on_downlink().await;
        assert!(matches!(event, HostedDownlinkEvent::Written));
        dl.flush().await.expect("Flushing output failed.");
        let value = out_reader.next().await.unwrap().unwrap();
        assert_eq!(value.as_ref(), b"3");

        in_writer
            .send(DownlinkNotification::Event { body: b"3" })
            .await
            .unwrap();
        expect_event(dl, true).await;
        lc.check(DlState::Synced, Some(3));
    };
    tokio::time::timeout(TEST_TIMEOUT, test_case)
        .await
        .expect("Test timed out;")
}

struct DlTestContext {
    expected_address: Address<Text>,
    expected_kind: DownlinkKind,
    io: Mutex<Option<(ByteWriter, ByteReader)>>,
}

impl DlTestContext {
    fn new(
        expected_address: Address<Text>,
        expected_kind: DownlinkKind,
        io: (ByteWriter, ByteReader),
    ) -> Self {
        DlTestContext {
            expected_address,
            expected_kind,
            io: Mutex::new(Some(io)),
        }
    }
}

impl AgentContext for DlTestContext {
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: swim_api::agent::LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn open_downlink(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        let DlTestContext {
            expected_address,
            expected_kind,
            io,
        } = self;
        let addr = Address::new(host, node, lane);
        assert_eq!(addr, expected_address.borrow_parts());
        assert_eq!(kind, *expected_kind);
        let io = io.lock().take().expect("Called twice.");
        ready(Ok(io)).boxed()
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: swim_api::store::StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Unexpected call.");
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<swim_api::agent::HttpLaneRequestChannel, AgentRuntimeError>>
    {
        panic!("Unexpected call.");
    }
}

#[tokio::test]
async fn reconnect_value_downlink() {
    let test_case = async {
        let (in_tx, in_rx) = byte_channel(non_zero_usize!(1024));
        let (out_tx, out_rx) = byte_channel(non_zero_usize!(1024));
        let (_stop_tx, stop_rx) = trigger();

        let (mut write_tx, write_rx) = circular_buffer::channel::<i32>(non_zero_usize!(8));

        let config = SimpleDownlinkConfig {
            events_when_not_synced: false,
            terminate_on_unlinked: false,
        };
        let state: RefCell<Option<i32>> = Default::default();

        let lc = TestState::default();
        let addr = Address::text(None, "/node", "lane");
        let fac = HostedValueDownlinkFactory::<i32, _, _>::new(
            addr.clone(),
            lc.clone(),
            state,
            config,
            stop_rx,
            write_rx,
        );
        let agent = FakeAgent;
        let dl: HostedDownlink<FakeAgent> = HostedDownlink::new(fac.create(&agent, out_tx, in_rx));

        let mut in_writer = FramedWrite::new(in_tx, DownlinkNotificationEncoder);

        in_writer
            .send(DownlinkNotification::<&[u8]>::Linked)
            .await
            .unwrap();

        let dl = expect_event(dl, true).await;
        lc.check(DlState::Linked, None);

        //Cause the downlink to stop.
        drop(in_writer);

        let dl = expect_event(dl, true).await;
        lc.check(DlState::Unlinked, None);
        let (dl, event) = dl.wait_on_downlink().await;
        assert!(matches!(event, HostedDownlinkEvent::Stopped));

        drop(out_rx);

        let (in_tx2, in_rx2) = byte_channel(non_zero_usize!(1024));
        let (out_tx2, out_rx2) = byte_channel(non_zero_usize!(1024));

        let con = DlTestContext::new(addr.clone(), DownlinkKind::Value, (out_tx2, in_rx2));

        let (mut dl, event) = dl.reconnect(&con, RetryStrategy::none(), true).await;

        match event {
            HostedDownlinkEvent::ReconnectSucceeded(rec) => {
                rec.connect(&mut dl, &FakeAgent);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }

        let mut in_writer = FramedWrite::new(in_tx2, DownlinkNotificationEncoder);
        let mut out_reader = FramedRead::new(out_rx2, WithLengthBytesCodec);

        in_writer
            .send(DownlinkNotification::<&[u8]>::Linked)
            .await
            .unwrap();

        let dl = expect_event(dl, true).await;
        lc.check(DlState::Linked, None);

        write_tx.try_send(3).unwrap();
        let (mut dl, event) = dl.wait_on_downlink().await;
        assert!(matches!(event, HostedDownlinkEvent::Written));
        dl.flush().await.expect("Flushing output failed.");
        let value = out_reader.next().await.unwrap().unwrap();
        assert_eq!(value.as_ref(), b"3");
    };
    tokio::time::timeout(TEST_TIMEOUT, test_case)
        .await
        .expect("Test timed out;")
}

async fn expect_event(
    dl: HostedDownlink<FakeAgent>,
    expect_handler: bool,
) -> HostedDownlink<FakeAgent> {
    let (mut dl, event) = dl.wait_on_downlink().await;
    assert!(matches!(
        event,
        HostedDownlinkEvent::HandlerReady { failed: false }
    ));
    {
        let maybe_handler = dl.next_event(&FakeAgent);
        if expect_handler {
            let handler = maybe_handler.expect("Expected handler.");
            run_handler(handler);
        } else {
            assert!(maybe_handler.is_none());
        }
    }
    dl
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

fn run_handler(mut event_handler: BoxEventHandler<'_, FakeAgent>) {
    let uri = make_uri();
    let params = HashMap::new();
    let meta = make_meta(&uri, &params);
    let agent = FakeAgent;
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();
    loop {
        match event_handler.step(
            &mut dummy_context(&mut join_lane_init, &mut ad_hoc_buffer),
            meta,
            &agent,
        ) {
            StepResult::Continue { modified_item } => {
                assert!(modified_item.is_none());
            }
            StepResult::Fail(err) => {
                panic!("Event handler failed: {}", err);
            }
            StepResult::Complete { modified_item, .. } => {
                assert!(modified_item.is_none());
                break;
            }
        }
    }
}
