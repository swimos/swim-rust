// Copyright 2015-2024 Swim Inc.
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

use bytes::Bytes;
use fake_context::LaneIo;
use fake_lifecycle::AddLane;
use futures::{
    future::{join, ready, BoxFuture},
    stream::BoxStream,
    Future, FutureExt, StreamExt,
};
use parking_lot::Mutex;
use std::{collections::HashMap, io::ErrorKind, sync::Arc, time::Duration};
use swimos_agent_protocol::{
    encoding::command::CommandMessageDecoder, CommandMessage, MapMessage, MapOperation,
};
use swimos_api::{
    address::Address,
    agent::{AgentConfig, AgentTask, DownlinkKind, HttpLaneRequest, WarpLaneKind},
    http::{HttpRequest, Method, StatusCode, Version},
};
use swimos_model::Text;
use swimos_utilities::{
    byte_channel::{ByteReader, ByteWriter},
    encoding::BytesStr,
    routing::RouteUri,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::{
    agent_model::{HostedDownlinkEvent, ItemFlags},
    event_handler::{HandlerActionExt, LocalBoxEventHandler, UnitHandler},
};

use self::{
    fake_agent::TestAgent,
    fake_context::TestAgentContext,
    fake_lifecycle::{LifecycleEvent, TestLifecycle},
    lane_io::{MapLaneReceiver, MapLaneSender, ValueLaneReceiver, ValueLaneSender},
};

use super::{
    downlink::{DownlinkChannel, DownlinkChannelError, DownlinkChannelEvent},
    AgentModel, HostedDownlink, ItemDescriptor, ItemModelFactory,
};

mod external_links;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestEvent {
    Value {
        body: i32,
    },
    DynValue {
        id: u64,
        body: i32,
    },
    Cmd {
        body: i32,
    },
    Map {
        body: MapMessage<i32, i32>,
    },
    DynMap {
        id: u64,
        body: MapMessage<i32, i32>,
    },
    Sync {
        id: Uuid,
    },
    LaneRegistration {
        id: u64,
        name: String,
        descriptor: ItemDescriptor,
    },
}

const VAL_ID: u64 = 0;
const MAP_ID: u64 = 1;
const CMD_ID: u64 = 2;
const HTTP_ID: u64 = 3;
const FIRST_DYN_ID: u64 = 4;

const VAL_LANE: &str = "first";
const MAP_LANE: &str = "second";
const CMD_LANE: &str = "third";
const HTTP_LANE: &str = "fourth";
const DYN_VAL_LANE: &str = "first_dynamic";
const DYN_MAP_LANE: &str = "second_dynamic";
const HTTP_LANE_URI: &str = "http://example/node?lane=fourth";
const TIMEOUT_ID: u64 = 674;

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

const SYNC_VALUE: i32 = -1;

const AD_HOC_HOST: &str = "localhost:8080";
const AD_HOC_NODE: &str = "/node";
const AD_HOC_LANE: &str = "lane";

const REMOTE_HOST: &str = "remote:8080";
const REMOTE_NODE: &str = "/node";
const REMOTE_LANE: &str = "lane";

//Values divisible by 3 cause the mock command lane to suspend a future.
const SUSPEND_VALUE: i32 = 456;
//Values congruent 1 mod 3 cause the mock command lane to send an ad ho command.
const AD_HOC_CMD_VALUE: i32 = 892;
//Values congruent to 2 mod 3 cause the mock command lane to schedule a timeout.
const TIMEOUT_EVENT: i32 = 1097;

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

type CommandReceiver = FramedRead<ByteReader, CommandMessageDecoder<BytesStr, Text>>;

struct TestContext {
    test_event_rx: UnboundedReceiverStream<TestEvent>,
    http_request_rx: UnboundedReceiverStream<HttpRequest<Bytes>>,
    lc_event_rx: UnboundedReceiverStream<LifecycleEvent>,
    val_lane_io: (ValueLaneSender, ValueLaneReceiver),
    map_lane_io: (MapLaneSender, MapLaneReceiver),
    cmd_lane_io: (ValueLaneSender, ValueLaneReceiver),
    dyn_val_lane_io: Option<(ValueLaneSender, ValueLaneReceiver)>,
    dyn_map_lane_io: Option<(MapLaneSender, MapLaneReceiver)>,
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

async fn init_agent_with_dyn_lanes(
    context: Box<TestAgentContext>,
    add_lanes: Vec<AddLane>,
) -> (AgentTask, TestContext) {
    let mut agent = TestAgent::default();
    let test_event_rx = agent.take_receiver();
    let http_req_rx = agent.take_http_receiver();
    let lane_model_fac = Fac::new(agent);

    let (lc_event_tx, lc_event_rx) = mpsc::unbounded_channel();
    let lifecycle = TestLifecycle::new(lc_event_tx, add_lanes);

    let model = AgentModel::<TestAgent, TestLifecycle>::new(lane_model_fac, lifecycle);

    let task = model
        .initialize_agent(make_uri(), HashMap::new(), CONFIG, context.clone())
        .await
        .expect("Initialization failed.");

    let LaneIo {
        value_lane,
        map_lane,
        cmd_lane,
        dyn_value_lane,
        dyn_map_lane,
    } = context.take_lane_io();
    let http_tx = context.take_http_io();

    let (val_tx, val_rx) = value_lane.expect("Value lane not registered.");
    let val_sender = ValueLaneSender::new(val_tx);
    let val_receiver = ValueLaneReceiver::new(val_rx);

    let (map_tx, map_rx) = map_lane.expect("Map lane not registered.");

    let map_sender = MapLaneSender::new(map_tx);
    let map_receiver = MapLaneReceiver::new(map_rx);

    let (cmd_tx, cmd_rx) = cmd_lane.expect("Command lane not registered.");
    let cmd_sender = ValueLaneSender::new(cmd_tx);
    let cmd_receiver = ValueLaneReceiver::new(cmd_rx);

    let dyn_val_lane_io =
        dyn_value_lane.map(|(tx, rx)| (ValueLaneSender::new(tx), ValueLaneReceiver::new(rx)));
    let dyn_map_lane_io =
        dyn_map_lane.map(|(tx, rx)| (MapLaneSender::new(tx), MapLaneReceiver::new(rx)));

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
            dyn_val_lane_io,
            dyn_map_lane_io,
            http_lane_tx: http_tx,
        },
    )
}

async fn init_agent(context: Box<TestAgentContext>) -> (AgentTask, TestContext) {
    init_agent_with_dyn_lanes(context, vec![]).await
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
async fn register_dynamic_value_lane_during_init() {
    with_timeout(async {
        let dyn_lanes = vec![AddLane::new(DYN_VAL_LANE, WarpLaneKind::Value)];
        let context = Box::<TestAgentContext>::default();
        let (
            _,
            TestContext {
                mut test_event_rx,
                lc_event_rx,
                dyn_val_lane_io,
                dyn_map_lane_io,
                ..
            },
        ) = init_agent_with_dyn_lanes(context, dyn_lanes).await;

        assert!(dyn_val_lane_io.is_some());
        assert!(dyn_map_lane_io.is_none());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        assert_eq!(
            events,
            vec![
                LifecycleEvent::Init,
                LifecycleEvent::Start,
                LifecycleEvent::dyn_lane(DYN_VAL_LANE, WarpLaneKind::Value, Ok(()))
            ]
        );

        if let Some(TestEvent::LaneRegistration {
            id,
            name,
            descriptor,
        }) = test_event_rx.next().await
        {
            assert_eq!(id, FIRST_DYN_ID);
            assert_eq!(name, DYN_VAL_LANE);
            assert_eq!(
                descriptor,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Value,
                    flags: ItemFlags::TRANSIENT
                }
            );
        } else {
            panic!("Expected lane registration.");
        }

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

#[tokio::test]
async fn register_dynamic_map_lane_during_init() {
    with_timeout(async {
        let dyn_lanes = vec![AddLane::new(DYN_MAP_LANE, WarpLaneKind::Map)];
        let context = Box::<TestAgentContext>::default();
        let (
            _,
            TestContext {
                mut test_event_rx,
                lc_event_rx,
                dyn_val_lane_io,
                dyn_map_lane_io,
                ..
            },
        ) = init_agent_with_dyn_lanes(context, dyn_lanes).await;

        assert!(dyn_val_lane_io.is_none());
        assert!(dyn_map_lane_io.is_some());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        assert_eq!(
            events,
            vec![
                LifecycleEvent::Init,
                LifecycleEvent::Start,
                LifecycleEvent::dyn_lane(DYN_MAP_LANE, WarpLaneKind::Map, Ok(()))
            ]
        );

        if let Some(TestEvent::LaneRegistration {
            id,
            name,
            descriptor,
        }) = test_event_rx.next().await
        {
            assert_eq!(id, FIRST_DYN_ID);
            assert_eq!(name, DYN_MAP_LANE);
            assert_eq!(
                descriptor,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Map,
                    flags: ItemFlags::TRANSIENT
                }
            );
        } else {
            panic!("Expected lane registration.");
        }

        let lane_events = test_event_rx.collect::<Vec<_>>().await;
        assert!(lane_events.is_empty());
    })
    .await
}

#[tokio::test]
async fn register_dynamic_lanes_during_init() {
    with_timeout(async {
        let dyn_lanes = vec![
            AddLane::new(DYN_VAL_LANE, WarpLaneKind::Value),
            AddLane::new(DYN_MAP_LANE, WarpLaneKind::Map),
        ];
        let context = Box::<TestAgentContext>::default();
        let (
            _,
            TestContext {
                mut test_event_rx,
                lc_event_rx,
                dyn_val_lane_io,
                dyn_map_lane_io,
                ..
            },
        ) = init_agent_with_dyn_lanes(context, dyn_lanes).await;

        assert!(dyn_val_lane_io.is_some());
        assert!(dyn_map_lane_io.is_some());

        let events = lc_event_rx.collect::<Vec<_>>().await;

        assert_eq!(
            events,
            vec![
                LifecycleEvent::Init,
                LifecycleEvent::Start,
                LifecycleEvent::dyn_lane(DYN_VAL_LANE, WarpLaneKind::Value, Ok(())),
                LifecycleEvent::dyn_lane(DYN_MAP_LANE, WarpLaneKind::Map, Ok(()))
            ]
        );

        if let Some(TestEvent::LaneRegistration {
            id,
            name,
            descriptor,
        }) = test_event_rx.next().await
        {
            assert_eq!(id, FIRST_DYN_ID);
            assert_eq!(name, DYN_VAL_LANE);
            assert_eq!(
                descriptor,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Value,
                    flags: ItemFlags::TRANSIENT
                }
            );
        } else {
            panic!("Expected lane registration.");
        }

        if let Some(TestEvent::LaneRegistration {
            id,
            name,
            descriptor,
        }) = test_event_rx.next().await
        {
            assert_eq!(id, FIRST_DYN_ID + 1);
            assert_eq!(name, DYN_MAP_LANE);
            assert_eq!(
                descriptor,
                ItemDescriptor::WarpLane {
                    kind: WarpLaneKind::Map,
                    flags: ItemFlags::TRANSIENT
                }
            );
        } else {
            panic!("Expected lane registration.");
        }

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
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        assert!(dyn_val_lane_io.is_none());
        assert!(dyn_map_lane_io.is_none());
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

            //Dropping all lane senders should cause the agent to stop.
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
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        assert!(dyn_val_lane_io.is_none());
        assert!(dyn_map_lane_io.is_none());

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
async fn command_to_dynamic_lane() {
    with_timeout(async {
        let context = Box::<TestAgentContext>::default();
        let dyn_lanes = vec![AddLane::new(DYN_VAL_LANE, WarpLaneKind::Value)];
        let (
            task,
            TestContext {
                mut test_event_rx,
                http_request_rx: _http_request_rx,
                mut lc_event_rx,
                val_lane_io,
                map_lane_io,
                cmd_lane_io,
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent_with_dyn_lanes(context, dyn_lanes).await;

        let dyn_value_lane = dyn_val_lane_io.expect("Expected lane to be registered.");
        assert!(dyn_map_lane_io.is_none());

        let test_case = async move {
            assert_eq!(
                lc_event_rx.next().await.expect("Expected init event."),
                LifecycleEvent::Init
            );
            assert_eq!(
                lc_event_rx.next().await.expect("Expected start event."),
                LifecycleEvent::Start
            );
            assert_eq!(
                lc_event_rx
                    .next()
                    .await
                    .expect("Expected lane registration."),
                LifecycleEvent::dyn_lane(DYN_VAL_LANE, WarpLaneKind::Value, Ok(()))
            );

            if let Some(TestEvent::LaneRegistration {
                id,
                name,
                descriptor,
            }) = test_event_rx.next().await
            {
                assert_eq!(id, FIRST_DYN_ID);
                assert_eq!(name, DYN_VAL_LANE);
                assert_eq!(
                    descriptor,
                    ItemDescriptor::WarpLane {
                        kind: WarpLaneKind::Value,
                        flags: ItemFlags::TRANSIENT
                    }
                );
            } else {
                panic!("Expected lane registration.");
            }

            let (mut sender, mut receiver) = dyn_value_lane;

            sender.command(56).await;

            // The agent should receive the command...
            assert_eq!(
                test_event_rx.next().await.expect("Expected command event."),
                TestEvent::DynValue {
                    id: FIRST_DYN_ID,
                    body: 56
                }
            );

            //... ,trigger the `on_command` event...
            assert_eq!(
                lc_event_rx.next().await.expect("Expected command event."),
                LifecycleEvent::Lane(Text::new(DYN_VAL_LANE))
            );

            //... and then generate an outgoing event.
            receiver.expect_event(56).await;

            drop(sender);
            drop(val_lane_io);
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
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        assert!(dyn_val_lane_io.is_none());
        assert!(dyn_map_lane_io.is_none());

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
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        assert!(dyn_val_lane_io.is_none());
        assert!(dyn_map_lane_io.is_none());

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
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        assert!(dyn_val_lane_io.is_none());
        assert!(dyn_map_lane_io.is_none());

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
async fn command_to_dynamic_map_lane() {
    with_timeout(async {
        let context = Box::<TestAgentContext>::default();
        let dyn_lanes = vec![AddLane::new(DYN_MAP_LANE, WarpLaneKind::Map)];
        let (
            task,
            TestContext {
                mut test_event_rx,
                http_request_rx: _http_request_rx,
                mut lc_event_rx,
                val_lane_io,
                map_lane_io,
                cmd_lane_io,
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent_with_dyn_lanes(context, dyn_lanes).await;

        assert!(dyn_val_lane_io.is_none());
        let dyn_map_lane = dyn_map_lane_io.expect("Expected lane to be registered.");

        let test_case = async move {
            assert_eq!(
                lc_event_rx.next().await.expect("Expected init event."),
                LifecycleEvent::Init
            );
            assert_eq!(
                lc_event_rx.next().await.expect("Expected start event."),
                LifecycleEvent::Start
            );
            assert_eq!(
                lc_event_rx
                    .next()
                    .await
                    .expect("Expected lane registration."),
                LifecycleEvent::dyn_lane(DYN_MAP_LANE, WarpLaneKind::Map, Ok(()))
            );

            if let Some(TestEvent::LaneRegistration {
                id,
                name,
                descriptor,
            }) = test_event_rx.next().await
            {
                assert_eq!(id, FIRST_DYN_ID);
                assert_eq!(name, DYN_MAP_LANE);
                assert_eq!(
                    descriptor,
                    ItemDescriptor::WarpLane {
                        kind: WarpLaneKind::Map,
                        flags: ItemFlags::TRANSIENT
                    }
                );
            } else {
                panic!("Expected lane registration.");
            }

            let (mut sender, mut receiver) = dyn_map_lane;

            sender.command(83, 9282).await;

            // The agent should receive the command...
            assert_eq!(
                test_event_rx.next().await.expect("Expected command event."),
                TestEvent::DynMap {
                    id: FIRST_DYN_ID,
                    body: MapMessage::Update {
                        key: 83,
                        value: 9282
                    }
                }
            );

            //... ,trigger the `on_command` event...
            assert_eq!(
                lc_event_rx.next().await.expect("Expected command event."),
                LifecycleEvent::Lane(Text::new(DYN_MAP_LANE))
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
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        assert!(dyn_val_lane_io.is_none());
        assert!(dyn_map_lane_io.is_none());

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
    let (cmd_tx, cmd_rx) = oneshot::channel();
    let context = Box::new(TestAgentContext::new(cmd_tx));
    let (
        task,
        TestContext {
            mut test_event_rx,
            http_request_rx: _http_request_rx,
            mut lc_event_rx,
            val_lane_io,
            map_lane_io,
            cmd_lane_io,
            dyn_val_lane_io,
            dyn_map_lane_io,
            http_lane_tx,
        },
    ) = init_agent(context).await;

    assert!(dyn_val_lane_io.is_none());
    assert!(dyn_map_lane_io.is_none());

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
            cmd_rx
                .await
                .expect("Ad hoc command channel not registered."),
            CommandMessageDecoder::default(),
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

        let expected = CommandMessage::ad_hoc(
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

#[tokio::test(start_paused = true)]
async fn schedule_timeout() {
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
                dyn_val_lane_io,
                dyn_map_lane_io,
                http_lane_tx,
            },
        ) = init_agent(context).await;

        assert!(dyn_val_lane_io.is_none());
        assert!(dyn_map_lane_io.is_none());

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

            let n = TIMEOUT_EVENT;

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

            // The runtime should auto-advance to trigger the timeout.
            assert_eq!(
                lc_event_rx.next().await.expect("Expected timeout event."),
                LifecycleEvent::Timeout(TIMEOUT_ID)
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

type WriteStream = BoxStream<'static, Result<(), std::io::Error>>;

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

    fn next_event(
        &mut self,
        _context: &TestDlAgent,
    ) -> Option<LocalBoxEventHandler<'_, TestDlAgent>> {
        if self.ready {
            self.ready = false;
            Some(UnitHandler::default().boxed_local())
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

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
