use std::sync::Arc;

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

use futures::{future::join, StreamExt};
use parking_lot::Mutex;
use swim_api::{
    agent::{AgentConfig, AgentTask},
    protocol::map::{MapMessage, MapOperation},
};
use swim_model::Text;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use self::{
    fake_agent::TestAgent,
    fake_context::TestAgentContext,
    fake_lifecycle::{LifecycleEvent, TestLifecycle},
    lane_io::{MapLaneReceiver, MapLaneSender, ValueLaneReceiver, ValueLaneSender},
};

use super::{AgentModel, LaneModelFactory};

mod fake_agent;
mod fake_context;
mod fake_lifecycle;
mod lane_io;
mod run_handler;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestEvent {
    Value { body: i32 },
    Map { body: MapMessage<i32, i32> },
    Sync { id: Uuid },
}

const VAL_ID: u64 = 0;
const MAP_ID: u64 = 1;

const VAL_LANE: &str = "first";
const MAP_LANE: &str = "second";

const CONFIG: AgentConfig = AgentConfig {};
const NODE_URI: &str = "/node";

const SYNC_VALUE: i32 = -1;

fn make_uri() -> RelativeUri {
    RelativeUri::try_from(NODE_URI).expect("Bad URI.")
}

struct TestContext {
    test_event_rx: UnboundedReceiverStream<TestEvent>,
    lc_event_rx: UnboundedReceiverStream<LifecycleEvent>,
    val_lane_io: (ValueLaneSender, ValueLaneReceiver),
    map_lane_io: (MapLaneSender, MapLaneReceiver),
}

#[derive(Clone)]
pub struct Fac {
    rx: Arc<Mutex<Option<TestAgent>>>,
}

impl LaneModelFactory for Fac {
    type LaneModel = TestAgent;

    fn create(&self) -> Self::LaneModel {
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
    let lane_model_fac = Fac::new(agent);

    let (lc_event_tx, lc_event_rx) = mpsc::unbounded_channel();
    let lifecycle = TestLifecycle::new(lc_event_tx);

    let model = AgentModel::<TestAgent, TestLifecycle>::new(lane_model_fac, lifecycle);

    let task = model
        .initialize_agent(make_uri(), CONFIG, context.clone())
        .await
        .expect("Initialization failed.");

    let (val_lane_io, map_lane_io) = context.take_lane_io();

    let (val_tx, val_rx) = val_lane_io.expect("Value lane not registered.");
    let val_sender = ValueLaneSender::new(val_tx);
    let val_receiver = ValueLaneReceiver::new(val_rx);

    let (map_tx, map_rx) = map_lane_io.expect("Map lane not registered.");

    let map_sender = MapLaneSender::new(map_tx);
    let map_receiver = MapLaneReceiver::new(map_rx);
    (
        task,
        TestContext {
            test_event_rx: UnboundedReceiverStream::new(test_event_rx),
            lc_event_rx: UnboundedReceiverStream::new(lc_event_rx),
            val_lane_io: (val_sender, val_receiver),
            map_lane_io: (map_sender, map_receiver),
        },
    )
}

#[tokio::test]
async fn run_agent_init_task() {
    let context = Box::new(TestAgentContext::default());
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

    assert!(matches!(events.as_slice(), [LifecycleEvent::Start]));

    let lane_events = test_event_rx.collect::<Vec<_>>().await;
    assert!(lane_events.is_empty());
}

#[tokio::test]
async fn stops_if_all_lanes_stop() {
    let context = Box::new(TestAgentContext::default());
    let (
        task,
        TestContext {
            test_event_rx,
            mut lc_event_rx,
            val_lane_io,
            map_lane_io,
        },
    ) = init_agent(context).await;

    let test_case = async move {
        assert_eq!(
            lc_event_rx.next().await.expect("Expected start event."),
            LifecycleEvent::Start
        );

        let (vtx, vrx) = val_lane_io;
        let (mtx, mrx) = map_lane_io;

        //Dropping both lane senders should cause the agent to stop.
        drop(vtx);
        drop(mtx);

        (lc_event_rx, vrx, mrx)
    };

    let (result, (lc_event_rx, _vrx, _mrx)) = join(task, test_case).await;
    assert!(result.is_ok());

    let events = lc_event_rx.collect::<Vec<_>>().await;

    //Check that the `on_stop` event fired.
    assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

    let lane_events = test_event_rx.collect::<Vec<_>>().await;
    assert!(lane_events.is_empty());
}

#[tokio::test]
async fn command_to_value_lane() {
    let context = Box::new(TestAgentContext::default());
    let (
        task,
        TestContext {
            mut test_event_rx,
            mut lc_event_rx,
            val_lane_io,
            map_lane_io,
        },
    ) = init_agent(context).await;

    let test_case = async move {
        assert_eq!(
            lc_event_rx.next().await.expect("Expected start event."),
            LifecycleEvent::Start
        );
        let (mut sender, mut receiver) = val_lane_io;

        sender.command(56).await;

        // The agent should receive the command...
        assert_eq!(
            test_event_rx.next().await.expect("Expected command event."),
            TestEvent::Value { body: 56 }
        );

        //... ,trigger the `on_command` event...
        assert_eq!(
            lc_event_rx.next().await.expect("Expected command event."),
            LifecycleEvent::Lane(Text::new(VAL_LANE))
        );

        //... and then generate an outgoing event.
        receiver.expect_event(56).await;

        drop(sender);
        drop(map_lane_io);
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

const SYNC_ID: Uuid = Uuid::from_u128(393883);

#[tokio::test]
async fn sync_with_lane() {
    let context = Box::new(TestAgentContext::default());
    let (
        task,
        TestContext {
            mut test_event_rx,
            mut lc_event_rx,
            val_lane_io,
            map_lane_io,
        },
    ) = init_agent(context).await;

    let test_case = async move {
        assert_eq!(
            lc_event_rx.next().await.expect("Expected start event."),
            LifecycleEvent::Start
        );
        let (mut sender, mut receiver) = val_lane_io;

        sender.sync(SYNC_ID).await;

        // The agent should receive the sync request..
        assert_eq!(
            test_event_rx.next().await.expect("Expected sync event."),
            TestEvent::Sync { id: SYNC_ID }
        );

        // ... and send out the response.
        receiver.expect_sync_event(SYNC_ID, SYNC_VALUE).await;

        drop(sender);
        drop(map_lane_io);
        (test_event_rx, lc_event_rx)
    };

    let (result, (test_event_rx, lc_event_rx)) = join(task, test_case).await;
    assert!(result.is_ok());

    let events = lc_event_rx.collect::<Vec<_>>().await;

    println!("{:?}", events);
    //Check that the `on_stop` event fired.
    assert!(matches!(events.as_slice(), [LifecycleEvent::Stop]));

    let lane_events = test_event_rx.collect::<Vec<_>>().await;
    assert!(lane_events.is_empty());
}

#[tokio::test]
async fn command_to_map_lane() {
    let context = Box::new(TestAgentContext::default());
    let (
        task,
        TestContext {
            mut test_event_rx,
            mut lc_event_rx,
            val_lane_io,
            map_lane_io,
        },
    ) = init_agent(context).await;

    let test_case = async move {
        assert_eq!(
            lc_event_rx.next().await.expect("Expected start event."),
            LifecycleEvent::Start
        );
        let (mut sender, mut receiver) = map_lane_io;

        sender.command(83, 9282).await;

        // The agent should receive the command...
        assert_eq!(
            test_event_rx.next().await.expect("Expected command event."),
            TestEvent::Map {
                body: MapMessage::Update {
                    key: 83,
                    value: 9282
                }
            }
        );

        //... ,triger the `on_command` event...
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
