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

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use swimos_api::{
    address::Address,
    agent::{AgentConfig, WarpLaneKind},
    error::{CommanderRegistrationError, DynamicRegistrationError, LaneSpawnError},
};
use swimos_model::Text;
use swimos_utilities::routing::RouteUri;

use crate::lanes::OpenLane;
use crate::{
    agent_lifecycle::HandlerContext,
    agent_model::downlink::BoxDownlinkChannelFactory,
    event_handler::{
        ActionContext, DownlinkSpawnOnDone, HandlerAction, HandlerFuture, LaneSpawnOnDone,
        LaneSpawner, LinkSpawner, Spawner, StepResult,
    },
    AgentMetadata,
};

pub struct TestAgent;

struct LaneReg {
    name: String,
    kind: WarpLaneKind,
    on_done: LaneSpawnOnDone<TestAgent>,
}

#[derive(Default)]
pub struct TestSpawner {
    inner: Arc<Mutex<Vec<LaneReg>>>,
}

impl Spawner<TestAgent> for TestSpawner {
    fn spawn_suspend(&self, _fut: HandlerFuture<TestAgent>) {
        panic!("Suspending futures not supported.");
    }

    fn schedule_timer(&self, _at: tokio::time::Instant, _id: u64) {
        panic!("Unexpected timer.");
    }
}

impl LinkSpawner<TestAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        _path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<TestAgent>,
        _on_done: DownlinkSpawnOnDone<TestAgent>,
    ) {
        panic!("Opening downlinks not supported.");
    }

    fn register_commander(&self, _path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        panic!("Registering commanders not supported.");
    }
}

impl LaneSpawner<TestAgent> for TestSpawner {
    fn spawn_warp_lane(
        &self,
        name: &str,
        kind: WarpLaneKind,
        on_done: LaneSpawnOnDone<TestAgent>,
    ) -> Result<(), DynamicRegistrationError> {
        let reg = LaneReg {
            name: name.to_string(),
            kind,
            on_done,
        };
        self.inner.lock().push(reg);
        Ok(())
    }
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

#[test]
fn open_lane() {
    use bytes::BytesMut;

    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();

    let flag = Arc::new(AtomicI64::new(0));
    let flag_copy = flag.clone();

    let handler = OpenLane::new(
        "lane".to_string(),
        WarpLaneKind::Value,
        move |result: Result<(), LaneSpawnError>| {
            let con: HandlerContext<TestAgent> = Default::default();
            con.effect(move || {
                if result.is_ok() {
                    flag.store(1, Ordering::SeqCst);
                } else {
                    flag.store(2, Ordering::SeqCst)
                }
            })
        },
    );

    let agent = TestAgent;
    let spawner = TestSpawner::default();
    let mut action_context = ActionContext::new(
        &spawner,
        &spawner,
        &spawner,
        &mut join_lane_init,
        &mut command_buffer,
    );

    run_handler(handler, &mut action_context, &agent, meta);

    let mut requests: Vec<_> = std::mem::take(spawner.inner.lock().as_mut());

    assert_eq!(requests.len(), 1);

    match requests.pop() {
        Some(LaneReg {
            name,
            kind,
            on_done,
        }) => {
            assert_eq!(name, "lane");
            assert_eq!(kind, WarpLaneKind::Value);
            let done_handler = on_done(Ok(1));
            run_handler(done_handler, &mut action_context, &agent, meta);

            assert_eq!(flag_copy.load(Ordering::SeqCst), 1);
        }
        _ => panic!("Expected exactly one request."),
    }
}

#[test]
fn open_lane_fail() {
    use bytes::BytesMut;

    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();

    let flag = Arc::new(AtomicI64::new(0));
    let flag_copy = flag.clone();

    let handler = OpenLane::new(
        "lane".to_string(),
        WarpLaneKind::Value,
        move |result: Result<(), LaneSpawnError>| {
            let con: HandlerContext<TestAgent> = Default::default();
            con.effect(move || {
                if result.is_ok() {
                    flag.store(1, Ordering::SeqCst);
                } else {
                    flag.store(2, Ordering::SeqCst)
                }
            })
        },
    );

    let agent = TestAgent;
    let spawner = TestSpawner::default();
    let mut action_context = ActionContext::new(
        &spawner,
        &spawner,
        &spawner,
        &mut join_lane_init,
        &mut command_buffer,
    );

    run_handler(handler, &mut action_context, &agent, meta);

    let mut requests: Vec<_> = std::mem::take(spawner.inner.lock().as_mut());

    assert_eq!(requests.len(), 1);

    match requests.pop() {
        Some(LaneReg {
            name,
            kind,
            on_done,
        }) => {
            assert_eq!(name, "lane");
            assert_eq!(kind, WarpLaneKind::Value);
            let done_handler = on_done(Err(LaneSpawnError::Registration(
                DynamicRegistrationError::AfterInitialization,
            )));
            run_handler(done_handler, &mut action_context, &agent, meta);

            assert_eq!(flag_copy.load(Ordering::SeqCst), 2);
        }
        _ => panic!("Expected exactly one request."),
    }
}

fn run_handler<H>(
    mut handler: H,
    action_context: &mut ActionContext<'_, TestAgent>,
    agent: &TestAgent,
    meta: AgentMetadata<'_>,
) -> H::Completion
where
    H: HandlerAction<TestAgent>,
{
    loop {
        match handler.step(action_context, meta, agent) {
            StepResult::Continue { modified_item } => {
                assert!(modified_item.is_none());
            }
            StepResult::Fail(err) => panic!("{}", err),
            StepResult::Complete {
                modified_item,
                result,
            } => {
                assert!(modified_item.is_none());
                break result;
            }
        }
    }
}
