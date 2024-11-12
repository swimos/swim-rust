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

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use bytes::BytesMut;
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use swimos_api::{
    address::Address,
    agent::{AgentConfig, WarpLaneKind},
    error::{CommanderRegistrationError, DynamicRegistrationError},
};
use swimos_model::Text;
use swimos_utilities::routing::RouteUri;
use tokio::time::Instant;

use crate::{
    agent_model::{AgentDescription, DownlinkSpawnRequest},
    config::{MapDownlinkConfig, SimpleDownlinkConfig},
    downlink_lifecycle::{StatefulMapDownlinkLifecycle, StatefulValueDownlinkLifecycle},
    event_handler::{
        ActionContext, BoxJoinLaneInit, DownlinkSpawnOnDone, HandlerAction, HandlerFuture,
        LaneSpawnOnDone, LaneSpawner, LinkSpawner, Spawner, StepResult,
    },
    meta::AgentMetadata,
};

use super::{BoxDownlinkChannelFactory, OpenMapDownlinkAction, OpenValueDownlinkAction};

struct TestAgent;

impl AgentDescription for TestAgent {
    fn item_name(&self, _id: u64) -> Option<Cow<'_, str>> {
        None
    }
}

#[derive(Default)]
struct SpawnerInner {
    downlink: Option<DownlinkSpawnRequest<TestAgent>>,
}

#[derive(Default)]
struct TestSpawner {
    futures: FuturesUnordered<HandlerFuture<TestAgent>>,
    inner: Arc<Mutex<SpawnerInner>>,
}

impl Spawner<TestAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<TestAgent>) {
        self.futures.push(fut);
    }

    fn schedule_timer(&self, _at: Instant, _id: u64) {
        panic!("Unexpected timer.");
    }
}

impl LinkSpawner<TestAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        make_channel: BoxDownlinkChannelFactory<TestAgent>,
        on_done: DownlinkSpawnOnDone<TestAgent>,
    ) {
        let mut guard = self.inner.lock();
        assert!(guard.downlink.is_none());
        guard.downlink = Some(DownlinkSpawnRequest {
            path,
            kind: make_channel.kind(),
            make_channel,
            on_done,
        });
    }

    fn register_commander(&self, _path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        panic!("Registering commanders not supported.");
    }
}

impl<Context> LaneSpawner<Context> for TestSpawner {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<Context>,
    ) -> Result<(), DynamicRegistrationError> {
        panic!("Spawning dynamic lanes not supported.");
    }
}

const HOST: &str = "localhost";
const NODE: &str = "/node";
const LANE: &str = "lane";

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

async fn run_all_and_check(
    mut spawner: TestSpawner,
    meta: AgentMetadata<'_>,
    join_lane_init: &mut HashMap<u64, BoxJoinLaneInit<'static, TestAgent>>,
    agent: &TestAgent,
) {
    let mut command_buffer = BytesMut::new();
    while let Some(handler) = spawner.futures.next().await {
        let mut action_context = ActionContext::new(
            &spawner,
            &spawner,
            &spawner,
            join_lane_init,
            &mut command_buffer,
        );
        run_handler(handler, &mut action_context, agent, meta);
    }
    assert!(join_lane_init.is_empty());
    assert!(command_buffer.is_empty());
    spawner
        .inner
        .lock()
        .downlink
        .take()
        .expect("Downlink was not registered.");
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

#[tokio::test]
async fn open_value_downlink() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();
    let lifecycle = StatefulValueDownlinkLifecycle::<TestAgent, _, i32>::new(());

    let handler = OpenValueDownlinkAction::<i32, _>::new(
        Address::text(Some(HOST), NODE, LANE),
        lifecycle,
        SimpleDownlinkConfig::default(),
    );

    let spawner = TestSpawner::default();

    let agent = TestAgent;
    let mut action_context = ActionContext::new(
        &spawner,
        &spawner,
        &spawner,
        &mut join_lane_init,
        &mut command_buffer,
    );
    let _handle = run_handler(handler, &mut action_context, &agent, meta);

    run_all_and_check(spawner, meta, &mut join_lane_init, &agent).await;
}

#[tokio::test]
async fn open_map_downlink() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();
    let lifecycle = StatefulMapDownlinkLifecycle::<TestAgent, _, i32, Text>::new(());

    let handler = OpenMapDownlinkAction::<i32, Text, _>::new(
        Address::text(Some(HOST), NODE, LANE),
        lifecycle,
        MapDownlinkConfig::default(),
    );

    let spawner = TestSpawner::default();

    let agent = TestAgent;
    let mut action_context = ActionContext::new(
        &spawner,
        &spawner,
        &spawner,
        &mut join_lane_init,
        &mut command_buffer,
    );
    let _handle = run_handler(handler, &mut action_context, &agent, meta);

    run_all_and_check(spawner, meta, &mut join_lane_init, &agent).await;
}
