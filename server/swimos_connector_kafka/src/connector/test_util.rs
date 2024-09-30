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
    cell::RefCell,
    collections::{HashMap, HashSet},
};

use bytes::BytesMut;
use futures::{stream::FuturesUnordered, StreamExt};
use swimos_agent::{
    agent_model::{
        downlink::BoxDownlinkChannelFactory, AgentSpec, ItemDescriptor, ItemFlags, WarpLaneKind,
    },
    event_handler::{
        ActionContext, DownlinkSpawnOnDone, EventHandler, HandlerFuture, LaneSpawnOnDone,
        LaneSpawner, LinkSpawner, Spawner, StepResult,
    },
    AgentMetadata,
};
use swimos_api::{
    address::Address,
    agent::AgentConfig,
    error::{CommanderRegistrationError, DynamicRegistrationError},
};
use swimos_connector::ConnectorAgent;
use swimos_model::Text;
use swimos_utilities::routing::RouteUri;

pub struct LaneRequest {
    name: String,
    is_map: bool,
    on_done: LaneSpawnOnDone<ConnectorAgent>,
}

impl std::fmt::Debug for LaneRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneRequest")
            .field("name", &self.name)
            .field("is_map", &self.is_map)
            .field("on_done", &"...")
            .finish()
    }
}

#[derive(Default, Debug)]
pub struct TestSpawner {
    allow_downlinks: bool,
    downlinks: RefCell<Vec<DownlinkRequest>>,
    suspended: FuturesUnordered<HandlerFuture<ConnectorAgent>>,
    lane_requests: RefCell<Vec<LaneRequest>>,
}

impl TestSpawner {
    pub fn with_downlinks() -> TestSpawner {
        TestSpawner {
            allow_downlinks: true,
            ..Default::default()
        }
    }

    pub fn take_downlinks(&self) -> Vec<DownlinkRequest> {
        let mut guard = self.downlinks.borrow_mut();
        std::mem::take(&mut *guard)
    }
}

impl Spawner<ConnectorAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<ConnectorAgent>) {
        self.suspended.push(fut);
    }

    fn schedule_timer(&self, _at: tokio::time::Instant, _id: u64) {
        panic!("Unexpected timer");
    }
}

pub struct DownlinkRequest {
    pub path: Address<String>,
    _make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
    _on_done: DownlinkSpawnOnDone<ConnectorAgent>,
}

impl std::fmt::Debug for DownlinkRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownlinkRequest")
            .field("path", &self.path)
            .field("make_channel", &"...")
            .field("_on_done", &"...")
            .finish()
    }
}

impl LinkSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
        _on_done: DownlinkSpawnOnDone<ConnectorAgent>,
    ) {
        if self.allow_downlinks {
            self.downlinks.borrow_mut().push(DownlinkRequest {
                path: path.into(),
                _make_channel,
                _on_done,
            })
        } else {
            panic!("Opening downlinks not supported.");
        }
    }

    fn register_commander(&self, _path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        panic!("Registering commanders not supported.");
    }
}

impl LaneSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_warp_lane(
        &self,
        name: &str,
        kind: WarpLaneKind,
        on_done: LaneSpawnOnDone<ConnectorAgent>,
    ) -> Result<(), DynamicRegistrationError> {
        let is_map = match kind {
            WarpLaneKind::Map => true,
            WarpLaneKind::Value => false,
            _ => panic!("Unexpected lane kind: {}", kind),
        };
        self.lane_requests.borrow_mut().push(LaneRequest {
            name: name.to_string(),
            is_map,
            on_done,
        });
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

pub async fn run_handler_with_futures_dl<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    handler: H,
) -> (HashSet<u64>, Vec<DownlinkRequest>) {
    let mut spawner = TestSpawner::with_downlinks();
    let modified = run_handler_with_futures_inner(agent, handler, &mut spawner).await;
    (modified, spawner.take_downlinks())
}

pub async fn run_handler_with_futures<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    handler: H,
) -> HashSet<u64> {
    run_handler_with_futures_inner(agent, handler, &mut TestSpawner::default()).await
}

async fn run_handler_with_futures_inner<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    handler: H,
    spawner: &mut TestSpawner,
) -> HashSet<u64> {
    let mut modified = run_handler(agent, spawner, handler);
    let mut handlers = vec![];
    let reg = move |req: LaneRequest| {
        let LaneRequest {
            name,
            is_map,
            on_done,
        } = req;
        let kind = if is_map {
            WarpLaneKind::Map
        } else {
            WarpLaneKind::Value
        };
        let descriptor = ItemDescriptor::WarpLane {
            kind,
            flags: ItemFlags::TRANSIENT,
        };
        let result = agent.register_dynamic_item(&name, descriptor);
        on_done(result.map_err(Into::into))
    };
    for request in std::mem::take::<Vec<LaneRequest>>(spawner.lane_requests.borrow_mut().as_mut()) {
        handlers.push(reg(request));
    }

    while !(handlers.is_empty() && spawner.suspended.is_empty()) {
        let m = if let Some(h) = handlers.pop() {
            run_handler(agent, spawner, h)
        } else {
            let h = spawner.suspended.next().await.expect("No handler.");
            run_handler(agent, spawner, h)
        };
        modified.extend(m);
        for request in
            std::mem::take::<Vec<LaneRequest>>(spawner.lane_requests.borrow_mut().as_mut())
        {
            handlers.push(reg(request));
        }
    }
    modified
}

pub fn run_handler<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    spawner: &TestSpawner,
    mut handler: H,
) -> HashSet<u64> {
    let route_params = HashMap::new();
    let uri = make_uri();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();

    let mut action_context = ActionContext::new(
        spawner,
        spawner,
        spawner,
        &mut join_lane_init,
        &mut command_buffer,
    );

    let mut modified = HashSet::new();

    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { modified_item } => {
                if let Some(m) = modified_item {
                    modified.insert(m.id());
                }
            }
            StepResult::Fail(err) => panic!("Handler Failed: {}", err),
            StepResult::Complete { modified_item, .. } => {
                if let Some(m) = modified_item {
                    modified.insert(m.id());
                }
                break modified;
            }
        }
    }
}
