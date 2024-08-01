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
    time::Duration,
};

use bytes::BytesMut;
use futures::{
    future::{join, BoxFuture},
    stream::FuturesUnordered,
    StreamExt,
};
use swimos_agent::{
    agent_model::{
        downlink::BoxDownlinkChannel, AgentSpec, ItemDescriptor, ItemFlags, WarpLaneKind,
    },
    event_handler::{
        ActionContext, DownlinkSpawner, EventHandler, HandlerFuture, LaneSpawnOnDone, LaneSpawner,
        Modification, Spawner, StepResult,
    },
    AgentMetadata,
};
use swimos_api::{
    agent::{
        AgentConfig, AgentContext, DownlinkKind, HttpLaneRequestChannel, LaneConfig, StoreKind,
    },
    error::{AgentRuntimeError, DownlinkRuntimeError, DynamicRegistrationError, OpenStoreError},
};
use swimos_connector::ConnectorAgent;
use swimos_utilities::{
    byte_channel::{ByteReader, ByteWriter},
    routing::RouteUri,
    trigger,
};
use tokio::time::timeout;

use crate::{
    config::KafkaLogLevel,
    connector::InvalidLanes,
    DeserializationFormat, KafkaConnectorConfiguration, MapLaneSpec, ValueLaneSpec,
};

use super::Lanes;

struct LaneRequest {
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

struct TestContext;

impl AgentContext for TestContext {
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Unexpected call.");
    }
}

#[derive(Default, Debug)]
struct TestSpawner {
    suspended: FuturesUnordered<HandlerFuture<ConnectorAgent>>,
    lane_requests: RefCell<Vec<LaneRequest>>,
}

impl Spawner<ConnectorAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<ConnectorAgent>) {
        self.suspended.push(fut);
    }
}

impl DownlinkSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        _dl_channel: BoxDownlinkChannel<ConnectorAgent>,
    ) -> Result<(), DownlinkRuntimeError> {
        panic!("Opening downlinks not supported.");
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

async fn run_handler_with_futures<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    handler: H,
) -> HashSet<Modification> {
    let mut spawner = TestSpawner::default();
    let mut modified = run_handler(agent, &spawner, handler);
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
            run_handler(agent, &spawner, h)
        } else {
            let h = spawner.suspended.next().await.expect("No handler.");
            run_handler(agent, &spawner, h)
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

fn run_handler<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    spawner: &TestSpawner,
    mut handler: H,
) -> HashSet<Modification> {
    let route_params = HashMap::new();
    let uri = make_uri();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();
    let agent_context = TestContext;

    let mut action_context = ActionContext::new(
        spawner,
        &agent_context,
        spawner,
        spawner,
        &mut join_lane_init,
        &mut ad_hoc_buffer,
    );

    let mut modified = HashSet::new();

    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { modified_item } => {
                if let Some(m) = modified_item {
                    modified.insert(m);
                }
            }
            StepResult::Fail(err) => panic!("Handler Failed: {}", err),
            StepResult::Complete { modified_item, .. } => {
                if let Some(m) = modified_item {
                    modified.insert(m);
                }
                break modified;
            }
        }
    }
}

#[test]
fn lanes_from_spec() {
    let value_lanes = vec![
        ValueLaneSpec::new(None, "$key", true),
        ValueLaneSpec::new(Some("name"), "$payload.field", false),
    ];
    let map_lanes = vec![MapLaneSpec::new("map", "$key", "$payload", true, false)];
    let lanes =
        Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect("Invalid specification.");

    assert_eq!(lanes.total_lanes, 3);

    let value_lanes = lanes
        .value_lanes
        .iter()
        .map(|l| l.name())
        .collect::<Vec<_>>();
    let map_lanes = lanes.map_lanes.iter().map(|l| l.name()).collect::<Vec<_>>();

    assert_eq!(&value_lanes, &["key", "name"]);
    assert_eq!(&map_lanes, &["map"]);
}

#[test]
fn value_lane_collision() {
    let value_lanes = vec![
        ValueLaneSpec::new(None, "$key", true),
        ValueLaneSpec::new(Some("key"), "$payload.field", false),
    ];
    let map_lanes = vec![];
    let err = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");
    assert_eq!(err, InvalidLanes::NameCollision("key".to_string()))
}

#[test]
fn map_lane_collision() {
    let value_lanes = vec![];
    let map_lanes = vec![
        MapLaneSpec::new("map", "$key", "$payload", true, false),
        MapLaneSpec::new("map", "$key[0]", "$payload", true, true),
    ];
    let err = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");
    assert_eq!(err, InvalidLanes::NameCollision("map".to_string()))
}

#[test]
fn value_map_lane_collision() {
    let value_lanes = vec![ValueLaneSpec::new(Some("field"), "$payload.field", false)];
    let map_lanes = vec![MapLaneSpec::new("field", "$key", "$payload", true, false)];
    let err = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");
    assert_eq!(err, InvalidLanes::NameCollision("field".to_string()))
}

fn make_config() -> KafkaConnectorConfiguration {
    KafkaConnectorConfiguration {
        properties: HashMap::new(),
        log_level: KafkaLogLevel::Warning,
        value_lanes: vec![ValueLaneSpec::new(None, "$key", true)],
        map_lanes: vec![MapLaneSpec::new(
            "map",
            "$payload.key",
            "$payload.value",
            true,
            true,
        )],
        key_deserializer: DeserializationFormat::Recon,
        value_deserializer: DeserializationFormat::Recon,
    }
}

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn open_lanes() {
    let value_specs = vec![ValueLaneSpec::new(None, "$key", true)];
    let map_specs = vec![MapLaneSpec::new(
        "map",
        "$payload.key",
        "$payload.value",
        true,
        true,
    )];
    let lanes =
        Lanes::try_from_lane_specs(&value_specs, &map_specs).expect("Invalid specifications.");

    let (tx, rx) = trigger::trigger();

    let handler = lanes.open_lanes(tx);
    let agent = ConnectorAgent::default();

    let handler_task = run_handler_with_futures(&agent, handler);

    let (modified, done_result) = timeout(TEST_TIMEOUT, join(handler_task, rx))
        .await
        .expect("Test timed out.");

    assert!(modified.is_empty());
    assert!(done_result.is_ok());

    let expected_value_lanes = ["key".to_string()].into_iter().collect::<HashSet<_>>();
    let expected_map_lanes = ["map".to_string()].into_iter().collect::<HashSet<_>>();

    assert_eq!(agent.value_lanes(), expected_value_lanes);
    assert_eq!(agent.map_lanes(), expected_map_lanes);
}
