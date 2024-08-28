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

#[cfg(feature = "json")]
mod end_to_end;
mod integration;

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    time::Duration,
};

use bytes::BytesMut;
use futures::{future::join, stream::FuturesUnordered, StreamExt};
use swimos_agent::{
    agent_model::{
        downlink::BoxDownlinkChannelFactory, AgentSpec, ItemDescriptor, ItemFlags, WarpLaneKind,
    },
    event_handler::{
        ActionContext, DownlinkSpawnOnDone, LinkSpawner, EventHandler, HandlerFuture,
        LaneSpawnOnDone, LaneSpawner, Spawner, StepResult
    },
    AgentMetadata,
};
use swimos_api::{address::Address, agent::AgentConfig, error::{CommanderRegistrationError, DynamicRegistrationError}};
use swimos_connector::ConnectorAgent;
use swimos_model::{Item, Text, Value};
use swimos_recon::print_recon_compact;
use swimos_utilities::{routing::RouteUri, trigger};
use tokio::time::timeout;

use crate::{
    connector::{InvalidLanes, MessageSelector},
    deser::{MessageDeserializer, MessageView, ReconDeserializer},
    error::{DeserializationError, LaneSelectorError},
    selector::{
        BasicSelector, ChainSelector, Deferred, LaneSelector, MapLaneSelector, SlotSelector,
        ValueLaneSelector,
    },
    MapLaneSpec, ValueLaneSpec,
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

impl LinkSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        _path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
        _on_done: DownlinkSpawnOnDone<ConnectorAgent>,
    ) {
        panic!("Opening downlinks not supported.");
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

async fn run_handler_with_futures<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    handler: H,
) -> HashSet<u64> {
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

fn setup_agent() -> (ConnectorAgent, HashMap<String, u64>) {
    let agent = ConnectorAgent::default();
    let mut ids = HashMap::new();
    let id1 = agent
        .register_dynamic_item(
            "key",
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Value,
                flags: ItemFlags::TRANSIENT,
            },
        )
        .expect("Registration failed.");
    let id2 = agent
        .register_dynamic_item(
            "map",
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Map,
                flags: ItemFlags::TRANSIENT,
            },
        )
        .expect("Registration failed.");
    ids.insert("key".to_string(), id1);
    ids.insert("map".to_string(), id2);
    (agent, ids)
}

struct TestDeferred {
    value: Value,
}

impl From<Value> for TestDeferred {
    fn from(value: Value) -> Self {
        TestDeferred { value }
    }
}

impl Deferred for TestDeferred {
    fn get(&mut self) -> Result<&Value, DeserializationError> {
        Ok(&self.value)
    }
}

fn make_key_value(key: impl Into<Value>, value: impl Into<Value>) -> Value {
    Value::record(vec![Item::slot("key", key), Item::slot("value", value)])
}

fn make_key_only(key: impl Into<Value>) -> Value {
    Value::record(vec![Item::slot("key", key)])
}

#[test]
fn value_lane_selector_handler() {
    let (mut agent, ids) = setup_agent();

    let selector = ValueLaneSelector::new(
        "key".to_string(),
        LaneSelector::Key(ChainSelector::default()),
        true,
    );

    let topic = Value::text("topic_name");
    let mut key = TestDeferred::from(Value::from(3));
    let mut value = TestDeferred::from(make_key_value("a", 7));

    let handler = selector
        .select_handler(&topic, &mut key, &mut value)
        .expect("Selector failed.");
    let spawner = TestSpawner::default();
    let modified = run_handler(&agent, &spawner, handler);

    assert_eq!(modified, [ids["key"]].into_iter().collect::<HashSet<_>>());
    let lane = agent.value_lane("key").expect("Lane missing.");
    lane.read(|v| assert_eq!(v, &Value::from(3)));
}

#[test]
fn value_lane_selector_handler_optional_field() {
    let (agent, _) = setup_agent();

    let selector = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("other"),
    )]));

    let selector = ValueLaneSelector::new("other".to_string(), selector, false);

    let topic = Value::text("topic_name");
    let mut key = TestDeferred::from(Value::from(3));
    let mut value = TestDeferred::from(make_key_value("a", 7));

    let handler = selector
        .select_handler(&topic, &mut key, &mut value)
        .expect("Selector failed.");
    let spawner = TestSpawner::default();
    let modified = run_handler(&agent, &spawner, handler);

    assert!(modified.is_empty());
}

#[test]
fn value_lane_selector_handler_missing_field() {
    let selector = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("other"),
    )]));

    let selector = ValueLaneSelector::new("other".to_string(), selector, true);

    let topic = Value::text("topic_name");
    let mut key = TestDeferred::from(Value::from(3));
    let mut value = TestDeferred::from(make_key_value("a", 7));

    let error = selector
        .select_handler(&topic, &mut key, &mut value)
        .expect_err("Should fail.");
    assert!(matches!(error, LaneSelectorError::MissingRequiredLane(name) if &name == "other"));
}

#[test]
fn map_lane_selector_handler() {
    let (mut agent, ids) = setup_agent();

    let key = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("key"),
    )]));
    let value = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("value"),
    )]));

    let selector = MapLaneSelector::new("map".to_string(), key, value, true, false);

    let topic = Value::text("topic_name");
    let mut key = TestDeferred::from(Value::from(3));
    let mut value = TestDeferred::from(make_key_value("a", 7));

    let handler = selector
        .select_handler(&topic, &mut key, &mut value)
        .expect("Selector failed.");
    let spawner = TestSpawner::default();
    let modified = run_handler(&agent, &spawner, handler);

    assert_eq!(modified, [ids["map"]].into_iter().collect::<HashSet<_>>());
    let lane = agent.map_lane("map").expect("Lane missing.");
    lane.get_map(|m| {
        let expected = [(Value::text("a"), Value::from(7))]
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(m, &expected);
    });
}

#[test]
fn map_lane_selector_handler_optional_field() {
    let (agent, _) = setup_agent();

    let key = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("key"),
    )]));
    let value = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("value"),
    )]));

    let selector = MapLaneSelector::new("map".to_string(), key, value, false, false);

    let topic = Value::text("topic_name");
    let mut key = TestDeferred::from(Value::from(3));
    let mut value = TestDeferred::from(Value::Extant);

    let handler = selector
        .select_handler(&topic, &mut key, &mut value)
        .expect("Selector failed.");
    let spawner = TestSpawner::default();
    let modified = run_handler(&agent, &spawner, handler);

    assert!(modified.is_empty());
}

#[test]
fn map_lane_selector_handler_missing_field() {
    let key = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("key"),
    )]));
    let value = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("value"),
    )]));

    let selector = MapLaneSelector::new("map".to_string(), key, value, true, false);

    let topic = Value::text("topic_name");
    let mut key = TestDeferred::from(Value::from(3));
    let mut value = TestDeferred::from(Value::Extant);

    let error = selector
        .select_handler(&topic, &mut key, &mut value)
        .expect_err("Should fail.");
    assert!(matches!(error, LaneSelectorError::MissingRequiredLane(name) if &name == "map"));
}

#[test]
fn map_lane_selector_remove() {
    let (mut agent, ids) = setup_agent();

    let key = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("key"),
    )]));
    let value = LaneSelector::Payload(ChainSelector::new(vec![BasicSelector::Slot(
        SlotSelector::for_field("value"),
    )]));

    let selector = MapLaneSelector::new("map".to_string(), key, value, true, true);

    let topic = Value::text("topic_name");
    let mut key = TestDeferred::from(Value::from(3));
    let mut value = TestDeferred::from(make_key_value("a", 7));

    let update_handler = selector
        .select_handler(&topic, &mut key, &mut value)
        .expect("Selector failed.");
    let spawner = TestSpawner::default();
    let modified = run_handler(&agent, &spawner, update_handler);

    assert_eq!(modified, [ids["map"]].into_iter().collect::<HashSet<_>>());
    let lane = agent.map_lane("map").expect("Lane missing.");
    lane.get_map(|m| {
        let expected = [(Value::text("a"), Value::from(7))]
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(m, &expected);
    });

    drop(lane);

    let mut value2 = TestDeferred::from(make_key_only("a"));
    let remove_handler = selector
        .select_handler(&topic, &mut key, &mut value2)
        .expect("Selector failed.");
    let modified = run_handler(&agent, &spawner, remove_handler);

    assert_eq!(modified, [ids["map"]].into_iter().collect::<HashSet<_>>());
    let lane = agent.map_lane("map").expect("Lane missing.");
    lane.get_map(|m| {
        assert!(m.is_empty());
    });
}

#[tokio::test]
async fn handle_message() {
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

    let (agent, ids) = setup_agent();

    let selector =
        MessageSelector::new(ReconDeserializer.boxed(), ReconDeserializer.boxed(), lanes);

    let key = Value::from(3);
    let payload = make_key_value("ab", 67);
    let key_str = format!("{}", print_recon_compact(&key));
    let payload_str = format!("{}", print_recon_compact(&payload));

    let message = MessageView {
        topic: "topic_name",
        key: key_str.as_bytes(),
        payload: payload_str.as_bytes(),
    };

    let (tx, rx) = trigger::trigger();

    let handler = selector
        .handle_message(&message, tx)
        .expect("Selector failed.");

    let handler_task = run_handler_with_futures(&agent, handler);

    let (modified, done_result) = timeout(TEST_TIMEOUT, join(handler_task, rx))
        .await
        .expect("Test timed out.");

    assert!(done_result.is_ok());
    assert_eq!(modified, ids.values().copied().collect::<HashSet<_>>());
}

#[tokio::test]
async fn handle_message_missing_field() {
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

    let selector =
        MessageSelector::new(ReconDeserializer.boxed(), ReconDeserializer.boxed(), lanes);

    let key = Value::from(3);
    let payload = Value::text("word");
    let key_str = format!("{}", print_recon_compact(&key));
    let payload_str = format!("{}", print_recon_compact(&payload));

    let message = MessageView {
        topic: "topic_name",
        key: key_str.as_bytes(),
        payload: payload_str.as_bytes(),
    };

    let (tx, _rx) = trigger::trigger();

    let result = selector.handle_message(&message, tx);
    assert!(matches!(result, Err(LaneSelectorError::MissingRequiredLane(name)) if name == "map"));
}

#[tokio::test]
async fn handle_message_bad_data() {
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

    let selector =
        MessageSelector::new(ReconDeserializer.boxed(), ReconDeserializer.boxed(), lanes);

    let key = Value::from(3);
    let key_str = format!("{}", print_recon_compact(&key));

    let message = MessageView {
        topic: "topic_name",
        key: key_str.as_bytes(),
        payload: b"^*$&@*@",
    };

    let (tx, _rx) = trigger::trigger();

    let result = selector.handle_message(&message, tx);
    assert!(matches!(
        result,
        Err(LaneSelectorError::DeserializationFailed(_))
    ));
}
