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
    collections::{HashMap, HashSet},
    time::Duration,
};

use futures::future::join;
use swimos_agent::agent_model::{AgentSpec, ItemDescriptor, ItemFlags, WarpLaneKind};
use swimos_connector::ConnectorAgent;
use swimos_model::{Item, Value};
use swimos_recon::print_recon_compact;
use swimos_utilities::trigger;
use tokio::time::timeout;

use crate::{
    connector::{
        ingress::{InvalidLanes, MessageSelector},
        test_util::{run_handler, run_handler_with_futures, TestSpawner},
    },
    deser::{MessageDeserializer, MessageView, ReconDeserializer},
    error::{DeserializationError, LaneSelectorError},
    selector::{
        BasicSelector, ChainSelector, Deferred, LaneSelector, MapLaneSelector, SlotSelector,
        ValueLaneSelector,
    },
    IngressMapLaneSpec, IngressValueLaneSpec,
};

use super::Lanes;

#[test]
fn lanes_from_spec() {
    let value_lanes = vec![
        IngressValueLaneSpec::new(None, "$key", true),
        IngressValueLaneSpec::new(Some("name"), "$payload.field", false),
    ];
    let map_lanes = vec![IngressMapLaneSpec::new(
        "map", "$key", "$payload", true, false,
    )];
    let lanes =
        Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect("Invalid specification.");

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
        IngressValueLaneSpec::new(None, "$key", true),
        IngressValueLaneSpec::new(Some("key"), "$payload.field", false),
    ];
    let map_lanes = vec![];
    let err = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");
    assert_eq!(err, InvalidLanes::NameCollision("key".to_string()))
}

#[test]
fn map_lane_collision() {
    let value_lanes = vec![];
    let map_lanes = vec![
        IngressMapLaneSpec::new("map", "$key", "$payload", true, false),
        IngressMapLaneSpec::new("map", "$key[0]", "$payload", true, true),
    ];
    let err = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");
    assert_eq!(err, InvalidLanes::NameCollision("map".to_string()))
}

#[test]
fn value_map_lane_collision() {
    let value_lanes = vec![IngressValueLaneSpec::new(
        Some("field"),
        "$payload.field",
        false,
    )];
    let map_lanes = vec![IngressMapLaneSpec::new(
        "field", "$key", "$payload", true, false,
    )];
    let err = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");
    assert_eq!(err, InvalidLanes::NameCollision("field".to_string()))
}

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

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
    let value_specs = vec![IngressValueLaneSpec::new(None, "$key", true)];
    let map_specs = vec![IngressMapLaneSpec::new(
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
    let value_specs = vec![IngressValueLaneSpec::new(None, "$key", true)];
    let map_specs = vec![IngressMapLaneSpec::new(
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
    let value_specs = vec![IngressValueLaneSpec::new(None, "$key", true)];
    let map_specs = vec![IngressMapLaneSpec::new(
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
