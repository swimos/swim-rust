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

use std::{borrow::Borrow, collections::HashMap};

use crate::connector::test_util::{run_handler, TestMessage, TestSpawner};
use swimos_agent::agent_model::{AgentSpec, ItemDescriptor, ItemFlags};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::{
    config::{IngressMapLaneSpec, IngressValueLaneSpec},
    deser::{MessageDeserializer, ReconDeserializer},
    selector::{
        ChainSelector, InvalidLanes, MapLaneSelector, PayloadSelector, TopicSelector,
        ValueLaneSelector,
    },
    ConnectorAgent,
};
use swimos_model::Value;

use super::{Lanes, MqttMessageSelector, MqttSelector};

const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";

#[test]
fn interpret_lanes_good() {
    let value_lanes = vec![IngressValueLaneSpec::new(
        Some(VALUE_LANE),
        "$payload",
        true,
    )];
    let map_lanes = vec![IngressMapLaneSpec::new(
        MAP_LANE, "$topic", "$payload", false, true,
    )];

    let Lanes {
        value_lanes,
        map_lanes,
    } = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect("Lanes should be valid");

    let expected_val_sel = MqttSelector::inject(PayloadSelector::new(ChainSelector::default()));
    let expected_map_key_sel = MqttSelector::inject(TopicSelector);

    let expected_value = vec![ValueLaneSelector::new(
        VALUE_LANE.to_string(),
        expected_val_sel.clone(),
        true,
    )];

    let expected_map = vec![MapLaneSelector::new(
        MAP_LANE.to_string(),
        expected_map_key_sel,
        expected_val_sel,
        true,
        false,
    )];

    assert_eq!(value_lanes, expected_value);
    assert_eq!(map_lanes, expected_map);
}

#[test]
fn interpret_lanes_value_lane_collision() {
    let value_lanes = vec![
        IngressValueLaneSpec::new(Some(VALUE_LANE), "$payload", true),
        IngressValueLaneSpec::new(Some(VALUE_LANE), "$topic", false),
    ];
    let map_lanes = vec![IngressMapLaneSpec::new(
        MAP_LANE, "$topic", "$payload", false, true,
    )];

    let error = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");

    assert_eq!(error, InvalidLanes::NameCollision(VALUE_LANE.to_string()));
}

#[test]
fn interpret_lanes_map_lane_collision() {
    let value_lanes = vec![IngressValueLaneSpec::new(
        Some(VALUE_LANE),
        "$payload",
        true,
    )];
    let map_lanes = vec![
        IngressMapLaneSpec::new(MAP_LANE, "$topic", "$payload", false, true),
        IngressMapLaneSpec::new(MAP_LANE, "$payload.key", "$payload.value", true, true),
    ];

    let error = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");

    assert_eq!(error, InvalidLanes::NameCollision(MAP_LANE.to_string()));
}

#[test]
fn interpret_lanes_value_and_map_lane_collision() {
    let value_lanes = vec![IngressValueLaneSpec::new(
        Some(VALUE_LANE),
        "$payload",
        true,
    )];
    let map_lanes = vec![
        IngressMapLaneSpec::new(MAP_LANE, "$topic", "$payload", false, true),
        IngressMapLaneSpec::new(VALUE_LANE, "$payload.key", "$payload.value", true, true),
    ];

    let error = Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect_err("Should fail.");

    assert_eq!(error, InvalidLanes::NameCollision(VALUE_LANE.to_string()));
}

fn init_agent() -> (ConnectorAgent, u64, u64) {
    let agent = ConnectorAgent::default();

    let value_id = agent
        .register_dynamic_item(
            VALUE_LANE,
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Value,
                flags: ItemFlags::TRANSIENT,
            },
        )
        .expect("Registration failed.");
    let map_id = agent
        .register_dynamic_item(
            MAP_LANE,
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Map,
                flags: ItemFlags::TRANSIENT,
            },
        )
        .expect("Registration failed");

    (agent, value_id, map_id)
}

#[test]
fn select_handler() {
    let (mut agent, value_id, map_id) = init_agent();
    let value_lanes = vec![IngressValueLaneSpec::new(
        Some(VALUE_LANE),
        "$payload",
        true,
    )];
    let map_lanes = vec![IngressMapLaneSpec::new(
        MAP_LANE, "$topic", "$payload", false, true,
    )];

    let lanes =
        Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect("Lanes should be valid");
    let selector = MqttMessageSelector::new(ReconDeserializer.boxed(), lanes);

    let message = TestMessage::new("topic", "34");

    let handler = selector
        .handle_message(&message)
        .expect("Should produce a handler.");
    let spawner = TestSpawner::default();
    let modified = run_handler(&agent, &spawner, handler);
    assert_eq!(modified, [value_id, map_id].into_iter().collect());

    let guard = agent.value_lane(VALUE_LANE).expect("Lane absent.");
    guard.read(|v| assert_eq!(v, &Value::from(34)));
    drop(guard);

    let guard = agent.map_lane(MAP_LANE).expect("Lane absent.");
    guard.get_map(|map| {
        let expected = [(Value::text("topic"), Value::from(34))]
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(map, &expected);
    });
}
