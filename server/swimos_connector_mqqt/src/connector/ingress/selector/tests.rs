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

use std::collections::HashMap;

use bytes::BytesMut;
use swimos_agent::agent_model::{AgentSpec, ItemDescriptor, ItemFlags};
use swimos_agent_protocol::{encoding::command::CommandMessageDecoder, CommandMessage};
use swimos_api::{address::Address, agent::WarpLaneKind};
use swimos_connector::{
    config::{
        IngressMapLaneSpec, IngressValueLaneSpec, RelaySpecification, ValueRelaySpecification,
    },
    deser::{MessageDeserializer, ReconDeserializer},
    selector::Relays,
    ConnectorAgent,
};
use swimos_connector_util::{run_handler_with_commands, TestSpawner};
use swimos_model::Value;
use tokio_util::codec::Decoder;

use crate::facade::MqttMessage;

use super::{Lanes, MqttMessageSelector};

const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";

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
    let relay_specs = vec![RelaySpecification::Value(ValueRelaySpecification {
        node: "/node".to_string(),
        lane: "lane".to_string(),
        payload: "$payload".to_string(),
        required: true,
    })];

    let lanes =
        Lanes::try_from_lane_specs(&value_lanes, &map_lanes).expect("Lanes should be valid");
    let relays = Relays::try_from(relay_specs).expect("Relays should be valid.");
    let selector = MqttMessageSelector::new(ReconDeserializer.boxed(), lanes, relays);

    let message = TestMessage::new("topic", "34");

    let handler = selector
        .handle_message(&message)
        .expect("Should produce a handler.");
    let spawner = TestSpawner::default();

    let mut command_buffer = BytesMut::new();
    let modified = run_handler_with_commands(&agent, &spawner, handler, &mut command_buffer);
    check_commands(&mut command_buffer);
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

fn check_commands(buffer: &mut BytesMut) {
    let mut decoder = CommandMessageDecoder::<String, Value>::default();
    match decoder.decode_eof(buffer) {
        Ok(Some(CommandMessage::Addressed {
            target,
            command,
            overwrite_permitted,
        })) => {
            assert_eq!(
                target,
                Address::new(None, "/node".to_string(), "lane".to_string())
            );
            assert_eq!(command, Value::from(34));
            assert!(!overwrite_permitted);
        }
        Ok(Some(_)) => panic!("Unexpected message."),
        Ok(None) => panic!("No message."),
        Err(err) => panic!("Failed: {}", err),
    }
}

pub struct TestMessage {
    topic: String,
    payload: Vec<u8>,
}

impl TestMessage {
    pub fn new(topic: &str, payload: &str) -> Self {
        TestMessage {
            topic: topic.to_string(),
            payload: payload.as_bytes().to_vec(),
        }
    }
}

impl MqttMessage for TestMessage {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
