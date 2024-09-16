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

use crate::connectors::tests::fail;
use crate::deserialization::{FnDeserializer, MessagePart, MessageView};
use crate::relay::{AgentRelay, LaneSelector, PayloadSelector, RecordSelectors, Relay, Selectors};
use crate::{
    connectors::relay::selector::NodeSelector,
    connectors::tests::{run_handler, TestSpawner},
    deserialization::{Computed, Deferred, DeserializationError},
    relay::RelayConnectorAgent,
};
use bytes::{Buf, BytesMut};
use serde::Serialize;
use serde_json::json;
use std::str::FromStr;
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::event_handler::EventHandler;
use swimos_agent::{agent_model::AgentSpec, event_handler::EventHandlerError};
use swimos_agent_protocol::encoding::ad_hoc::AdHocCommandDecoder;
use swimos_agent_protocol::{AdHocCommand, MapMessage};
use swimos_api::address::Address;
use swimos_form::Form;
use swimos_model::{Item, Value};
use tokio_util::codec::Decoder;

fn mock_value() -> serde_json::Value {
    json! {
        {
            "success": 200,
            "payload": "waffles"
        }
    }
}

fn to_bytes(s: impl Serialize) -> Vec<u8> {
    serde_json::to_vec(&s).unwrap()
}

fn convert_json_value(input: serde_json::Value) -> swimos_model::Value {
    match input {
        serde_json::Value::Null => swimos_model::Value::Extant,
        serde_json::Value::Bool(p) => swimos_model::Value::BooleanValue(p),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_u64() {
                swimos_model::Value::UInt64Value(i)
            } else if let Some(i) = n.as_i64() {
                swimos_model::Value::Int64Value(i)
            } else {
                swimos_model::Value::Float64Value(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => swimos_model::Value::Text(s.into()),
        serde_json::Value::Array(arr) => swimos_model::Value::record(
            arr.into_iter()
                .map(|v| Item::ValueItem(convert_json_value(v)))
                .collect(),
        ),
        serde_json::Value::Object(obj) => swimos_model::Value::record(
            obj.into_iter()
                .map(|(k, v)| {
                    Item::Slot(swimos_model::Value::Text(k.into()), convert_json_value(v))
                })
                .collect(),
        ),
    }
}

fn deferred_deserializer(payload: Vec<u8>) -> impl Deferred {
    Computed::new(move || match serde_json::from_slice(payload.as_ref()) {
        Ok(v) => Ok(convert_json_value(v)),
        Err(e) => Err(DeserializationError::new(e)),
    })
}

#[test]
fn node_uri() {
    let route = NodeSelector::from_str("/$key/$value.payload").expect("Failed to parse route");
    let key = to_bytes(1);
    let value = to_bytes(mock_value());

    let node_uri = route
        .select(
            &mut deferred_deserializer(key),
            &mut deferred_deserializer(value),
            &"topic".into(),
        )
        .expect("Failed to build node URI");

    assert_eq!(node_uri, "/1/waffles")
}

#[test]
fn invalid_node_uri() {
    let route = NodeSelector::from_str("/$key/$value").expect("Failed to parse route");
    let key = to_bytes(1);
    let value = to_bytes(mock_value());

    route
        .select(
            &mut deferred_deserializer(key),
            &mut deferred_deserializer(value),
            &"topic".into(),
        )
        .expect_err("Should have failed to build node URI due to object being used");
}

#[test]
fn stop_agent() {
    let agent = RelayConnectorAgent;
    let handler = agent
        .on_value_command("stop", BytesMut::default())
        .expect("Missing lane");

    run_handler(
        &TestSpawner::default(),
        &mut BytesMut::default(),
        &agent,
        handler,
        |e| {
            if !matches!(e, EventHandlerError::StopInstructed) {
                panic!("Expected a stop instruction")
            }
        },
    )
}

fn relay(selectors: impl Into<RecordSelectors>) -> impl Relay {
    let deser = FnDeserializer::new(|message: &MessageView, part| {
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Payload => message.payload(),
        };
        let v: serde_json::Value = serde_json::from_slice(payload)?;
        Ok::<_, serde_json::Error>(convert_json_value(v))
    });
    AgentRelay::new(selectors, deser, deser)
}

fn run_relay<R, H>(
    relay: &R,
    messages: Vec<MessageView<'_>>,
    mut commands: Vec<AdHocCommand<String, Command>>,
) where
    R: Relay<Handler = H>,
    H: EventHandler<RelayConnectorAgent> + 'static,
{
    let mut ad_hoc_buffer = BytesMut::default();
    let spawner = TestSpawner::default();
    let agent = RelayConnectorAgent;

    for msg in messages {
        let handler = relay
            .on_record(msg, HandlerContext::default())
            .expect("Failed to build handler");
        run_handler(&spawner, &mut ad_hoc_buffer, &agent, handler, fail);

        while let Some(actual_command) = decode_command(&mut ad_hoc_buffer) {
            let expected_command = commands.pop().expect("Exhausted expected commands early");
            assert_eq!(actual_command, expected_command);
        }

        ad_hoc_buffer.clear();
    }
}

fn decode_command(buf: &mut BytesMut) -> Option<AdHocCommand<String, Command>> {
    let mut decoder = AdHocCommandDecoder::<String, MapMessage<Value, Value>>::default();
    let mut map_buf = buf.clone();
    let command = decoder.decode(&mut map_buf);

    match command {
        Ok(Some(AdHocCommand {
            address,
            command,
            overwrite_permitted,
        })) => {
            buf.advance(buf.len() - map_buf.len());
            Some(AdHocCommand {
                address,
                command: Command::Map(command),
                overwrite_permitted,
            })
        }
        Ok(None) => None,
        Err(_) => {
            let mut decoder = AdHocCommandDecoder::<String, Value>::default();
            decoder.decode(buf).expect("Failed to decode command").map(
                |AdHocCommand {
                     address,
                     command,
                     overwrite_permitted,
                 }| {
                    AdHocCommand {
                        address,
                        command: Command::Value(command),
                        overwrite_permitted,
                    }
                },
            )
        }
    }
}

#[derive(Form, PartialEq, Debug)]
enum Command {
    Value(Value),
    Map(MapMessage<Value, Value>),
}

#[test]
fn value_command() {
    let node = NodeSelector::from_str("/node").unwrap();
    let lane = LaneSelector::from_str("lane").unwrap();
    let payload = PayloadSelector::value("$value", true).unwrap();
    let relay = relay(Selectors::new(node, lane, payload));

    run_relay(
        &relay,
        vec![MessageView {
            topic: "topic",
            key: serde_json::to_vec(&json! {13}).unwrap().as_ref(),
            payload: serde_json::to_vec(&json! {13}).unwrap().as_ref(),
        }],
        vec![AdHocCommand::new(
            Address::new(None, "/node".to_string(), "lane".to_string()),
            Command::Value(Value::Int64Value(13)),
            false,
        )],
    );
}

#[test]
fn map_update() {
    let node = NodeSelector::from_str("/node").unwrap();
    let lane = LaneSelector::from_str("lane").unwrap();
    let payload = PayloadSelector::map("$key", "$value", true, true).unwrap();
    let relay = relay(Selectors::new(node, lane, payload));

    run_relay(
        &relay,
        vec![MessageView {
            topic: "topic",
            key: serde_json::to_vec(&json! {13}).unwrap().as_ref(),
            payload: serde_json::to_vec(&json! {"text"}).unwrap().as_ref(),
        }],
        vec![AdHocCommand::new(
            Address::new(None, "/node".to_string(), "lane".to_string()),
            Command::Map(MapMessage::Update {
                key: Value::Int64Value(13),
                value: Value::from("text"),
            }),
            false,
        )],
    );
}

#[test]
fn map_remove() {
    let node = NodeSelector::from_str("/node").unwrap();
    let lane = LaneSelector::from_str("lane").unwrap();
    let payload = PayloadSelector::map("$key", "$value", false, true).unwrap();
    let relay = relay(Selectors::new(node, lane, payload));

    run_relay(
        &relay,
        vec![MessageView {
            topic: "topic",
            key: serde_json::to_vec(&json! {13}).unwrap().as_ref(),
            payload: &[],
        }],
        vec![AdHocCommand::new(
            Address::new(None, "/node".to_string(), "lane".to_string()),
            Command::Map(MapMessage::Remove {
                key: Value::Int64Value(13),
            }),
            false,
        )],
    );
}

#[test]
fn heterogeneous_selectors() {
    let a = Selectors::new(
        NodeSelector::from_str("/node/$value.a").unwrap(),
        LaneSelector::from_str("lane").unwrap(),
        PayloadSelector::map("$key", "$value.a", true, true).unwrap(),
    );
    let b = Selectors::new(
        NodeSelector::from_str("/node/$value.b").unwrap(),
        LaneSelector::from_str("lane").unwrap(),
        PayloadSelector::map("$key", "$value.b", true, true).unwrap(),
    );
    let c = Selectors::new(
        NodeSelector::from_str("/node/3").unwrap(),
        LaneSelector::from_str("lane").unwrap(),
        PayloadSelector::value("$value", true).unwrap(),
    );
    let relay = relay(RecordSelectors::from([a, b, c]));

    let value = json! {{
        "a": 1,
        "b": 2
    }};

    run_relay(
        &relay,
        vec![MessageView {
            topic: "topic",
            key: serde_json::to_vec(&json! {13}).unwrap().as_ref(),
            payload: serde_json::to_vec(&value).unwrap().as_ref(),
        }],
        vec![
            AdHocCommand::new(
                Address::new(None, "/node/3".to_string(), "lane".to_string()),
                Command::Value(Value::from_vec(vec![("a", 1), ("b", 2)])),
                false,
            ),
            AdHocCommand::new(
                Address::new(None, "/node/2".to_string(), "lane".to_string()),
                Command::Map(MapMessage::Update {
                    key: Value::Int64Value(13),
                    value: Value::Int64Value(2),
                }),
                false,
            ),
            AdHocCommand::new(
                Address::new(None, "/node/1".to_string(), "lane".to_string()),
                Command::Map(MapMessage::Update {
                    key: Value::Int64Value(13),
                    value: Value::Int64Value(1),
                }),
                false,
            ),
        ],
    );
}
