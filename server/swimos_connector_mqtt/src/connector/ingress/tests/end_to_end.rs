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

use std::time::Duration;

use bytes::BytesMut;
use futures::{future::join, TryStreamExt};
use swimos_agent::agent_model::{AgentSpec, ItemDescriptor, ItemFlags};
use swimos_agent_protocol::{encoding::command::CommandMessageDecoder, CommandMessage};
use swimos_api::{address::Address, agent::WarpLaneKind};
use swimos_connector::{
    config::{
        format::DataFormat, IngressValueLaneSpec, RelaySpecification, ValueRelaySpecification,
    },
    BaseConnector, ConnectorAgent, IngressConnector, IngressContext,
};
use swimos_connector_util::{run_handler_with_futures, run_handler_with_futures_and_cmds};
use swimos_model::Value;
use swimos_utilities::trigger;
use tokio::time::timeout;
use tokio_util::codec::Decoder;

use crate::{
    facade::MqttFactory,
    generator::{generate_data, TOPIC},
    MqttIngressConfiguration, MqttIngressConnector, Subscription,
};

const CLIENT_URL: &str = "mqtt://localhost:1883?client_id=test";
const GEN_URL: &str = "mqtt://localhost:1883?client_id=generator";
const LANE_NAME: &str = "value_lane";

fn make_configuration() -> MqttIngressConfiguration {
    MqttIngressConfiguration {
        url: CLIENT_URL.to_string(),
        value_lanes: vec![IngressValueLaneSpec::new(Some(LANE_NAME), "$payload", true)],
        map_lanes: vec![],
        relays: vec![RelaySpecification::Value(ValueRelaySpecification::new(
            "/node", "lane", "$payload", true,
        ))],
        payload_deserializer: DataFormat::String,
        subscription: Subscription::Topic(TOPIC.to_string()),
        keep_alive_secs: None,
        max_packet_size: None,
        channel_size: Some(0),
        credentials: None,
    }
}

struct TestContext<'a> {
    agent: &'a ConnectorAgent,
}

impl IngressContext for TestContext<'_> {
    fn open_lane(&mut self, name: &str, kind: WarpLaneKind) {
        let TestContext { agent } = self;
        agent
            .register_dynamic_item(
                name,
                ItemDescriptor::WarpLane {
                    kind,
                    flags: ItemFlags::TRANSIENT,
                },
            )
            .expect("Registering lane failed.");
    }
}

async fn init_connector(agent: &ConnectorAgent) -> MqttIngressConnector<MqttFactory> {
    let config = make_configuration();
    let connector = MqttIngressConnector::for_config(config);

    let mut init_context = TestContext { agent };
    connector
        .initialize(&mut init_context)
        .expect("Initialization failed.");

    let (done_tx, done_rx) = trigger::trigger();
    let handler = connector.on_start(done_tx);

    assert!(run_handler_with_futures(agent, handler).await.is_empty());
    done_rx.await.expect("Starting connector failed.");
    connector
}

async fn drive_connector(
    agent: &mut ConnectorAgent,
    connector: &MqttIngressConnector<MqttFactory>,
    events: usize,
) {
    let mut stream = connector.create_stream().expect("Creating stream failed.");
    let mut command_buffer = BytesMut::new();
    let mut prev = None;
    for _ in 0..events {
        let handler = stream
            .try_next()
            .await
            .expect("Receiving message failed.")
            .expect("Stream stopped.");
        command_buffer.clear();
        run_handler_with_futures_and_cmds(agent, handler, &mut command_buffer).await;
        check_value(&mut prev, agent, &mut command_buffer);
    }
}

fn check_value(
    prev: &mut Option<String>,
    agent: &mut ConnectorAgent,
    command_buffer: &mut BytesMut,
) {
    let guard = agent.value_lane(LANE_NAME).expect("Lane not defined");
    let mut decoder = CommandMessageDecoder::<String, Value>::default();
    let expected_addr = Address::new(None, "/node", "lane").owned();
    match guard.read(Value::clone) {
        Value::Text(s) => {
            let string = s.to_string();
            if let Some(p) = prev.take() {
                assert_ne!(string, p);
            }
            let cmd_message = decoder
                .decode_eof(command_buffer)
                .expect("Decoding failed.")
                .expect("Expected command.");
            match cmd_message {
                CommandMessage::Addressed {
                    target,
                    command,
                    overwrite_permitted,
                } => {
                    assert_eq!(target, expected_addr);
                    assert_eq!(command, Value::text(&string));
                    assert!(!overwrite_permitted);
                }
                ow => panic!("Unexpected command message: {:?}", ow),
            }
            *prev = Some(string);
        }
        _ => panic!("Expected text."),
    }
}

async fn connector_task(done_tx: trigger::Sender) {
    let mut agent = ConnectorAgent::default();
    let connector = init_connector(&agent).await;
    drive_connector(&mut agent, &connector, 10).await;
    done_tx.trigger();
}

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
#[ignore] // Ignored by default as this relies on an external service being present.
async fn run_ingress_connector() {
    let (done_tx, done_rx) = trigger::trigger();
    let run_connector = connector_task(done_tx);
    let generator = generate_data(GEN_URL.to_string(), done_rx);
    let (gen_result, _) = timeout(TEST_TIMEOUT, join(generator, run_connector))
        .await
        .expect("Timed out.");
    assert!(gen_result.is_ok());
}
