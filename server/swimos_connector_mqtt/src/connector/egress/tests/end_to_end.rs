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

use std::{collections::HashMap, time::Duration};

use futures::{future::join, TryFutureExt};
use rumqttc::{Event, Incoming, MqttOptions, QoS};
use swimos_agent::agent_model::{AgentSpec, ItemDescriptor, ItemFlags};
use swimos_api::{address::Address, agent::WarpLaneKind};
use swimos_connector::{
    config::format::DataFormat, BaseConnector, ConnectorAgent, EgressConnector,
    EgressConnectorSender, EgressContext, MessageSource, SendResult,
};
use swimos_connector_util::run_handler_with_futures;
use swimos_model::Value;
use swimos_utilities::trigger;
use tokio::time::timeout;
use tracing::debug;

use crate::{
    config::{ExtractionSpec, TopicSpecifier},
    facade::MqttFactory,
    EgressDownlinkSpec, EgressLaneSpec, MqttEgressConfiguration, MqttEgressConnector,
};

const CLIENT_URL: &str = "mqtt://localhost:1883?client_id=test";
const CONSUMER_URL: &str = "mqtt://localhost:1883?client_id=consumer";
const LANE_NAME: &str = "lane_name";
const TOPIC: &str = "test/egress";

fn make_config() -> MqttEgressConfiguration {
    MqttEgressConfiguration {
        url: CLIENT_URL.to_string(),
        fixed_topic: Some(TOPIC.to_string()),
        value_lanes: vec![EgressLaneSpec {
            name: LANE_NAME.to_string(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                payload_selector: None,
            },
        }],
        map_lanes: vec![],
        event_downlinks: vec![EgressDownlinkSpec {
            address: Address::new(None, "/node", "lane").owned(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                payload_selector: None,
            },
        }],
        map_event_downlinks: vec![],
        payload_serializer: DataFormat::String,
        keep_alive_secs: None,
        max_packet_size: None,
        max_inflight: None,
        channel_size: Some(0),
        credentials: None,
    }
}

struct TestContext<'a> {
    agent: &'a ConnectorAgent,
}

impl EgressContext for TestContext<'_> {
    fn open_lane(&mut self, name: &str, kind: WarpLaneKind) {
        self.agent
            .register_dynamic_item(
                name,
                ItemDescriptor::WarpLane {
                    kind,
                    flags: ItemFlags::TRANSIENT,
                },
            )
            .expect("Failed to register lane.");
    }

    fn open_event_downlink(&mut self, address: Address<&str>) {
        assert_eq!(address, Address::new(None, "/node", "lane"));
    }

    fn open_map_downlink(&mut self, _address: Address<&str>) {
        panic!("Unexpected map downlink.");
    }
}

async fn init_connector(
    agent: &ConnectorAgent,
    connector: &MqttEgressConnector<MqttFactory>,
    done_tx: trigger::Sender,
) {
    let mut context = TestContext { agent };

    assert!(connector.initialize(&mut context).is_ok());

    let handler = connector.on_start(done_tx);
    assert!(run_handler_with_futures(agent, handler).await.is_empty());
}

async fn read_from_topic(num_messages: usize, subscribed: trigger::Sender) -> Vec<String> {
    let opts = MqttOptions::parse_url(CONSUMER_URL).expect("BAD URL.");

    let (client, mut event_loop) = rumqttc::AsyncClient::new(opts, 0);

    let sub = async {
        client
            .subscribe(TOPIC, QoS::AtMostOnce)
            .await
            .expect("Subscription request not sent.");
    };

    let mut messages = Vec::with_capacity(num_messages);

    let mut sub_trigger = Some(subscribed);

    let events = async move {
        while messages.len() < num_messages {
            match event_loop.poll().await.expect("Client failed.") {
                Event::Incoming(Incoming::SubAck(_)) => {
                    if let Some(s) = sub_trigger.take() {
                        s.trigger();
                    }
                }
                Event::Incoming(Incoming::Publish(body)) => {
                    let bytes = body.payload.as_ref();
                    let string = std::str::from_utf8(bytes)
                        .expect("Bad payload.")
                        .to_string();
                    messages.push(string);
                }
                ow => debug!(event = ?ow, "Processed MQTT event."),
            }
        }
        messages
    };

    let (_, messages) = join(sub, events).await;
    messages
}

async fn drive_connector(
    agent: &ConnectorAgent,
    connector: &MqttEgressConnector<MqttFactory>,
    lane_messages: Vec<String>,
    dl_messages: Vec<String>,
    init_done: trigger::Receiver,
    subscribed: trigger::Receiver,
) {
    init_done.await.expect("Initialization did not complete.");
    subscribed.await.expect("Subscription failed.");
    let sender = connector
        .make_sender(&HashMap::new())
        .expect("Failed to create sender.");

    for message in lane_messages {
        let value = Value::text(&message);
        match sender
            .send(MessageSource::Lane(LANE_NAME), None, &value)
            .expect("Expected future.")
        {
            SendResult::Suspend(fut) => {
                let handler = fut.into_future().await.expect("Send failed.");
                assert!(run_handler_with_futures(agent, handler).await.is_empty());
            }
            _ => panic!("Expected future."),
        }
    }

    let addr = Address::new(None, "/node", "lane").owned();

    for message in dl_messages {
        let value = Value::text(&message);
        match sender
            .send(MessageSource::Downlink(&addr), None, &value)
            .expect("Expected future.")
        {
            SendResult::Suspend(fut) => {
                let handler = fut.into_future().await.expect("Send failed.");
                assert!(run_handler_with_futures(agent, handler).await.is_empty());
            }
            _ => panic!("Expected future."),
        }
    }
}

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
#[ignore] // Ignored by default as this relies on an external service being present.
async fn run_egress_connector() {
    timeout(TEST_TIMEOUT, async {
        let config = make_config();
        let agent = ConnectorAgent::default();
        let connector = MqttEgressConnector::for_config(config);
        let (done_tx, done_rx) = trigger::trigger();
        let (sub_tx, sub_rx) = trigger::trigger();

        let background = init_connector(&agent, &connector, done_tx);

        let lane_messages = vec!["lane1".to_string(), "lane2".to_string()];
        let dl_messages = vec!["downlink1".to_string(), "downlink2".to_string()];

        let connector_task = drive_connector(
            &agent,
            &connector,
            lane_messages.clone(),
            dl_messages.clone(),
            done_rx,
            sub_rx,
        );

        let consume_task = read_from_topic(lane_messages.len() + dl_messages.len(), sub_tx);

        let messages = tokio::select! {
            (_, messages) = join(connector_task, consume_task) => messages,
            _ = background => panic!("Background task stopped."),
        };

        let expected_messages = lane_messages
            .into_iter()
            .chain(dl_messages.into_iter())
            .collect::<Vec<_>>();
        assert_eq!(messages, expected_messages);
    })
    .await
    .expect("Timed out.");
}
