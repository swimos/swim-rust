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

use futures::future::join;
use futures::TryFutureExt;
use rumqttc::MqttOptions;
use swimos_agent::agent_model::{AgentSpec, ItemDescriptor, ItemFlags};
use swimos_api::{address::Address, agent::WarpLaneKind};
use swimos_connector::{
    config::format::DataFormat, BaseConnector, ConnectorAgent, EgressConnector,
    EgressConnectorSender, EgressContext, MessageSource, SendResult,
};
use swimos_model::Value;
use swimos_recon::print_recon_compact;
use swimos_utilities::trigger;

use crate::{
    config::{Credentials, ExtractionSpec, TopicSpecifier},
    connector::{
        egress::tests::mock::Outputs,
        test_util::{run_handler, run_handler_with_futures, TestSpawner},
    },
    facade::MqttMessage,
    EgressDownlinkSpec, EgressLaneSpec, MqttEgressConfiguration, MqttEgressConnector,
};

use super::mock::MockFactory;

const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";
const HOST: &str = "ws://localhost:8080";
const NODE1: &str = "/node1";
const NODE2: &str = "/node2";
const LANE: &str = "lane";

fn make_config() -> MqttEgressConfiguration {
    let spec = ExtractionSpec {
        topic_specifier: TopicSpecifier::Fixed,
        payload_selector: None,
    };
    MqttEgressConfiguration {
        url: "mqtt://localhost:8080?client_id=exampleid".to_string(),
        fixed_topic: Some("topic".to_string()),
        value_lanes: vec![EgressLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: spec.clone(),
        }],
        map_lanes: vec![EgressLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: spec.clone(),
        }],
        value_downlinks: vec![EgressDownlinkSpec {
            address: Address::new(Some(HOST), NODE1, LANE).owned(),
            extractor: spec.clone(),
        }],
        map_downlinks: vec![EgressDownlinkSpec {
            address: Address::new(None, NODE2, LANE).owned(),
            extractor: spec,
        }],
        payload_serializer: DataFormat::Recon,
        keep_alive_secs: Some(60),
        max_packet_size: Some(8192),
        max_inflight: Some(5),
        channel_size: Some(16),
        credentials: Some(Credentials {
            username: "mqtt_user".to_string(),
            password: "passw0rd".to_string(),
        }),
    }
}

fn make_expected_opts() -> MqttOptions {
    let mut options = MqttOptions::new("exampleid", "localhost", 8080);
    options.set_keep_alive(Duration::from_secs(60));
    options.set_max_packet_size(8192, 8192);
    options.set_request_channel_capacity(16);
    options.set_inflight(5);
    options.set_credentials("mqtt_user", "passw0rd");
    options
}

#[derive(Default)]
struct TestContext {
    lanes: Vec<(String, WarpLaneKind)>,
    event_dls: Vec<Address<String>>,
    map_dls: Vec<Address<String>>,
}

impl EgressContext for TestContext {
    fn open_lane(&mut self, name: &str, kind: WarpLaneKind) {
        self.lanes.push((name.to_string(), kind));
    }

    fn open_event_downlink(&mut self, address: Address<&str>) {
        self.event_dls.push(address.owned());
    }

    fn open_map_downlink(&mut self, address: Address<&str>) {
        self.map_dls.push(address.owned());
    }
}

#[test]
fn initialize_connector() {
    let factory = MockFactory::new(make_expected_opts());
    let connector = MqttEgressConnector::new(factory, make_config());
    let mut context = TestContext::default();
    assert!(connector.initialize(&mut context).is_ok());

    let TestContext {
        lanes,
        event_dls,
        map_dls,
    } = context;

    assert_eq!(lanes.len(), 2);
    let expected_lanes: HashMap<_, _> = [
        (VALUE_LANE.to_string(), WarpLaneKind::Value),
        (MAP_LANE.to_string(), WarpLaneKind::Map),
    ]
    .into_iter()
    .collect();
    let lane_map: HashMap<_, _> = lanes.into_iter().collect();
    assert_eq!(lane_map, expected_lanes);

    assert_eq!(
        event_dls,
        vec![Address::new(Some(HOST), NODE1, LANE).owned()]
    );
    assert_eq!(map_dls, vec![Address::new(None, NODE2, LANE).owned()]);
}

fn setup_agent() -> ConnectorAgent {
    let agent = ConnectorAgent::default();
    assert!(agent
        .register_dynamic_item(
            VALUE_LANE,
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Value,
                flags: ItemFlags::TRANSIENT
            }
        )
        .is_ok());
    assert!(agent
        .register_dynamic_item(
            MAP_LANE,
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Map,
                flags: ItemFlags::TRANSIENT
            }
        )
        .is_ok());
    agent
}

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn start_connector() {
    let factory = MockFactory::new(make_expected_opts());

    let stop_tx = factory.with_stop();

    let connector = MqttEgressConnector::new(factory, make_config());
    let mut context = TestContext::default();
    assert!(connector.initialize(&mut context).is_ok());
    let agent = setup_agent();

    let (init_tx, init_rx) = trigger::trigger();
    let handler = connector.on_start(init_tx);

    // Stop the driver task otherwise the futures will run forever.
    stop_tx.trigger();

    let handler_task = run_handler_with_futures(&agent, handler);
    let wait_for_done = tokio::time::timeout(TEST_TIMEOUT, join(handler_task, init_rx));

    let (modified, result) = wait_for_done.await.expect("Timed out.");
    assert!(modified.is_empty());
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_and_use_sender() {
    let factory = MockFactory::new(make_expected_opts());

    let stop_tx = factory.with_stop();
    let outputs = factory.outputs();

    let connector = MqttEgressConnector::new(factory, make_config());
    let mut context = TestContext::default();
    assert!(connector.initialize(&mut context).is_ok());
    let agent = setup_agent();

    let (init_tx, init_rx) = trigger::trigger();
    let handler = connector.on_start(init_tx);

    let on_start_and_driver = run_handler_with_futures(&agent, handler);

    let agent_ref = &agent;
    let connector_ref = &connector;
    let send_task = async move {
        // Wait for initialization to complete.
        assert!(init_rx.await.is_ok());

        let sender = connector_ref
            .make_sender(&HashMap::new())
            .expect("Creation failed.");

        let addr = Address::new(None, NODE2, LANE).owned();
        let data = [
            (MessageSource::Lane(VALUE_LANE), None, Value::from(56)),
            (
                MessageSource::Lane(MAP_LANE),
                Some(Value::text("hello")),
                Value::from(4),
            ),
            (
                MessageSource::Downlink(&addr),
                Some(Value::text("world")),
                Value::from(true),
            ),
        ];
        let spawner = TestSpawner::default();
        for (source, key, value) in data {
            let fut = match sender
                .send(source, key.as_ref(), &value)
                .expect("Expected result.")
            {
                SendResult::Suspend(fut) => fut,
                SendResult::RequestCallback(_, _) => panic!("Unexpected callback request."),
                SendResult::Fail(err) => panic!("Failed: {}", err),
            };
            let handler = fut.into_future().await.expect("Write failed.");
            assert!(run_handler(agent_ref, &spawner, handler).is_empty());
        }
        //Stop the driver.
        stop_tx.trigger();
    };

    let wait_for_done = tokio::time::timeout(TEST_TIMEOUT, join(on_start_and_driver, send_task));

    let (modified, _) = wait_for_done.await.expect("Timed out.");
    assert!(modified.is_empty());

    let Outputs { published } = &*outputs.lock();

    assert_eq!(published.len(), 3);
    let expected_payloads = vec![Value::from(56), Value::from(4), Value::from(true)];

    for (publish, payload) in published.iter().zip(expected_payloads.into_iter()) {
        assert_eq!(publish.topic(), "topic");
        let payload_bytes = format!("{}", print_recon_compact(&payload)).into_bytes();
        assert_eq!(publish.payload(), &payload_bytes);
    }
}
