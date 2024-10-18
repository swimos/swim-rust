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
    collections::{HashMap, HashSet},
    time::Duration,
};

use futures::{future::join, TryStreamExt};
use rumqttc::MqttOptions;
use swimos_agent::agent_model::{AgentSpec, ItemDescriptor, ItemFlags};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::{
    config::{format::DataFormat, IngressMapLaneSpec, IngressValueLaneSpec},
    BaseConnector, ConnectorAgent, IngressConnector, IngressContext,
};
use swimos_model::Value;
use swimos_utilities::trigger;

use crate::{
    config::Credentials, connector::test_util::run_handler_with_futures, MqttIngressConfiguration,
    MqttIngressConnector, Subscription,
};

use super::mock::MockFactory;

const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";
const TOPIC: &str = "topic";

fn make_config() -> MqttIngressConfiguration {
    MqttIngressConfiguration {
        url: "mqtt://localhost:8080?client_id=exampleid".to_string(),
        value_lanes: vec![IngressValueLaneSpec::new(
            Some(VALUE_LANE),
            "$payload",
            true,
        )],
        map_lanes: vec![IngressMapLaneSpec::new(
            MAP_LANE, "$topic", "$payload", false, true,
        )],
        payload_deserializer: DataFormat::Recon,
        subscription: Subscription::Topic("topic".to_string()),
        keep_alive_secs: Some(30),
        max_packet_size: Some(4096),
        channel_size: Some(8),
        credentials: Some(Credentials {
            username: "mqtt_user".to_string(),
            password: "passw0rd".to_string(),
        }),
    }
}

fn make_expected_opts() -> MqttOptions {
    let mut options = MqttOptions::new("exampleid", "localhost", 8080);
    options.set_keep_alive(Duration::from_secs(30));
    options.set_max_packet_size(4096, 4096);
    options.set_request_channel_capacity(8);
    options.set_credentials("mqtt_user", "passw0rd");
    options
}

#[derive(Default)]
struct TestContext {
    lanes: Vec<(String, WarpLaneKind)>,
}

impl IngressContext for TestContext {
    fn open_lane(&mut self, name: &str, kind: WarpLaneKind) {
        self.lanes.push((name.to_string(), kind));
    }
}

#[test]
fn initialize_connector() {
    let factory = MockFactory::new(Default::default(), make_expected_opts());
    let connector = MqttIngressConnector::new(factory, make_config());
    let mut context = TestContext::default();
    assert!(connector.initialize(&mut context).is_ok());

    let guard = connector.lanes.borrow();
    assert_eq!(guard.value_lanes().len(), 1);
    assert_eq!(guard.map_lanes().len(), 1);

    let TestContext { lanes } = context;
    assert_eq!(lanes.len(), 2);

    let lanes: HashMap<_, _> = lanes.into_iter().collect();
    let expected_lanes: HashMap<_, _> = [
        (VALUE_LANE.to_string(), WarpLaneKind::Value),
        (MAP_LANE.to_string(), WarpLaneKind::Map),
    ]
    .into_iter()
    .collect();
    assert_eq!(lanes, expected_lanes);
}

fn create_agent() -> ConnectorAgent {
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

async fn init_and_start(
    config: MqttIngressConfiguration,
    factory: MockFactory,
) -> (ConnectorAgent, MqttIngressConnector<MockFactory>) {
    let connector = MqttIngressConnector::new(factory, config);
    let mut context = TestContext::default();
    assert!(connector.initialize(&mut context).is_ok());

    let agent = create_agent();

    let (init_tx, init_rx) = trigger::trigger();
    let handler = connector.on_start(init_tx);

    let on_start_task = run_handler_with_futures(&agent, handler);

    let (modified, res) = tokio::time::timeout(TEST_TIMEOUT, join(on_start_task, init_rx))
        .await
        .expect("Timed out.");
    assert!(modified.is_empty());
    assert!(res.is_ok());

    (agent, connector)
}

#[tokio::test]
async fn start_connector() {
    let factory = MockFactory::new(Default::default(), make_expected_opts());
    init_and_start(make_config(), factory).await;
}

fn make_simple_config() -> MqttIngressConfiguration {
    MqttIngressConfiguration {
        url: "mqtt://localhost:8080?client_id=exampleid".to_string(),
        value_lanes: vec![IngressValueLaneSpec::new(
            Some(VALUE_LANE),
            "$payload",
            true,
        )],
        map_lanes: vec![],
        payload_deserializer: DataFormat::Recon,
        subscription: Subscription::Topic("topic".to_string()),
        keep_alive_secs: Some(30),
        max_packet_size: Some(4096),
        channel_size: Some(8),
        credentials: Some(Credentials {
            username: "mqtt_user".to_string(),
            password: "passw0rd".to_string(),
        }),
    }
}

#[tokio::test]
async fn run_connector_stream() {
    let mut messages = HashMap::new();
    messages.insert(
        TOPIC.to_string(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
    );
    messages.insert("other".to_string(), vec!["d".to_string()]);

    let factory = MockFactory::new(messages, make_expected_opts());
    let test_case = async move {
        let (mut agent, connector) = init_and_start(make_simple_config(), factory).await;

        let mut stream = connector.create_stream().expect("Loading stream failed.");

        let mut values = vec![];
        while let Some(handler) = stream
            .try_next()
            .await
            .expect("Getting next message failed.")
        {
            assert_eq!(run_handler_with_futures(&agent, handler).await.len(), 1);
            values.push(get_value(&mut agent));
        }

        assert_eq!(values.len(), 3);
        let value_set: HashSet<_> = values.into_iter().collect();
        let expected_values: HashSet<_> = ["a", "b", "c"].into_iter().map(Value::text).collect();
        assert_eq!(value_set, expected_values);
    };
    tokio::time::timeout(TEST_TIMEOUT, test_case)
        .await
        .expect("Timed out.");
}

fn get_value(agent: &mut ConnectorAgent) -> Value {
    let guard = agent.value_lane(VALUE_LANE).expect("Lane missing.");
    guard.read(|v| v.clone())
}
