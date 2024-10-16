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

use crate::ingress::{FluvioClient, FluvioConsumer, FluvioRecord};
use crate::{ErrorCode, FluvioConnectorError, FluvioIngressConfiguration, FluvioIngressConnector};
use fluvio::dataplane::types::PartitionId;
use fluvio::Offset;
use futures::future::join;
use futures::TryStreamExt;
use rand::Rng;
use std::collections::{HashMap, VecDeque};
use std::future::{ready, Future};
use std::time::Duration;
use swimos_agent::agent_model::AgentSpec;
use swimos_agent::agent_model::{ItemDescriptor, ItemFlags};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::config::format::DataFormat;
use swimos_connector::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use swimos_connector::{BaseConnector, ConnectorAgent, IngressConnector, IngressContext};
use swimos_connector_util::run_handler_with_futures;
use swimos_model::{Item, Value};
use swimos_recon::print_recon_compact;
use swimos_utilities::trigger;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct TestIngressContext {
    requests: Vec<(String, WarpLaneKind)>,
}

impl IngressContext for TestIngressContext {
    fn open_lane(&mut self, name: &str, kind: WarpLaneKind) {
        self.requests.push((name.to_string(), kind));
    }
}

fn make_config() -> FluvioIngressConfiguration {
    FluvioIngressConfiguration {
        topic: "topic".to_string(),
        fluvio: fluvio::FluvioConfig::load().unwrap(),
        partition: 0,
        offset: Offset::beginning(),
        value_lanes: vec![IngressValueLaneSpec::new(None, "$key", true)],
        map_lanes: vec![IngressMapLaneSpec::new(
            "map",
            "$payload.key",
            "$payload.value",
            true,
            true,
        )],
        key_deserializer: DataFormat::Recon,
        payload_deserializer: DataFormat::Recon,
        relays: Default::default(),
    }
}

#[derive(Debug, Clone)]
struct TestRecord {
    offset: i64,
    partition_id: PartitionId,
    key: Vec<u8>,
    key_v: Value,
    value: Vec<u8>,
    value_v: Value,
}

impl FluvioRecord for TestRecord {
    fn offset(&self) -> i64 {
        self.offset
    }

    fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    fn key(&self) -> &[u8] {
        self.key.as_slice()
    }

    fn value(&self) -> &[u8] {
        self.value.as_slice()
    }
}

#[derive(Clone)]
struct FnProducer<F>(F);

impl<F> FnProducer<F> {
    fn new(funky: F) -> Self
    where
        F: Fn() -> VecDeque<Result<TestRecord, FluvioConnectorError>> + Send + 'static,
    {
        FnProducer(funky)
    }
}

impl<F> FluvioConsumer for FnProducer<F>
where
    F: Fn() -> VecDeque<Result<TestRecord, FluvioConnectorError>> + Send + Clone + 'static,
{
    type Client = VecClient;

    fn open(
        &self,
        _config: FluvioIngressConfiguration,
    ) -> impl Future<Output = Result<Self::Client, FluvioConnectorError>> + Send {
        ready(Ok(VecClient(self.0())))
    }
}

#[derive(Default)]
struct VecClient(VecDeque<Result<TestRecord, FluvioConnectorError>>);

impl FluvioClient for VecClient {
    type FluvioRecord = TestRecord;

    fn next(
        &mut self,
    ) -> impl Future<Output = Option<Result<Self::FluvioRecord, FluvioConnectorError>>> + Send {
        ready(self.0.pop_front())
    }

    fn shutdown(&mut self) -> impl Future<Output = ()> + Send {
        ready(())
    }
}

#[test]
fn connector_initialize() {
    let mut context = TestIngressContext::default();
    let config = make_config();
    let factory = FnProducer::new(VecDeque::default);
    let connector = FluvioIngressConnector::new(config, factory);
    assert!(connector.initialize(&mut context).is_ok());
    let requests = context.requests;
    assert_eq!(requests.len(), 2);

    let lane_map = requests.into_iter().collect::<HashMap<_, _>>();
    let lanes_expected = [
        ("key".to_string(), WarpLaneKind::Value),
        ("map".to_string(), WarpLaneKind::Map),
    ]
    .into_iter()
    .collect::<HashMap<_, _>>();
    assert_eq!(lane_map, lanes_expected);
}

fn make_key_value(key: impl Into<Value>, value: impl Into<Value>) -> Value {
    Value::record(vec![Item::slot("key", key), Item::slot("value", value)])
}

fn generate_messages(n: usize) -> Vec<TestRecord> {
    let mut rng = rand::thread_rng();
    let mut messages = Vec::with_capacity(n);

    for i in 0..n {
        let key = Value::from(rng.r#gen::<i32>());
        let payload_value = rng.r#gen::<u64>();
        let payload = make_key_value(key.clone(), payload_value);

        let message = TestRecord {
            offset: i as i64,
            partition_id: 0,
            key: format!("{}", print_recon_compact(&key)).as_bytes().to_vec(),
            key_v: key,
            value: format!("{}", print_recon_compact(&payload))
                .as_bytes()
                .to_vec(),
            value_v: Value::from(payload_value),
        };
        messages.push(message);
    }
    messages
}

#[tokio::test]
async fn connector_on_start() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let num_messages = 3;
        let messages = generate_messages(num_messages);
        let config = make_config();
        let factory = FnProducer::new(move || {
            messages
                .clone()
                .into_iter()
                .map(Ok)
                .collect::<VecDeque<_>>()
        });
        let connector = FluvioIngressConnector::new(config, factory);

        let (tx, rx) = trigger::trigger();
        let handler = connector.on_start(tx);

        let agent = ConnectorAgent::default();
        let start_task = run_handler_with_futures(&agent, handler);

        let (modified, result) = join(start_task, rx).await;
        assert!(result.is_ok());
        assert!(modified.is_empty());
    })
    .await
    .expect("Test timed out.");
}

async fn init_agent<F>(connector: &FluvioIngressConnector<F>, agent: &ConnectorAgent)
where
    F: FluvioConsumer,
{
    let mut context = TestIngressContext::default();
    assert!(connector.initialize(&mut context).is_ok());

    for (name, kind) in context.requests {
        assert!(agent
            .register_dynamic_item(
                &name,
                ItemDescriptor::WarpLane {
                    kind,
                    flags: ItemFlags::TRANSIENT
                }
            )
            .is_ok());
    }

    let (tx, rx) = trigger::trigger();
    let handler = connector.on_start(tx);
    let start_task = run_handler_with_futures(agent, handler);
    let (_, result) = join(start_task, rx).await;

    assert!(result.is_ok());
}

#[derive(Default)]
struct MessageChecker {
    expected_map: HashMap<Value, Value>,
}

impl MessageChecker {
    fn check_message(&mut self, agent: &mut ConnectorAgent, message: &TestRecord) {
        let MessageChecker { expected_map } = self;
        let TestRecord { key_v, value_v, .. } = message;
        let guard = agent.value_lane("key").expect("Lane missing.");
        guard.read(|v| {
            assert_eq!(v, key_v);
        });
        drop(guard);
        let guard = agent.map_lane("map").expect("Lane missing.");
        expected_map.insert(key_v.clone(), value_v.clone());
        guard.get_map(|map| {
            assert_eq!(map, expected_map);
        });
    }
}

#[tokio::test]
async fn connector_stream() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let num_messages = 3;
        let messages = generate_messages(num_messages);
        let config = make_config();
        let producer_messages = messages.clone();
        let factory = FnProducer::new(move || {
            producer_messages
                .clone()
                .into_iter()
                .map(Ok)
                .collect::<VecDeque<_>>()
        });
        let connector = FluvioIngressConnector::new(config, factory);

        let mut agent = ConnectorAgent::default();
        init_agent(&connector, &agent).await;

        let mut stream = connector.create_stream().expect("Connector failed.");

        let mut checker = MessageChecker::default();
        for message in &messages {
            let handler = stream
                .try_next()
                .await
                .expect("Consumer failed.")
                .expect("Consumer terminated.");
            run_handler_with_futures(&agent, handler).await;
            checker.check_message(&mut agent, message);
        }
    })
    .await
    .expect("Test timed out.");
}

#[tokio::test]
async fn failed_connector_stream_start() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = make_config();
        let factory = FnProducer::new(move || {
            VecDeque::from([Err(FluvioConnectorError::Code(ErrorCode::TopicError))])
        });
        let connector = FluvioIngressConnector::new(config, factory);
        let agent = ConnectorAgent::default();
        init_agent(&connector, &agent).await;

        let item = connector
            .create_stream()
            .expect("Opening a Fluvio connector should be infallible")
            .try_next()
            .await;

        match item {
            Ok(Some(_)) | Ok(None) => {
                panic!("Connector should have yielded an error")
            }
            Err(e) => {
                assert!(matches!(
                    e,
                    FluvioConnectorError::Code(ErrorCode::TopicError)
                ));
            }
        }
    })
    .await
    .expect("Test timed out.");
}
