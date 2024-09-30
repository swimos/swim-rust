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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::{
    future::{join, BoxFuture},
    FutureExt, TryFutureExt,
};
use parking_lot::Mutex;
use rdkafka::error::KafkaError;
use swimos_api::address::Address;
use swimos_connector::{
    BaseConnector, ConnectorAgent, EgressConnector, EgressConnectorSender, MessageSource,
    SendResult,
};
use swimos_model::{Item, Value};
use swimos_recon::print_recon_compact;
use swimos_utilities::trigger;

use crate::{
    config::{
        EgressMapLaneSpec, EgressValueLaneSpec, KafkaEgressConfiguration, MapDownlinkSpec,
        TopicSpecifier, ValueDownlinkSpec,
    },
    connector::{
        egress::KafkaEgressConnector,
        test_util::{run_handler_with_futures, run_handler_with_futures_dl},
    },
    facade::{KafkaProducer, ProduceResult, ProducerFactory},
    DataFormat, DownlinkAddress, ExtractionSpec, KafkaLogLevel,
};

fn props() -> HashMap<String, String> {
    [("key".to_string(), "value".to_string())]
        .into_iter()
        .collect()
}

#[derive(Clone)]
struct MockFactory {
    is_busy: Arc<AtomicBool>,
    inner: Arc<Mutex<MockProducerInner>>,
}

impl MockFactory {
    fn take_messages(&self) -> Vec<Message> {
        let mut guard = self.inner.lock();
        std::mem::take(&mut guard.messages)
    }
}

impl MockFactory {
    pub fn new(is_busy: Arc<AtomicBool>) -> Self {
        MockFactory {
            is_busy,
            inner: Default::default(),
        }
    }
}

impl ProducerFactory for MockFactory {
    type Producer = MockProducer;

    fn create(
        &self,
        properties: &HashMap<String, String>,
        log_level: KafkaLogLevel,
    ) -> Result<Self::Producer, rdkafka::error::KafkaError> {
        assert_eq!(properties, &props());
        assert_eq!(log_level, KafkaLogLevel::Warning);
        Ok(MockProducer {
            is_busy: self.is_busy.clone(),
            inner: self.inner.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Message {
    topic: String,
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
}

#[derive(Default, Debug, PartialEq, Eq)]
struct MockProducerInner {
    messages: Vec<Message>,
}

#[derive(Debug, Clone)]
struct MockProducer {
    is_busy: Arc<AtomicBool>,
    inner: Arc<Mutex<MockProducerInner>>,
}

impl KafkaProducer for MockProducer {
    type Fut = BoxFuture<'static, Result<(), KafkaError>>;

    fn send<'a>(
        &'a self,
        topic: &'a str,
        key: Option<&'a [u8]>,
        payload: &'a [u8],
    ) -> ProduceResult<Self::Fut> {
        if self.is_busy.load(Ordering::SeqCst) {
            ProduceResult::QueueFull
        } else {
            let inner = self.inner.clone();
            let message = Message {
                topic: topic.to_owned(),
                key: key.map(ToOwned::to_owned),
                payload: payload.to_owned(),
            };
            ProduceResult::ResultFuture(
                async move {
                    let mut guard = inner.lock();
                    guard.messages.push(message);
                    Ok(())
                }
                .boxed(),
            )
        }
    }
}

const FIXED: &str = "fixed";

fn make_connector(
    is_busy: Arc<AtomicBool>,
    value_lanes: Vec<EgressValueLaneSpec>,
    map_lanes: Vec<EgressMapLaneSpec>,
    value_downlinks: Vec<ValueDownlinkSpec>,
    map_downlinks: Vec<MapDownlinkSpec>,
) -> (MockFactory, KafkaEgressConnector<MockFactory>) {
    let configuration = KafkaEgressConfiguration {
        properties: props(),
        log_level: KafkaLogLevel::Warning,
        key_serializer: DataFormat::Recon,
        payload_serializer: DataFormat::Recon,
        fixed_topic: Some(FIXED.to_string()),
        value_lanes,
        map_lanes,
        value_downlinks,
        map_downlinks,
        retry_timeout_ms: 5000,
    };
    let fac = MockFactory::new(is_busy);
    (fac.clone(), KafkaEgressConnector::new(fac, configuration))
}

const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";
const HOST: &str = "host";
const NODE1: &str = "/node1";
const NODE2: &str = "/node2";
const LANE: &str = "lane";

fn addr1() -> DownlinkAddress {
    DownlinkAddress {
        host: Some(HOST.to_string()),
        node: NODE1.to_string(),
        lane: LANE.to_string(),
    }
}

fn addr2() -> DownlinkAddress {
    DownlinkAddress {
        host: None,
        node: NODE2.to_string(),
        lane: LANE.to_string(),
    }
}

async fn init_agent(agent: &ConnectorAgent, connector: &KafkaEgressConnector<MockFactory>) {
    let (tx, rx) = trigger::trigger();
    let handler = connector.on_start(tx);
    let ((modified, downlinks), result) =
        join(run_handler_with_futures_dl(agent, handler), rx).await;
    assert!(modified.is_empty());
    assert!(result.is_ok());
    assert!(downlinks.is_empty());
}

#[tokio::test]
async fn connector_on_start() {
    let (_, connector) = make_connector(
        Default::default(),
        vec![EgressValueLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: ExtractionSpec::default(),
        }],
        vec![EgressMapLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: ExtractionSpec::default(),
        }],
        vec![ValueDownlinkSpec {
            address: addr1(),
            extractor: ExtractionSpec::default(),
        }],
        vec![MapDownlinkSpec {
            address: addr2(),
            extractor: ExtractionSpec::default(),
        }],
    );

    let agent = ConnectorAgent::default();

    init_agent(&agent, &connector).await;

    let expected_value_lanes = [VALUE_LANE.to_string()].into_iter().collect::<HashSet<_>>();
    let expected_map_lanes = [MAP_LANE.to_string()].into_iter().collect::<HashSet<_>>();

    assert_eq!(agent.value_lanes(), expected_value_lanes);
    assert_eq!(agent.map_lanes(), expected_map_lanes);
}

#[tokio::test]
async fn create_sender() {
    let (_, connector) = make_connector(
        Default::default(),
        vec![EgressValueLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: ExtractionSpec::default(),
        }],
        vec![EgressMapLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: ExtractionSpec::default(),
        }],
        vec![ValueDownlinkSpec {
            address: addr1(),
            extractor: ExtractionSpec::default(),
        }],
        vec![MapDownlinkSpec {
            address: addr2(),
            extractor: ExtractionSpec::default(),
        }],
    );

    let agent = ConnectorAgent::default();

    init_agent(&agent, &connector).await;

    assert!(connector.make_sender(&HashMap::new()).is_ok());
}

fn make_rec() -> Value {
    Value::from_vec(vec![Item::slot("first", 5), Item::slot("second", false)])
}

fn value_ext_spec() -> ExtractionSpec {
    ExtractionSpec {
        topic_specifier: TopicSpecifier::Fixed,
        key_selector: Some("$value.first".to_string()),
        payload_selector: None,
    }
}

fn map_ext_spec() -> ExtractionSpec {
    ExtractionSpec {
        topic_specifier: TopicSpecifier::Fixed,
        key_selector: Some("$key".to_string()),
        payload_selector: None,
    }
}

#[tokio::test]
async fn produce_message_from_value_lane() {
    let (factory, connector) = make_connector(
        Default::default(),
        vec![EgressValueLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: value_ext_spec(),
        }],
        vec![EgressMapLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: map_ext_spec(),
        }],
        vec![ValueDownlinkSpec {
            address: addr1(),
            extractor: value_ext_spec(),
        }],
        vec![MapDownlinkSpec {
            address: addr2(),
            extractor: map_ext_spec(),
        }],
    );

    let agent = ConnectorAgent::default();

    init_agent(&agent, &connector).await;

    let sender = connector
        .make_sender(&HashMap::new())
        .expect("Creating sender failed.");

    let value = make_rec();

    let result = sender
        .send(MessageSource::Lane(VALUE_LANE), None, &value)
        .expect("No result.");
    match result {
        SendResult::Suspend(fut) => {
            let h = fut.into_future().await.expect("Send failed.");
            assert!(run_handler_with_futures(&agent, h).await.is_empty());
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }

    let messages = factory.take_messages();

    let expected_key = format!("{}", print_recon_compact(&Value::from(5))).into_bytes();
    let expected_payload = format!("{}", print_recon_compact(&value)).into_bytes();

    assert_eq!(
        messages,
        vec![Message {
            topic: FIXED.to_string(),
            key: Some(expected_key),
            payload: expected_payload
        }]
    );
}

#[tokio::test]
async fn produce_message_from_map_lane() {
    let (factory, connector) = make_connector(
        Default::default(),
        vec![EgressValueLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: value_ext_spec(),
        }],
        vec![EgressMapLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: map_ext_spec(),
        }],
        vec![ValueDownlinkSpec {
            address: addr1(),
            extractor: value_ext_spec(),
        }],
        vec![MapDownlinkSpec {
            address: addr2(),
            extractor: map_ext_spec(),
        }],
    );

    let agent = ConnectorAgent::default();

    init_agent(&agent, &connector).await;

    let sender = connector
        .make_sender(&HashMap::new())
        .expect("Creating sender failed.");

    let key = Value::from("hello");
    let value = make_rec();

    let result = sender
        .send(MessageSource::Lane(MAP_LANE), Some(&key), &value)
        .expect("No result.");
    match result {
        SendResult::Suspend(fut) => {
            let h = fut.into_future().await.expect("Send failed.");
            assert!(run_handler_with_futures(&agent, h).await.is_empty());
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }

    let messages = factory.take_messages();

    let expected_key = format!("{}", print_recon_compact(&key)).into_bytes();
    let expected_payload = format!("{}", print_recon_compact(&value)).into_bytes();

    assert_eq!(
        messages,
        vec![Message {
            topic: FIXED.to_string(),
            key: Some(expected_key),
            payload: expected_payload
        }]
    );
}

#[tokio::test]
async fn produce_message_from_value_dl() {
    let addr = addr1();
    let target = Address::<String>::from(&addr);

    let (factory, connector) = make_connector(
        Default::default(),
        vec![EgressValueLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: value_ext_spec(),
        }],
        vec![EgressMapLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: map_ext_spec(),
        }],
        vec![ValueDownlinkSpec {
            address: addr,
            extractor: value_ext_spec(),
        }],
        vec![MapDownlinkSpec {
            address: addr2(),
            extractor: map_ext_spec(),
        }],
    );

    let agent = ConnectorAgent::default();

    init_agent(&agent, &connector).await;

    let sender = connector
        .make_sender(&HashMap::new())
        .expect("Creating sender failed.");

    let value = make_rec();

    let result = sender
        .send(MessageSource::Downlink(&target), None, &value)
        .expect("No result.");
    match result {
        SendResult::Suspend(fut) => {
            let h = fut.into_future().await.expect("Send failed.");
            assert!(run_handler_with_futures(&agent, h).await.is_empty());
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }

    let messages = factory.take_messages();

    let expected_key = format!("{}", print_recon_compact(&Value::from(5))).into_bytes();
    let expected_payload = format!("{}", print_recon_compact(&value)).into_bytes();

    assert_eq!(
        messages,
        vec![Message {
            topic: FIXED.to_string(),
            key: Some(expected_key),
            payload: expected_payload
        }]
    );
}

#[tokio::test]
async fn produce_message_from_map_dl() {
    let addr = addr2();
    let target = Address::<String>::from(&addr);

    let (factory, connector) = make_connector(
        Default::default(),
        vec![EgressValueLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: value_ext_spec(),
        }],
        vec![EgressMapLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: map_ext_spec(),
        }],
        vec![ValueDownlinkSpec {
            address: addr1(),
            extractor: value_ext_spec(),
        }],
        vec![MapDownlinkSpec {
            address: addr,
            extractor: map_ext_spec(),
        }],
    );

    let agent = ConnectorAgent::default();

    init_agent(&agent, &connector).await;

    let sender = connector
        .make_sender(&HashMap::new())
        .expect("Creating sender failed.");

    let key = Value::from("hello");
    let value = make_rec();

    let result = sender
        .send(MessageSource::Downlink(&target), Some(&key), &value)
        .expect("No result.");
    match result {
        SendResult::Suspend(fut) => {
            let h = fut.into_future().await.expect("Send failed.");
            assert!(run_handler_with_futures(&agent, h).await.is_empty());
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }

    let messages = factory.take_messages();

    let expected_key = format!("{}", print_recon_compact(&key)).into_bytes();
    let expected_payload = format!("{}", print_recon_compact(&value)).into_bytes();

    assert_eq!(
        messages,
        vec![Message {
            topic: FIXED.to_string(),
            key: Some(expected_key),
            payload: expected_payload
        }]
    );
}

#[tokio::test]
async fn produce_message_when_busy() {
    let is_busy = Arc::new(AtomicBool::new(true));
    let (factory, connector) = make_connector(
        is_busy.clone(),
        vec![EgressValueLaneSpec {
            name: VALUE_LANE.to_string(),
            extractor: value_ext_spec(),
        }],
        vec![EgressMapLaneSpec {
            name: MAP_LANE.to_string(),
            extractor: map_ext_spec(),
        }],
        vec![ValueDownlinkSpec {
            address: addr1(),
            extractor: value_ext_spec(),
        }],
        vec![MapDownlinkSpec {
            address: addr2(),
            extractor: map_ext_spec(),
        }],
    );

    let agent = ConnectorAgent::default();

    init_agent(&agent, &connector).await;

    let sender = connector
        .make_sender(&HashMap::new())
        .expect("Creating sender failed.");

    let value = make_rec();

    let result = sender
        .send(MessageSource::Lane(VALUE_LANE), None, &value)
        .expect("No result.");
    let id = match result {
        SendResult::RequestCallback(wait, id) => {
            assert_eq!(wait, Duration::from_secs(5));
            id
        }
        ow => panic!("Unexpected result: {:?}", ow),
    };

    let messages = factory.take_messages();
    assert!(messages.is_empty());

    is_busy.store(false, Ordering::SeqCst);

    let result = sender.timer_event(id).expect("No result.");

    match result {
        SendResult::Suspend(fut) => {
            let h = fut.into_future().await.expect("Send failed.");
            assert!(run_handler_with_futures(&agent, h).await.is_empty());
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }

    let messages = factory.take_messages();

    let expected_key = format!("{}", print_recon_compact(&Value::from(5))).into_bytes();
    let expected_payload = format!("{}", print_recon_compact(&value)).into_bytes();

    assert_eq!(
        messages,
        vec![Message {
            topic: FIXED.to_string(),
            key: Some(expected_key),
            payload: expected_payload
        }]
    );
}
