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
    sync::Arc,
};

use futures::future::join;
use parking_lot::Mutex;
use rand::{rngs::ThreadRng, Rng};
use rdkafka::error::KafkaError;
use swimos_connector::ConnectorAgent;
use swimos_model::{Item, Value};
use swimos_recon::print_recon_compact;
use swimos_utilities::trigger;
use tokio::sync::mpsc;

use crate::{
    config::KafkaLogLevel,
    connector::{message_to_handler, Lanes, MessageSelector, MessageState},
    deser::{MessageDeserializer, MessageView, ReconDeserializer},
    facade::{KafkaConsumer, KafkaMessage},
    DeserializationFormat, KafkaConnectorConfiguration, MapLaneSpec, ValueLaneSpec,
};

use super::{run_handler_with_futures, setup_agent};

fn make_config() -> KafkaConnectorConfiguration {
    KafkaConnectorConfiguration {
        properties: HashMap::new(),
        log_level: KafkaLogLevel::Warning,
        value_lanes: vec![ValueLaneSpec::new(None, "$key", true)],
        map_lanes: vec![MapLaneSpec::new(
            "map",
            "$payload.key",
            "$payload.value",
            true,
            true,
        )],
        key_deserializer: DeserializationFormat::Recon,
        value_deserializer: DeserializationFormat::Recon,
    }
}

#[derive(Default)]
struct MockConsumerInner {
    messages: Vec<MockMessage>,
    message_index: usize,
    recv_error: Option<KafkaError>,
    commit_errors: HashMap<String, KafkaError>,
}

impl MockConsumerInner {
    fn take_message(&mut self) -> Option<Result<IndexedMockMessage, KafkaError>> {
        let MockConsumerInner {
            messages,
            message_index,
            recv_error,
            ..
        } = self;
        let i = *message_index;
        *message_index += 1;
        if let Some(msg) = messages.get(i).cloned() {
            Some(Ok(IndexedMockMessage {
                message: msg,
                index: i,
            }))
        } else {
            recv_error.take().map(Err)
        }
    }

    fn commit(&mut self, message: IndexedMockMessage) -> Result<(), KafkaError> {
        let MockConsumerInner {
            message_index,
            commit_errors,
            ..
        } = self;
        if let Some(err) = commit_errors.remove(message.message.key_str.as_str()) {
            Err(err)
        } else {
            assert_eq!(*message_index, message.index + 1);
            Ok(())
        }
    }
}

#[derive(Default)]
pub struct MockConsumer {
    inner: Arc<Mutex<MockConsumerInner>>,
}

impl MockConsumer {
    fn new(messages: Vec<MockMessage>) -> Self {
        let inner = MockConsumerInner {
            messages,
            message_index: 0,
            recv_error: None,
            commit_errors: HashMap::new(),
        };
        MockConsumer {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MockMessage {
    topic: String,
    key: Value,
    key_str: String,
    payload_key: Value,
    payload_value: Value,
    payload_str: String,
}

#[derive(Clone, Debug)]
pub struct IndexedMockMessage {
    message: MockMessage,
    index: usize,
}

impl KafkaMessage for IndexedMockMessage {
    fn view(&self) -> MessageView<'_> {
        let MockMessage {
            topic,
            key_str,
            payload_str,
            ..
        } = &self.message;
        MessageView {
            topic: topic.as_str(),
            key: key_str.as_bytes(),
            payload: payload_str.as_bytes(),
        }
    }
}

impl KafkaConsumer for MockConsumer {
    type Msg<'a> = IndexedMockMessage
    where
        Self: 'a;

    async fn recv(&self) -> Result<Self::Msg<'_>, KafkaError> {
        let msg = self.inner.lock().take_message();
        match msg {
            Some(result) => result,
            _ => std::future::pending().await,
        }
    }

    fn commit(&self, message: Self::Msg<'_>) -> Result<(), KafkaError> {
        self.inner.lock().commit(message)
    }
}

fn make_key_value(key: impl Into<Value>, value: impl Into<Value>) -> Value {
    Value::record(vec![Item::slot("key", key), Item::slot("value", value)])
}

fn generate_string(len: usize, rng: &mut ThreadRng) -> String {
    let mut string = String::with_capacity(len);
    for _ in 0..len {
        string.push(rng.gen_range('A'..='Z'));
    }
    string
}

fn generate_messages(n: usize, topic: &str) -> Vec<MockMessage> {
    let mut rng = rand::thread_rng();
    let mut messages = Vec::with_capacity(n);
    for _ in 0..n {
        let key = Value::from(rng.r#gen::<i32>());
        let payload_key_len = rng.gen_range(5..10);
        let payload_key = generate_string(payload_key_len, &mut rng);
        let payload_value = rng.r#gen::<u64>();
        let payload = make_key_value(payload_key.clone(), payload_value);

        let key_str = format!("{}", print_recon_compact(&key));
        let payload_str = format!("{}", print_recon_compact(&payload));
        let message = MockMessage {
            topic: topic.to_string(),
            key,
            key_str,
            payload_key: Value::from(payload_key),
            payload_value: Value::from(payload_value),
            payload_str,
        };
        messages.push(message);
    }
    messages
}

#[tokio::test]
async fn message_state() {
    let num_messages = 3;
    let messages = generate_messages(num_messages, "topic_name");
    let mock_consumer = MockConsumer::new(messages.clone());

    let (mut agent, ids) = setup_agent();
    let id_set = ids.values().copied().collect::<HashSet<_>>();
    let value_specs = vec![ValueLaneSpec::new(None, "$key", true)];
    let map_specs = vec![MapLaneSpec::new(
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

    let (tx, mut rx) = mpsc::channel(1);
    let message_state = MessageState::new(mock_consumer, selector, message_to_handler, tx);

    let (stop_tx, stop_rx) = trigger::trigger();
    let consume_task = message_state.consume_messages(Some(stop_rx));

    let handler_task = async move {
        let mut checker = MessageChecker::default();
        for message in &messages {
            let handler = rx.recv().await.expect("Consumer failed.");
            let modifications = run_handler_with_futures(&agent, handler).await;
            assert_eq!(modifications, id_set);
            checker.check_message(&mut agent, message);
        }
        stop_tx.trigger();
    };

    let (result, _) = join(consume_task, handler_task).await;
    assert!(result.is_ok());
}

#[derive(Default)]
struct MessageChecker {
    expected_map: HashMap<Value, Value>,
}

impl MessageChecker {
    fn check_message(&mut self, agent: &mut ConnectorAgent, message: &MockMessage) {
        let MessageChecker { expected_map } = self;
        let MockMessage {
            key,
            payload_key,
            payload_value,
            ..
        } = message;
        let guard = agent.value_lane("key").expect("Lane missing.");
        guard.read(|v| {
            assert_eq!(v, key);
        });
        drop(guard);
        let guard = agent.map_lane("map").expect("Lane missing.");
        expected_map.insert(payload_key.clone(), payload_value.clone());
        guard.get_map(|map| {
            assert_eq!(map, expected_map);
        });
    }
}
