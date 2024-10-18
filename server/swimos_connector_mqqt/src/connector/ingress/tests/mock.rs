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

use std::{collections::HashMap, sync::Arc};

use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rumqttc::{ClientError, ConnectionError, MqttOptions, Transport};
use swimos_utilities::trigger;

use crate::{
    facade::{ConsumerFactory, MqttConsumer, MqttMessage, MqttSubscriber},
    Subscription,
};

#[derive(Clone)]
pub struct MockFactory {
    inner: Arc<Mutex<Inner>>,
    expected_opts: MqttOptions,
}

impl MockFactory {
    pub fn new(messages: HashMap<String, Vec<String>>, expected_opts: MqttOptions) -> Self {
        MockFactory {
            inner: Arc::new(Mutex::new(Inner {
                messages,
                subscribed: Default::default(),
            })),
            expected_opts,
        }
    }
}

struct Inner {
    messages: HashMap<String, Vec<String>>,
    subscribed: HashMap<String, Vec<String>>,
}

impl ConsumerFactory for MockFactory {
    type Message = TestMessage;

    type Subscriber = TestSubscriber;

    type Consumer = TestConsumer;

    fn create(&self, options: MqttOptions) -> (Self::Subscriber, Self::Consumer) {
        let MockFactory {
            inner,
            expected_opts,
        } = self;
        assert_eq!(options.inflight(), expected_opts.inflight());
        assert_eq!(options.keep_alive(), expected_opts.keep_alive());
        assert_eq!(options.manual_acks(), expected_opts.manual_acks());
        assert_eq!(options.max_packet_size(), expected_opts.max_packet_size());
        assert_eq!(options.credentials(), expected_opts.credentials());
        assert_eq!(
            options.request_channel_capacity(),
            expected_opts.request_channel_capacity()
        );
        assert_eq!(options.clean_session(), expected_opts.clean_session());
        assert_eq!(options.client_id(), expected_opts.client_id());
        assert_eq!(options.broker_address(), expected_opts.broker_address());
        match (options.transport(), expected_opts.transport()) {
            (Transport::Tls(_), Transport::Tls(_)) => {}
            (Transport::Tcp, Transport::Tcp) => {}
            (Transport::Unix, Transport::Unix) => {}
            _ => panic!("Transports do not match."),
        }
        let (subscribed_tx, subscribed_rx) = trigger::trigger();
        (
            TestSubscriber {
                subscribed: Some(subscribed_tx),
                inner: inner.clone(),
            },
            TestConsumer {
                subscribed: Some(subscribed_rx),
                inner: inner.clone(),
            },
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

pub struct TestSubscriber {
    subscribed: Option<trigger::Sender>,
    inner: Arc<Mutex<Inner>>,
}

impl MqttSubscriber for TestSubscriber {
    async fn subscribe(mut self, subscription: Subscription) -> Result<Self, ClientError> {
        let TestSubscriber {
            subscribed: tx,
            inner,
        } = &mut self;
        let mut guard = inner.lock();
        let Inner {
            messages,
            subscribed,
        } = &mut *guard;
        let pred = to_pred(subscription);
        for (topic, messages) in messages.iter_mut() {
            if pred(topic) {
                subscribed.insert(topic.clone(), std::mem::take(messages));
            }
        }
        for topic in subscribed.keys() {
            messages.remove(topic);
        }
        drop(guard);
        if let Some(tx) = tx.take() {
            tx.trigger();
        }
        Ok(self)
    }
}

fn to_pred(sub: Subscription) -> Box<dyn Fn(&str) -> bool> {
    match sub {
        Subscription::Topic(t) => Box::new(move |topic| topic == t),
        Subscription::Topics(topics) => {
            Box::new(move |topic| topics.iter().any(|t| t.as_str() == topic))
        }
        Subscription::Filters(_) => Box::new(move |topic| topic == "filtered"),
    }
}

pub struct TestConsumer {
    subscribed: Option<trigger::Receiver>,
    inner: Arc<Mutex<Inner>>,
}

impl MqttConsumer<TestMessage> for TestConsumer {
    fn into_stream(
        self,
    ) -> impl Stream<Item = Result<TestMessage, ConnectionError>> + Send + 'static {
        let messages_fut = async move {
            let TestConsumer {
                mut subscribed,
                inner,
            } = self;
            if let Some(rx) = subscribed.take() {
                assert!(rx.await.is_ok());
            }
            let guard = inner.lock();
            let mut messages = vec![];
            let mut rng = rand::thread_rng();

            for (topic, msgs) in &guard.subscribed {
                for msg in msgs {
                    messages.push(TestMessage::new(topic, msg));
                }
            }
            messages.shuffle(&mut rng);
            futures::stream::iter(messages.into_iter().map(Ok))
        };
        futures::stream::once(messages_fut).flatten()
    }
}
