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

use std::{cell::RefCell, future::Future, pin::pin, sync::Arc};

use bytes::Bytes;
use parking_lot::Mutex;
use rumqttc::{ClientError, ConnectionError, MqttOptions, Publish, Request, Transport};
use swimos_utilities::trigger;
use tokio::sync::mpsc::{self, error::SendError};

use crate::facade::{MqttPublisher, PublisherDriver, PublisherFactory};

pub struct MockFactory {
    stop: RefCell<Option<trigger::Receiver>>,
    inner: Arc<Mutex<Outputs>>,
    expected_opts: MqttOptions,
}

impl MockFactory {
    pub fn new(expected_opts: MqttOptions) -> Self {
        MockFactory {
            stop: Default::default(),
            inner: Default::default(),
            expected_opts,
        }
    }

    pub fn with_stop(&self) -> trigger::Sender {
        let (tx, rx) = trigger::trigger();
        *self.stop.borrow_mut() = Some(rx);
        tx
    }
}

#[derive(Default)]
pub struct Outputs {
    published: Vec<Publish>,
}

impl PublisherFactory for MockFactory {
    type Publisher = TestPublisher;

    type Driver = TestDriver;

    fn create(&self, options: MqttOptions) -> (Self::Publisher, Self::Driver) {
        let MockFactory {
            stop,
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
        let stop = self.stop.borrow_mut().take();
        let (tx, rx) = mpsc::channel(16);
        (
            TestPublisher { tx },
            TestDriver {
                stop,
                rx,
                inner: inner.clone(),
            },
        )
    }
}

#[derive(Clone)]
pub struct TestPublisher {
    tx: mpsc::Sender<Publish>,
}

impl MqttPublisher for TestPublisher {
    fn publish(
        self,
        topic: String,
        payload: Bytes,
        retain: bool,
    ) -> impl Future<Output = Result<(), ClientError>> + Send + 'static {
        let tx = self.tx.clone();
        let payload = payload.to_vec();
        let mut publish = Publish::new(topic, rumqttc::QoS::AtMostOnce, payload.to_vec());
        publish.retain = retain;
        async move {
            let result = tx.send(publish).await;
            result.map_err(|SendError(publish)| ClientError::Request(Request::Publish(publish)))
        }
    }
}

pub struct TestDriver {
    stop: Option<trigger::Receiver>,
    rx: mpsc::Receiver<Publish>,
    inner: Arc<Mutex<Outputs>>,
}

impl PublisherDriver for TestDriver {
    async fn into_future(self) -> Result<(), ConnectionError> {
        let TestDriver {
            stop,
            mut rx,
            inner,
        } = self;
        if let Some(stop_rx) = stop {
            let mut stop_rx = pin!(stop_rx);
            loop {
                let publish = tokio::select! {
                    _ = &mut stop_rx => break,
                    maybe_publish = rx.recv() => {
                        if let Some(publish) = maybe_publish {
                            publish
                        } else {
                            break;
                        }
                    }
                };
                inner.lock().published.push(publish);
            }
        } else {
            while let Some(publish) = rx.recv().await {
                inner.lock().published.push(publish);
            }
        }
        Ok(())
    }
}
