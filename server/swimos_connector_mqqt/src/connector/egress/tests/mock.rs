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

use std::{future::Future, sync::Arc};

use bytes::Bytes;
use parking_lot::Mutex;
use rumqttc::{ClientError, ConnectionError, MqttOptions, Publish, Request, Transport};
use tokio::sync::mpsc::{self, error::SendError};

use crate::facade::{MqttPublisher, PublisherDriver, PublisherFactory};

pub struct MockFactory {
    inner: Arc<Mutex<Outputs>>,
    expected_opts: MqttOptions,
}

impl MockFactory {

    pub fn new(expected_opts: MqttOptions) -> Self {
        MockFactory {
            inner: Default::default(),
            expected_opts,
        }
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
        let (tx, rx) = mpsc::channel(16);
        (TestPublisher { tx }, TestDriver { rx, inner: inner.clone() })
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
    rx: mpsc::Receiver<Publish>,
    inner: Arc<Mutex<Outputs>>,
}

impl PublisherDriver for TestDriver {
    async fn into_future(self) -> Result<(), ConnectionError> {
        let TestDriver { mut rx, inner } = self;
        while let Some(publish) = rx.recv().await {
            inner.lock().published.push(publish);
        }
        Ok(())
    }
}