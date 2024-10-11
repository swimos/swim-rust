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

use std::future::Future;

use bytes::Bytes;
use futures::{stream::unfold, Stream};
use rumqttc::{
    AsyncClient, ClientError, ConnectionError, Event, EventLoop, Incoming, MqttOptions, Publish,
    QoS, SubscribeFilter,
};
use tracing::trace;

use crate::Subscription;

pub trait MqttSubscriber: Sized {
    fn subscribe(
        self,
        subscription: Subscription,
    ) -> impl Future<Output = Result<Self, ClientError>> + Send + 'static;
}

pub trait MqttMessage {
    fn topic(&self) -> &str;
    fn payload(&self) -> &[u8];
}

pub trait MqttConsumer<Message> {
    fn into_stream(self) -> impl Stream<Item = Result<Message, ConnectionError>> + Send + 'static;
}

pub trait MqttPublisher {
    fn publish(
        &self,
        topic: String,
        payload: Bytes,
        retain: bool,
    ) -> impl Future<Output = Result<(), ClientError>> + Send + 'static;
}

pub trait ConsumerFactory {
    type Message: MqttMessage;
    type Subscriber: MqttSubscriber + Send + 'static;
    type Consumer: MqttConsumer<Self::Message> + Send + 'static;

    fn create(&self, options: MqttOptions) -> (Self::Subscriber, Self::Consumer);
}

pub trait PublisherDriver {
    fn into_future(self) -> impl Future<Output = Result<(), ConnectionError>> + Send + 'static;
}

pub trait PublisherFactory {
    type Publisher: MqttPublisher + Send + 'static;
    type Driver: PublisherDriver + Send + 'static;

    fn create(&self, options: MqttOptions) -> (Self::Publisher, Self::Driver);
}

pub struct MqttFactory {
    channel_size: usize,
}

impl MqttFactory {
    pub fn new(channel_size: usize) -> Self {
        MqttFactory { channel_size }
    }
}

impl MqttMessage for Publish {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl MqttSubscriber for AsyncClient {
    async fn subscribe(self, subscription: Subscription) -> Result<Self, ClientError> {
        match subscription {
            Subscription::Topic(topic) => {
                AsyncClient::subscribe(&self, topic, QoS::AtLeastOnce).await?;
            }
            Subscription::Topics(topics) => {
                for topic in topics {
                    AsyncClient::subscribe(&self, topic, QoS::AtLeastOnce).await?;
                }
            }
            Subscription::Filters(filters) => {
                let sub_filters = filters
                    .into_iter()
                    .map(|s| SubscribeFilter::new(s, QoS::AtLeastOnce));
                AsyncClient::subscribe_many(&self, sub_filters).await?;
            }
        }
        Ok(self)
    }
}

impl MqttConsumer<Publish> for EventLoop {
    fn into_stream(self) -> impl Stream<Item = Result<Publish, ConnectionError>> + Send + 'static {
        unfold(self, |mut consumer| async move {
            let outcome = loop {
                match consumer.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(msg))) => break Some(Ok(msg)),
                    Err(ConnectionError::RequestsDone) => break None,
                    Err(err) => break Some(Err(err)),
                    _ => {}
                }
            };
            outcome.map(move |result| (result, consumer))
        })
    }
}

impl ConsumerFactory for MqttFactory {
    type Message = Publish;

    type Subscriber = AsyncClient;

    type Consumer = EventLoop;

    fn create(&self, options: MqttOptions) -> (Self::Subscriber, Self::Consumer) {
        AsyncClient::new(options, self.channel_size)
    }
}

impl PublisherFactory for MqttFactory {
    type Publisher = AsyncClient;
    type Driver = EventLoop;

    fn create(&self, options: MqttOptions) -> (Self::Publisher, Self::Driver) {
        let (client, event_loop) = AsyncClient::new(options, self.channel_size);
        (client, event_loop)
    }
}

pub struct ClientPublisher {
    client: AsyncClient,
    retain: bool,
}

impl MqttPublisher for AsyncClient {
    fn publish(
        &self,
        topic: String,
        payload: Bytes,
        retain: bool,
    ) -> impl Future<Output = Result<(), ClientError>> + Send + 'static {
        let cpy = self.clone();
        async move {
            cpy.publish_bytes(topic, QoS::AtLeastOnce, retain, payload)
                .await
        }
    }
}

impl PublisherDriver for EventLoop {
    fn into_future(self) -> impl Future<Output = Result<(), ConnectionError>> + Send + 'static {
        drive_events(self)
    }
}

async fn drive_events(mut event_loop: EventLoop) -> Result<(), ConnectionError> {
    loop {
        match event_loop.poll().await {
            Ok(Event::Outgoing(event)) => {
                trace!(event = ?event, "Outgoing MQTT event.");
            }
            Err(ConnectionError::RequestsDone) => break Ok(()),
            Err(err) => break Err(err),
            _ => {}
        }
    }
}
