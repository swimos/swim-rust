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

use futures::{stream::unfold, Stream};
use rumqttc::{
    AsyncClient, ClientError, ConnectionError, Event, EventLoop, Incoming, MqttOptions, Publish, QoS, SubscribeFilter
};

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
    fn into_stream(
        self,
    ) -> impl Stream<Item = Result<Message, ConnectionError>> + Send + 'static;
}

pub trait ConsumerFactory {
    type Message: MqttMessage;
    type Subscriber: MqttSubscriber;
    type Consumer: MqttConsumer<Self::Message>;

    fn create(&self, options: MqttOptions) -> (Self::Subscriber, Self::Consumer);
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

pub struct AsyncSubscriber {
    client: AsyncClient,
}

pub struct EventLoopConsumer {
    event_loop: EventLoop,
}

impl MqttSubscriber for AsyncSubscriber {
    async fn subscribe(self, subscription: Subscription) -> Result<Self, ClientError> {
        let AsyncSubscriber { client } = &self;
        match subscription {
            Subscription::Topic(topic) => {
                client.subscribe(topic, QoS::AtLeastOnce).await?;
            }
            Subscription::Topics(topics) => {
                for topic in topics {
                    client.subscribe(topic, QoS::AtLeastOnce).await?;
                }
            }
            Subscription::Filters(filters) => {
                let sub_filters = filters
                    .into_iter()
                    .map(|s| SubscribeFilter::new(s, QoS::AtLeastOnce));
                client.subscribe_many(sub_filters).await?;
            }
        }
        Ok(self)
    }
}

impl MqttConsumer<Publish> for EventLoopConsumer {
    fn into_stream(
        self,
    ) -> impl Stream<Item = Result<Publish, ConnectionError>> + Send + 'static {
        unfold(self, |mut consumer| async move {
            let outcome = loop {
                match consumer.event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(msg))) => break Some(Ok(msg)),
                    Err(ConnectionError::RequestsDone) => break None,
                    Err(err) => break Some(Err(err)),
                    _ => {},
                }
            };
            outcome.map(move |result| (result, consumer))
        })
    }
}

impl ConsumerFactory for MqttFactory {
    type Message = Publish;

    type Subscriber = AsyncSubscriber;

    type Consumer = EventLoopConsumer;

    fn create(&self, options: MqttOptions) -> (Self::Subscriber, Self::Consumer) {
        let (client, event_loop) = AsyncClient::new(options, self.channel_size);
        let sub = AsyncSubscriber { client };
        let con = EventLoopConsumer { event_loop };
        (sub, con)
    }
}
