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
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};

use crate::config::KafkaLogLevel;
use futures::{Future, FutureExt};
use rdkafka::consumer::BaseConsumer;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::{KafkaError, KafkaResult},
    message::BorrowedMessage,
    producer::{DeliveryFuture, FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
    ClientConfig, ClientContext, Message, Statistics, TopicPartitionList,
};
use swimos_connector::deser::MessageView;
use tracing::{debug, error, info, warn};

pub trait KafkaMessage {
    fn view(&self) -> MessageView<'_>;
}

pub trait KafkaConsumer {
    type Msg<'a>: KafkaMessage + Send + 'a
    where
        Self: 'a;

    fn recv(&self) -> impl Future<Output = Result<Self::Msg<'_>, KafkaError>> + Send + '_;

    fn commit(&self, message: Self::Msg<'_>) -> Result<(), KafkaError>;
}

pub struct MessageFacade<M>(M);

impl<M> KafkaMessage for MessageFacade<M>
where
    M: Message,
{
    fn view(&self) -> MessageView<'_> {
        MessageView {
            topic: self.0.topic(),
            key: self.0.key().unwrap_or_default(),
            payload: self.0.payload().unwrap_or_default(),
        }
    }
}

impl KafkaConsumer for LoggingConsumer {
    type Msg<'a>
        = MessageFacade<BorrowedMessage<'a>>
    where
        Self: 'a;

    async fn recv(&self) -> Result<Self::Msg<'_>, KafkaError> {
        LoggingConsumer::recv(self).await.map(MessageFacade)
    }

    fn commit(&self, message: Self::Msg<'_>) -> Result<(), KafkaError> {
        self.commit_message(&message.0, CommitMode::Async)
    }
}

pub trait ConsumerFactory {
    type Consumer: KafkaConsumer + Send + Sync + 'static;

    fn create(
        &self,
        properties: &HashMap<String, String>,
        log_level: KafkaLogLevel,
        topics: &[&str],
    ) -> Result<Self::Consumer, KafkaError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KafkaFactory;

impl ConsumerFactory for KafkaFactory {
    type Consumer = LoggingConsumer;

    fn create(
        &self,
        properties: &HashMap<String, String>,
        log_level: KafkaLogLevel,
        topics: &[&str],
    ) -> Result<Self::Consumer, KafkaError> {
        let mut client_builder = ClientConfig::new();
        properties.iter().for_each(|(k, v)| {
            client_builder.set(k, v);
        });
        let consumer = client_builder
            .set_log_level(log_level.into())
            .create_with_context::<_, LoggingConsumer>(KafkaClientContext)?;
        consumer.subscribe(topics)?;
        Ok(consumer)
    }
}

pub struct KafkaClientContext;

impl ClientContext for KafkaClientContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                error!("Kafka Connector: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Warning => {
                warn!("Kafka Connector: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Notice => {
                info!("Kafka Connector: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Info => {
                info!("Kafka Connector: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => {
                debug!("Kafka Connector: {} {}", fac, log_message)
            }
        }
    }

    fn stats(&self, statistics: Statistics) {
        info!("Kafka Connector Statistics: {:?}", statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        error!("Kafka Connector: {}: {}", error, reason);
    }
}

impl ConsumerContext for KafkaClientContext {
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

pub type LoggingConsumer = StreamConsumer<KafkaClientContext>;

pub enum ProduceResult<F> {
    ResultFuture(F),
    QueueFull,
}

pub trait KafkaProducer {
    type Fut: Future<Output = Result<(), KafkaError>> + Send + 'static;

    fn send<'a>(
        &'a self,
        topic: &'a str,
        key: Option<&'a [u8]>,
        payload: &'a [u8],
    ) -> ProduceResult<Self::Fut>;
}

#[doc(hidden)]
pub struct Simplified(Option<Result<DeliveryFuture, KafkaError>>);

impl Future for Simplified {
    type Output = Result<(), KafkaError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_mut().get_mut().0.take().expect("Polled twice.") {
            Ok(mut fut) => match fut.poll_unpin(cx) {
                Poll::Ready(Ok(Ok(_))) => Poll::Ready(Ok(())),
                Poll::Ready(Ok(Err((err, _)))) => Poll::Ready(Err(err)),
                Poll::Ready(Err(_)) => Poll::Ready(Err(KafkaError::Canceled)),
                Poll::Pending => {
                    self.get_mut().0 = Some(Ok(fut));
                    Poll::Pending
                }
            },
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

impl KafkaProducer for FutureProducer<KafkaClientContext> {
    type Fut = Simplified;

    fn send<'a>(
        &'a self,
        topic: &'a str,
        key: Option<&'a [u8]>,
        payload: &'a [u8],
    ) -> ProduceResult<Self::Fut> {
        let mut record: FutureRecord<[u8], [u8]> = FutureRecord::to(topic);
        if let Some(key) = key {
            record = record.key(key);
        }
        record = record.payload(payload);
        let fut = match self.send_result(record) {
            Ok(fut) => Simplified(Some(Ok(fut))),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                return ProduceResult::QueueFull;
            }
            Err((err, _)) => Simplified(Some(Err(err))),
        };
        ProduceResult::ResultFuture(fut)
    }
}

pub trait ProducerFactory {
    type Producer: KafkaProducer + Clone + Send + Sync + 'static;

    fn create(
        &self,
        properties: &HashMap<String, String>,
        log_level: KafkaLogLevel,
    ) -> Result<Self::Producer, KafkaError>;
}

impl ProducerFactory for KafkaFactory {
    type Producer = FutureProducer<KafkaClientContext>;

    fn create(
        &self,
        properties: &HashMap<String, String>,
        log_level: KafkaLogLevel,
    ) -> Result<Self::Producer, KafkaError> {
        let mut client_builder = ClientConfig::new();
        properties.iter().for_each(|(k, v)| {
            client_builder.set(k, v);
        });
        let producer = client_builder
            .set_log_level(log_level.into())
            .create_with_context::<_, FutureProducer<KafkaClientContext>>(KafkaClientContext)?;
        Ok(producer)
    }
}
