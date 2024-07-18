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

pub mod config;
pub mod deser;
pub mod selector;

use std::collections::HashMap;

use config::KafkaConnectorConfiguration;
use futures::{stream::unfold, Future};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::{KafkaError, KafkaResult},
    message::BorrowedMessage,
    ClientConfig, ClientContext, Message, Statistics, TopicPartitionList,
};
use swimos_agent::{
    agent_lifecycle::{ConnectorContext, HandlerContext},
    event_handler::{EventHandler, HandlerActionExt, TryHandlerActionExt, UnitHandler},
};
use swimos_model::Value;
use swimos_utilities::trigger;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use swimos_connector::{Connector, ConnectorStream, GenericConnectorAgent, ValueLaneSelectorFn};

type ConnHandlerContext = HandlerContext<GenericConnectorAgent>;
type ConnContext = ConnectorContext<GenericConnectorAgent>;

pub struct KafkaConnector {
    configuration: KafkaConnectorConfiguration,
}

impl KafkaConnector {
    pub fn new(configuration: KafkaConnectorConfiguration) -> Self {
        KafkaConnector { configuration }
    }
}

type LoggingConsumer = StreamConsumer<KafkaClientContext>;

impl Connector for KafkaConnector {
    type StreamError = KafkaError;

    fn on_start(
        &self,
        init_complete: trigger::Sender,
    ) -> impl EventHandler<GenericConnectorAgent> + '_ {
        let context: ConnContext = ConnContext::default();

        context.open_value_lane(BODY_LANE, |result| {
            let handler_context = ConnHandlerContext::default();
            handler_context
                .value(result)
                .try_handler()
                .followed_by(handler_context.effect(move || {
                    init_complete.trigger();
                }))
        });
        UnitHandler::default()
    }

    fn on_stop(&self) -> impl EventHandler<GenericConnectorAgent> + '_ {
        UnitHandler::default()
    }

    fn create_stream(&self) -> Result<impl ConnectorStream<KafkaError>, Self::StreamError> {
        let KafkaConnector { configuration } = self;
        let mut client_builder = ClientConfig::new();
        configuration.properties.iter().for_each(|(k, v)| {
            client_builder.set(k, v);
        });
        let consumer = client_builder
            .set_log_level(configuration.log_level)
            .create_with_context::<_, LoggingConsumer>(KafkaClientContext)?;
        let (tx, rx) = mpsc::channel(1);
        let state = MessageState::new(consumer, message_to_handler, tx);
        let consumer_task = Box::pin(state.consume_messages());
        let stream_src = MessageTasks::new(consumer_task, rx);
        Ok(stream_src.into_stream())
    }
}

struct KafkaClientContext;

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
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

const BODY_LANE: &str = "body";

fn message_to_handler<'a>(
    message: &'a BorrowedMessage<'a>,
    trigger_tx: trigger::Sender,
) -> impl EventHandler<GenericConnectorAgent> + Send + 'static {
    let context: ConnectorContext<GenericConnectorAgent> = ConnectorContext::default();
    let handler_context: HandlerContext<GenericConnectorAgent> = HandlerContext::default();
    let set_body = if let Some(Ok(body)) = message.payload_view::<str>() {
        Some(context.set_value(
            ValueLaneSelectorFn::new(BODY_LANE.to_string()),
            Value::from(body),
        ))
    } else {
        None
    };
    set_body.followed_by(handler_context.effect(move || {
        trigger_tx.trigger();
    }))
}

struct MessageState<F, H> {
    consumer: LoggingConsumer,
    to_handler: F,
    tx: mpsc::Sender<H>,
}

impl<F, H> MessageState<F, H> {
    fn new(consumer: LoggingConsumer, to_handler: F, tx: mpsc::Sender<H>) -> Self {
        MessageState {
            consumer,
            to_handler,
            tx,
        }
    }
}

impl<F, H> MessageState<F, H>
where
    F: for<'a> Fn(&'a BorrowedMessage<'a>, trigger::Sender) -> H + Send + Sync + 'static,
    H: EventHandler<GenericConnectorAgent> + Send + 'static,
{
    async fn consume_messages(self) -> Result<(), KafkaError> {
        let MessageState {
            consumer,
            to_handler,
            tx,
        } = &self;
        loop {
            let reservation = if let Ok(res) = tx.reserve().await {
                res
            } else {
                break;
            };
            let message = consumer.recv().await?;
            let (trigger_tx, trigger_rx) = trigger::trigger();
            let handler = to_handler(&message, trigger_tx);
            reservation.send(handler);
            let _ = trigger_rx.await;
            consumer.commit_message(&message, CommitMode::Async)?;
        }
        Ok(())
    }
}

struct MessageTasks<F, H> {
    consume_fut: Option<F>,
    rx: mpsc::Receiver<H>,
}

impl<F, H> MessageTasks<F, H> {
    pub fn new(consume_fut: F, rx: mpsc::Receiver<H>) -> Self {
        MessageTasks {
            consume_fut: Some(consume_fut),
            rx,
        }
    }
}

impl<F, H> MessageTasks<F, H>
where
    F: Future<Output = Result<(), KafkaError>> + Send + Unpin + 'static,
    H: EventHandler<GenericConnectorAgent> + Send + 'static,
{
    fn into_stream(self) -> impl ConnectorStream<KafkaError> {
        Box::pin(unfold(self, |s| s.next_handler()))
    }

    async fn next_handler(mut self) -> Option<(Result<H, KafkaError>, Self)> {
        let MessageTasks { consume_fut, rx } = &mut self;
        if let Some(fut) = consume_fut {
            tokio::select! {
                biased;
                end_result = fut => {
                    *consume_fut = None;
                    if let Err(e) = end_result {
                        Some((Err(e), self))
                    } else {
                        None
                    }
                },
                next = rx.recv() => {
                    if next.is_none() {
                        *consume_fut = None;
                        None
                    } else {
                        next.map(move |h| (Ok(h), self))
                    }
                },
            }
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessagePart {
    Key,
    Value,
}
