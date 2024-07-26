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

use std::{cell::RefCell, sync::Arc};

use crate::config::{DerserializerLoadError, KafkaConnectorConfiguration};
use crate::deser::BoxMessageDeserializer;
use crate::selector::{
    Computed, InvalidLaneSpec, LaneSelectorError, MapLaneSelector, ValueLaneSelector,
};
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
    event_handler::{
        EventHandler, HandlerActionExt, Sequentially, TryHandlerActionExt, UnitHandler,
    },
};
use swimos_model::Value;
use swimos_utilities::trigger;
use thiserror::Error;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error, info, warn};

use swimos_connector::{Connector, ConnectorAgent, ConnectorStream};

type ConnHandlerContext = HandlerContext<ConnectorAgent>;
type ConnContext = ConnectorContext<ConnectorAgent>;

pub struct KafkaConnector {
    configuration: KafkaConnectorConfiguration,
    lanes: RefCell<Lanes>,
}

impl KafkaConnector {
    pub fn new(configuration: KafkaConnectorConfiguration) -> Self {
        KafkaConnector {
            configuration,
            lanes: Default::default(),
        }
    }
}

type LoggingConsumer = StreamConsumer<KafkaClientContext>;

#[derive(Debug, Error)]
pub enum KafkaConnectorError {
    #[error(transparent)]
    Configuration(#[from] DerserializerLoadError),
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error(transparent)]
    Lane(#[from] LaneSelectorError),
}

impl Connector for KafkaConnector {
    type StreamError = KafkaConnectorError;

    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let handler_context = ConnHandlerContext::default();
        let KafkaConnector {
            configuration,
            lanes,
        } = self;
        let result = Lanes::try_from(configuration);
        let handler = handler_context
            .value(result)
            .try_handler()
            .and_then(|l: Lanes| {
                let open_handler = l.open_lanes(init_complete);
                *lanes.borrow_mut() = l;
                open_handler
            });

        handler
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }

    fn create_stream(
        &self,
    ) -> Result<impl ConnectorStream<KafkaConnectorError>, Self::StreamError> {
        let KafkaConnector {
            configuration,
            lanes,
        } = self;

        let mut client_builder = ClientConfig::new();
        configuration.properties.iter().for_each(|(k, v)| {
            client_builder.set(k, v);
        });
        let consumer = client_builder
            .set_log_level(configuration.log_level.into())
            .create_with_context::<_, LoggingConsumer>(KafkaClientContext)?;
        let (tx, rx) = mpsc::channel(1);

        let key_deser_cpy = configuration.key_deserializer.clone();
        let val_deser_cpy = configuration.value_deserializer.clone();
        let lanes = lanes.take();
        let consumer_task = Box::pin(async move {
            let key_deser = key_deser_cpy.load().await?;
            let val_deser = val_deser_cpy.load().await?;
            let selector = MessageSelector::new(key_deser, val_deser, lanes);
            let state = MessageState::new(consumer, selector, message_to_handler, tx);
            state.consume_messages().await
        });

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

fn message_to_handler<'a>(
    selector: &'a MessageSelector,
    message: &'a BorrowedMessage<'a>,
    trigger_tx: trigger::Sender,
) -> Result<impl EventHandler<ConnectorAgent> + Send + 'static, LaneSelectorError> {
    selector.handle_message(message, trigger_tx)
}

struct MessageState<F, H> {
    consumer: LoggingConsumer,
    selector: MessageSelector,
    to_handler: F,
    tx: mpsc::Sender<H>,
}

impl<F, H> MessageState<F, H>
where
    F: for<'a> Fn(
            &'a MessageSelector,
            &'a BorrowedMessage<'a>,
            trigger::Sender,
        ) -> Result<H, LaneSelectorError>
        + Send
        + 'static,
    H: EventHandler<ConnectorAgent> + Send + 'static,
{
    fn new(
        consumer: LoggingConsumer,
        selector: MessageSelector,
        to_handler: F,
        tx: mpsc::Sender<H>,
    ) -> Self {
        MessageState {
            consumer,
            selector,
            to_handler,
            tx,
        }
    }

    async fn consume_messages(self) -> Result<(), KafkaConnectorError> {
        let MessageState {
            consumer,
            selector,
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
            let handler = to_handler(selector, &message, trigger_tx)?;
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
    F: Future<Output = Result<(), KafkaConnectorError>> + Send + Unpin + 'static,
    H: EventHandler<ConnectorAgent> + Send + 'static,
{
    fn into_stream(self) -> impl ConnectorStream<KafkaConnectorError> {
        Box::pin(unfold(self, |s| s.next_handler()))
    }

    async fn next_handler(mut self) -> Option<(Result<H, KafkaConnectorError>, Self)> {
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

#[derive(Debug, Default)]
struct Lanes {
    total_lanes: u32,
    value_lanes: Vec<ValueLaneSelector>,
    map_lanes: Vec<MapLaneSelector>,
}

#[derive(Clone, Debug, Error)]
enum InvalidLanes {
    #[error(transparent)]
    Spec(#[from] InvalidLaneSpec),
    #[error("The connector has {0} lanes which cannot fit in a u32.")]
    TooManyLanes(usize),
}

impl TryFrom<&KafkaConnectorConfiguration> for Lanes {
    type Error = InvalidLanes;

    fn try_from(value: &KafkaConnectorConfiguration) -> Result<Self, Self::Error> {
        let KafkaConnectorConfiguration {
            value_lanes,
            map_lanes,
            ..
        } = value;
        let value_selectors = value_lanes
            .iter()
            .map(ValueLaneSelector::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let map_selectors = map_lanes
            .iter()
            .map(MapLaneSelector::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let total = value_selectors.len() + map_selectors.len();
        let total_lanes = if let Ok(n) = u32::try_from(total) {
            n
        } else {
            return Err(InvalidLanes::TooManyLanes(total));
        };
        Ok(Lanes {
            value_lanes: value_selectors,
            map_lanes: map_selectors,
            total_lanes,
        })
    }
}

impl Lanes {
    fn open_lanes(
        &self,
        init_complete: trigger::Sender,
    ) -> impl EventHandler<ConnectorAgent> + 'static {
        let handler_context = ConnHandlerContext::default();
        let context: ConnContext = ConnContext::default();
        let Lanes {
            value_lanes,
            map_lanes,
            total_lanes,
        } = self;

        let semaphore = Arc::new(Semaphore::new(0));

        let wait_handle = semaphore.clone();
        let total = *total_lanes;
        let await_done = async move {
            let result = wait_handle.acquire_many(total).await.map(|_| ());
            handler_context
                .value(result)
                .try_handler()
                .followed_by(handler_context.effect(|| {
                    let _ = init_complete.trigger();
                }))
        };

        let mut open_value_lanes = Vec::with_capacity(value_lanes.len());
        let mut open_map_lanes = Vec::with_capacity(map_lanes.len());

        for selector in value_lanes {
            let sem_cpy = semaphore.clone();
            open_value_lanes.push(context.open_value_lane(selector.name(), move |_| {
                handler_context.effect(move || sem_cpy.add_permits(1))
            }));
        }

        for selector in map_lanes {
            let sem_cpy = semaphore.clone();
            open_map_lanes.push(context.open_map_lane(selector.name(), move |_| {
                handler_context.effect(move || sem_cpy.add_permits(1))
            }));
        }

        handler_context
            .suspend(await_done)
            .followed_by(Sequentially::new(open_value_lanes))
            .followed_by(Sequentially::new(open_map_lanes))
            .discard()
    }
}

struct MessageSelector {
    key_deserializer: BoxMessageDeserializer,
    value_deserializer: BoxMessageDeserializer,
    lanes: Lanes,
}

impl MessageSelector {
    pub fn new(
        key_deserializer: BoxMessageDeserializer,
        value_deserializer: BoxMessageDeserializer,
        lanes: Lanes,
    ) -> Self {
        MessageSelector {
            key_deserializer,
            value_deserializer,
            lanes,
        }
    }

    fn handle_message<'a>(
        &self,
        message: &'a BorrowedMessage<'a>,
        on_done: trigger::Sender,
    ) -> Result<impl EventHandler<ConnectorAgent> + Send + 'static, LaneSelectorError> {
        let MessageSelector {
            key_deserializer,
            value_deserializer,
            lanes,
        } = self;
        let Lanes {
            value_lanes,
            map_lanes,
            ..
        } = lanes;
        let mut value_lane_handlers = Vec::with_capacity(value_lanes.len());
        let mut map_lane_handlers = Vec::with_capacity(map_lanes.len());
        {
            let topic = Value::text(message.topic());
            let mut key = Computed::new(|| key_deserializer.deserialize(message, MessagePart::Key));
            let mut value =
                Computed::new(|| value_deserializer.deserialize(message, MessagePart::Value));

            for value_lane in value_lanes {
                value_lane_handlers.push(value_lane.select_handler(&topic, &mut key, &mut value)?);
            }
            for map_lane in map_lanes {
                map_lane_handlers.push(map_lane.select_handler(&topic, &mut key, &mut value)?);
            }
        }
        let handler_context = ConnHandlerContext::default();
        Ok(Sequentially::new(value_lane_handlers)
            .followed_by(Sequentially::new(map_lane_handlers))
            .followed_by(handler_context.effect(move || {
                let _ = on_done.trigger();
            })))
    }
}
