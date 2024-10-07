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

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::HashSet;

use crate::config::KafkaIngressConfiguration;
use crate::deser::{BoxMessageDeserializer, MessagePart, MessageView};
use crate::error::{KafkaConnectorError, LaneSelectorError};
use crate::facade::{ConsumerFactory, KafkaConsumer, KafkaFactory, KafkaMessage};
use crate::selector::{Computed, MapLaneSelector, ValueLaneSelector};
use crate::{IngressMapLaneSpec, IngressValueLaneSpec, InvalidLanes};
use futures::{stream::unfold, Future};
use swimos_agent::event_handler::{EventHandler, HandlerActionExt, Sequentially, UnitHandler};
use swimos_api::agent::WarpLaneKind;
use swimos_model::Value;
use swimos_utilities::trigger;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace};

use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorStream, IngressConnector, IngressContext,
};

use super::ConnHandlerContext;

/// A [connector](IngressConnector) to ingest a stream of Kafka messages into a Swim application. This should be used to
/// provide a lifecycle for a [connector agent](ConnectorAgent).
///
/// The details of the Kafka brokers and the topics to subscribe to are provided through the
/// [configuration](KafkaIngressConfiguration) which also includes descriptors of the lanes that the agent should
/// expose. When the agent starts, the connector will register all of the lanes specified in the configuration and
/// then attempt to open a Kafka consumer which will be spawned into the agent's own task. Each time a Kafka message
/// is received, an event handler will be executed in the agent which updates the states of the lanes, according
/// th the specification provided in the configuration.
///
/// If a message is received that is invalid with respect to the configuration, the entire agent will fail with an
/// error.
#[derive(Debug, Clone)]
pub struct KafkaIngressConnector<F> {
    factory: F,
    configuration: KafkaIngressConfiguration,
    lanes: RefCell<Lanes>,
}

impl<F> KafkaIngressConnector<F> {
    fn new(factory: F, configuration: KafkaIngressConfiguration) -> Self {
        KafkaIngressConnector {
            factory,
            configuration,
            lanes: Default::default(),
        }
    }
}

impl KafkaIngressConnector<KafkaFactory> {
    /// Create a [`KafkaIngressConnector`] with the provided configuration. The configuration is only validated when
    /// the agent attempts to start so this will never fail.
    ///
    /// # Arguments
    /// * `configuration` - The connector configuration, specifying the connection details for the Kafka consumer
    ///   an the lanes that the connector agent should expose.
    pub fn for_config(configuration: KafkaIngressConfiguration) -> Self {
        Self::new(KafkaFactory, configuration)
    }
}

impl<F> BaseConnector for KafkaIngressConnector<F>
where
    F: ConsumerFactory + Send + 'static,
{
    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let handler_context = ConnHandlerContext::default();
        handler_context.effect(move || {
            init_complete.trigger();
        })
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

impl<F> IngressConnector for KafkaIngressConnector<F>
where
    F: ConsumerFactory + Send + 'static,
{
    type StreamError = KafkaConnectorError;

    fn create_stream(
        &self,
    ) -> Result<impl ConnectorStream<KafkaConnectorError>, Self::StreamError> {
        let KafkaIngressConnector {
            factory,
            configuration,
            lanes,
        } = self;

        info!(properties = ?{&configuration.properties}, "Opening a kafka consumer.");
        let topics = configuration
            .topics
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();
        let consumer =
            factory.create(&configuration.properties, configuration.log_level, &topics)?;
        let (tx, rx) = mpsc::channel(1);

        let key_deser_cpy = configuration.key_deserializer.clone();
        let payload_deser_cpy = configuration.payload_deserializer.clone();
        let lanes = lanes.take();
        let consumer_task = Box::pin(async move {
            debug!(key = ?key_deser_cpy, payload = ?payload_deser_cpy, "Attempting to load message deserializers.");
            let key_deser = key_deser_cpy.load_deserializer().await?;
            let payload_deser = payload_deser_cpy.load_deserializer().await?;
            let selector = MessageSelector::new(key_deser, payload_deser, lanes);
            let state = MessageState::new(consumer, selector, message_to_handler, tx);
            state.consume_messages(None).await
        });

        let stream_src = MessageTasks::new(consumer_task, rx);
        Ok(stream_src.into_stream())
    }

    fn initialize(&self, context: &mut dyn IngressContext) -> Result<(), Self::StreamError> {
        let KafkaIngressConnector {
            configuration,
            lanes,
            ..
        } = self;
        let mut guard = lanes.borrow_mut();
        match Lanes::try_from(configuration) {
            Ok(lanes_from_conf) => {
                for lane_spec in &lanes_from_conf.value_lanes {
                    context.open_lane(lane_spec.name(), WarpLaneKind::Value);
                }
                for lane_spec in &lanes_from_conf.map_lanes {
                    context.open_lane(lane_spec.name(), WarpLaneKind::Map);
                }
                *guard = lanes_from_conf;
            }
            Err(err) => {
                error!(error = %err, "Failed to create lanes for a Kafka connector.");
                return Err(err.into());
            }
        }
        Ok(())
    }
}

fn message_to_handler<'a>(
    selector: &'a MessageSelector,
    message: &'a MessageView<'a>,
    trigger_tx: trigger::Sender,
) -> Result<impl EventHandler<ConnectorAgent> + Send + 'static, LaneSelectorError> {
    selector.handle_message(message, trigger_tx)
}

// Consumes the Kafka messages and converts them into event handlers.
struct MessageState<C, F, H> {
    consumer: C,
    selector: MessageSelector,
    to_handler: F,
    tx: mpsc::Sender<H>,
}

impl<C, F, H> MessageState<C, F, H>
where
    C: KafkaConsumer + Send + 'static,
    F: for<'a> Fn(
            &'a MessageSelector,
            &'a MessageView<'a>,
            trigger::Sender,
        ) -> Result<H, LaneSelectorError>
        + Send
        + 'static,
    H: EventHandler<ConnectorAgent> + Send + 'static,
{
    fn new(consumer: C, selector: MessageSelector, to_handler: F, tx: mpsc::Sender<H>) -> Self {
        MessageState {
            consumer,
            selector,
            to_handler,
            tx,
        }
    }

    // A task that consumes Kafka messages from the consumer and convert them to event handlers that are
    // send out via an MPSC channel
    async fn consume_messages(
        self,
        mut stop_rx: Option<trigger::Receiver>, // Can be used to stop the task prematurely.
    ) -> Result<(), KafkaConnectorError> {
        let MessageState {
            consumer,
            selector,
            to_handler,
            tx,
        } = &self;
        loop {
            let reservation = if let Some(rx) = stop_rx.as_mut() {
                let result = tokio::select! {
                    biased;
                    _ = rx => break,
                    result = tx.reserve() => result,
                };
                if let Ok(res) = result {
                    res
                } else {
                    break;
                }
            } else if let Ok(res) = tx.reserve().await {
                res
            } else {
                break;
            };

            let message = consumer.recv().await?;
            let view = message.view();
            let (trigger_tx, trigger_rx) = trigger::trigger();
            // We need to keep a borrow on the receiver in order to be bale to commit it. However, we don't want
            // to do this until after we know the handler has been run. The handler cannot be executed within the
            // as it requires access to the agent state. Therefore, we send it out via an MPSC channel and
            // wait for a signal to indicate that it has been handled.
            let handler = to_handler(selector, &view, trigger_tx)?;
            reservation.send(handler);
            if trigger_rx.await.is_err() {
                // If the trigger is dropped, we cannot know that the message has been handled and so we should
                // not commit it.
                return Err(KafkaConnectorError::MessageNotHandled);
            }
            consumer.commit(message)?;
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
        // The `consume_fut` future is that task that is consuming the Kafka messages. It needs to hold
        // a reference to the Kafka message while it is being handled in a different task so the consumer
        // cannot be held directly by the stream (as it is self referential) without using unsafe code.
        // This task will only end when the Kafka consumer terminates. It passes out the event handlers
        // using and MPSC channel (of which `rx` is the receiving end).
        //
        // Each handler contains a trigger that is executed when the handler completes and the consumer
        // task will suspend until this trigger is completed. Therefore, polling the consumer task will
        // never block the executor as the MPSC channel that is uses takes part in the Tokio coop mechanism.
        let MessageTasks { consume_fut, rx } = &mut self;
        if let Some(fut) = consume_fut {
            tokio::select! {
                biased;
                end_result = fut => {
                    *consume_fut = None;
                    if let Err(e) = end_result {
                        error!(error = %e, "The consumer task failed with an error.");
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

// Information about the lanes of the connector. These are computed from the configuration in the `on_start` handler
// and stored in the lifecycle to be used to start the consumer stream.
#[derive(Debug, Default, Clone)]
struct Lanes {
    value_lanes: Vec<ValueLaneSelector>,
    map_lanes: Vec<MapLaneSelector>,
}

impl TryFrom<&KafkaIngressConfiguration> for Lanes {
    type Error = InvalidLanes;

    fn try_from(value: &KafkaIngressConfiguration) -> Result<Self, Self::Error> {
        let KafkaIngressConfiguration {
            value_lanes,
            map_lanes,
            ..
        } = value;
        Lanes::try_from_lane_specs(value_lanes, map_lanes)
    }
}

impl Lanes {
    fn try_from_lane_specs(
        value_lanes: &[IngressValueLaneSpec],
        map_lanes: &[IngressMapLaneSpec],
    ) -> Result<Self, InvalidLanes> {
        let value_selectors = value_lanes
            .iter()
            .map(ValueLaneSelector::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let map_selectors = map_lanes
            .iter()
            .map(MapLaneSelector::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        check_selectors(&value_selectors, &map_selectors)?;
        Ok(Lanes {
            value_lanes: value_selectors,
            map_lanes: map_selectors,
        })
    }
}

fn check_selectors(
    value_selectors: &[ValueLaneSelector],
    map_selectors: &[MapLaneSelector],
) -> Result<(), InvalidLanes> {
    let mut names = HashSet::new();
    for value_selector in value_selectors {
        let name = value_selector.name();
        if names.contains(name) {
            return Err(InvalidLanes::NameCollision(name.to_string()));
        } else {
            names.insert(name);
        }
    }
    for map_selector in map_selectors {
        let name = map_selector.name();
        if names.contains(name) {
            return Err(InvalidLanes::NameCollision(name.to_string()));
        } else {
            names.insert(name);
        }
    }
    Ok(())
}

// Uses the information about the lanes of the agent to convert Kafka messages into event handlers that update the lanes.
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
        message: &'a MessageView<'a>,
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
        trace!(topic = { message.topic() }, "Handling a Kafka message.");
        let mut value_lane_handlers = Vec::with_capacity(value_lanes.len());
        let mut map_lane_handlers = Vec::with_capacity(map_lanes.len());
        {
            let topic = Value::text(message.topic());
            let mut key = Computed::new(|| key_deserializer.deserialize(message, MessagePart::Key));
            let mut value =
                Computed::new(|| value_deserializer.deserialize(message, MessagePart::Payload));

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
