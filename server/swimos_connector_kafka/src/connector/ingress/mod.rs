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

use super::ConnHandlerContext;
use crate::error::KafkaConnectorError;
use crate::facade::{ConsumerFactory, KafkaConsumer, KafkaFactory, KafkaMessage};
use crate::KafkaIngressConfiguration;
use futures::{stream::unfold, Future};
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::event_handler::{EventHandler, HandlerActionExt, UnitHandler};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::deser::MessageView;
use swimos_connector::ingress::{Lanes, MessageSelector};
use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorStream, IngressConnector, IngressContext, SelectorError,
};
use swimos_utilities::trigger;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

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
    type Error = KafkaConnectorError;

    fn create_stream(&self) -> Result<impl ConnectorStream<KafkaConnectorError>, Self::Error> {
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
        let relays = configuration.relays.clone();
        let lanes = lanes.take();

        let consumer_task = Box::pin(async move {
            debug!(key = ?key_deser_cpy, payload = ?payload_deser_cpy, "Attempting to load message deserializers.");
            let key_deser = key_deser_cpy.load_deserializer().await?;
            let payload_deser = payload_deser_cpy.load_deserializer().await?;
            let selector = MessageSelector::new(key_deser, payload_deser, lanes, relays);
            let state = MessageState::new(consumer, selector, message_to_handler, tx);
            state.consume_messages(None).await
        });

        let stream_src = MessageTasks::new(consumer_task, rx);
        Ok(stream_src.into_stream())
    }

    fn initialize(&self, context: &mut dyn IngressContext) -> Result<(), Self::Error> {
        let KafkaIngressConnector {
            configuration,
            lanes,
            ..
        } = self;
        let mut guard = lanes.borrow_mut();
        match Lanes::try_from_lane_specs(&configuration.value_lanes, &configuration.map_lanes) {
            Ok(lanes_from_conf) => {
                for lane_spec in lanes_from_conf.value_lanes() {
                    context.open_lane(lane_spec.name(), WarpLaneKind::Value);
                }
                for lane_spec in lanes_from_conf.map_lanes() {
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
) -> Result<impl EventHandler<ConnectorAgent> + Send + 'static, SelectorError> {
    let handler_context = HandlerContext::default();
    selector.handle_message(message).map(|handler| {
        handler.followed_by(handler_context.effect(move || {
            let _ = trigger_tx.trigger();
        }))
    })
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
        ) -> Result<H, SelectorError>
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
            // We need to keep a borrow on the receiver in order to be able to commit it. However, we don't want
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
