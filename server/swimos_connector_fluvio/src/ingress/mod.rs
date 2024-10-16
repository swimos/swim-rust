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

mod tests;

use crate::config::FluvioIngressConfiguration;
use crate::FluvioConnectorError;
use fluvio::consumer::{ConsumerConfigExt, Record};
use fluvio::dataplane::link::ErrorCode;
use fluvio::dataplane::record::ConsumerRecord;
use fluvio::dataplane::types::PartitionId;
use fluvio::{Fluvio, FluvioError};
use futures::stream::{unfold, BoxStream};
use futures::StreamExt;
use std::cell::RefCell;
use std::future::{ready, Future};
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::event_handler::{EventHandler, UnitHandler};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::config::format::DataFormat;
use swimos_connector::deser::{BoxMessageDeserializer, MessageView};
use swimos_connector::ingress::{pubsub::MessageSelector, Lanes};
use swimos_connector::selector::PubSubSelector;
use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorStream, IngressConnector, IngressContext, LoadError,
};
use swimos_utilities::trigger::Sender;
use tracing::{error, info, trace};

/// A Fluivo ingress [connector](`swimos_connector::IngressConnector`) to ingest a stream of Fluvio
/// records into a Swim application.
#[derive(Debug, Clone)]
pub struct FluvioIngressConnector<F> {
    configuration: FluvioIngressConfiguration,
    lanes: RefCell<Lanes<PubSubSelector>>,
    factory: F,
}

impl<F> FluvioIngressConnector<F> {
    pub fn new(configuration: FluvioIngressConfiguration, factory: F) -> FluvioIngressConnector<F> {
        FluvioIngressConnector {
            configuration,
            lanes: RefCell::new(Default::default()),
            factory,
        }
    }
}

impl FluvioIngressConnector<FluvioIngressConsumer> {
    /// Create a [`FluvioIngressConnector`] with the provided configuration. The configuration is
    /// only validated when the agent attempts to start so this will never fail.
    ///
    /// # Arguments
    /// * `configuration` - The connector configuration, specifying the connection details for the
    ///   Fluvio consumer and the lanes that the connector agent should expose.
    pub fn for_config(
        configuration: FluvioIngressConfiguration,
    ) -> FluvioIngressConnector<FluvioIngressConsumer> {
        FluvioIngressConnector::new(configuration, FluvioIngressConsumer)
    }
}

impl<F> BaseConnector for FluvioIngressConnector<F> {
    fn on_start(&self, init_complete: Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let handler_context = HandlerContext::<ConnectorAgent>::default();
        handler_context.effect(move || {
            init_complete.trigger();
        })
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

impl<F> IngressConnector for FluvioIngressConnector<F>
where
    F: FluvioConsumer,
{
    type Error = FluvioConnectorError;

    fn create_stream(&self) -> Result<impl ConnectorStream<Self::Error>, Self::Error> {
        let FluvioIngressConnector {
            configuration,
            lanes,
            factory,
        } = self;
        let FluvioIngressConfiguration {
            topic,
            key_deserializer,
            payload_deserializer,
            relays,
            ..
        } = configuration;

        let key_deser = key_deserializer.clone();
        let value_deser = payload_deserializer.clone();
        let lanes = lanes.take();
        let topic = topic.clone();
        let relays = relays.clone();

        Ok(unfold(
            ConnectorState::Uninit(configuration.clone(), factory.clone()),
            move |state: ConnectorState<F::Client, F>| {
                let topic = topic.clone();
                let key_deser = key_deser.clone();
                let value_deser = value_deser.clone();
                let lanes = lanes.clone();
                let relays = relays.clone();

                let fut = async move {
                    match state {
                        ConnectorState::Uninit(config, factory) => match factory.open(config).await
                        {
                            Ok(consumer) => {
                                let (key, value) =
                                    match load_deserializers(key_deser, value_deser).await {
                                        Ok((key, value)) => (key, value),
                                        Err(e) => {
                                            return Some((
                                                Err(FluvioConnectorError::Configuration(e)),
                                                ConnectorState::Failed,
                                            ))
                                        }
                                    };
                                poll_dispatch(
                                    consumer,
                                    topic,
                                    MessageSelector::new(key, value, lanes, relays),
                                )
                                .await
                            }
                            Err(e) => Some((Err(e), ConnectorState::Failed)),
                        },
                        ConnectorState::Running {
                            topic,
                            consumer,
                            message_selector,
                        } => poll_dispatch(consumer, topic, message_selector).await,
                        ConnectorState::Failed => None,
                    }
                };
                Box::pin(fut)
            },
        ))
    }

    fn initialize(&self, context: &mut dyn IngressContext) -> Result<(), Self::Error> {
        let FluvioIngressConnector {
            lanes,
            configuration,
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
                error!(error = %err, "Failed to create lanes for a Fluvio connector.");
                return Err(err.into());
            }
        }
        Ok(())
    }
}

enum ConnectorState<C, F> {
    Uninit(FluvioIngressConfiguration, F),
    Running {
        topic: String,
        consumer: C,
        message_selector: MessageSelector,
    },
    Failed,
}

trait FluvioConsumer: Clone + Send + 'static {
    type Client: FluvioClient;

    fn open(
        &self,
        config: FluvioIngressConfiguration,
    ) -> impl Future<Output = Result<Self::Client, FluvioConnectorError>> + Send;
}

trait FluvioRecord: Send {
    fn offset(&self) -> i64;

    fn partition_id(&self) -> PartitionId;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];
}

impl FluvioRecord for ConsumerRecord {
    fn offset(&self) -> i64 {
        self.offset
    }

    fn partition_id(&self) -> PartitionId {
        self.partition
    }

    fn key(&self) -> &[u8] {
        self.record.key().map(|k| k.as_ref()).unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        self.record.value().as_ref()
    }
}

trait FluvioClient: Send + 'static {
    type FluvioRecord: FluvioRecord;

    fn next(
        &mut self,
    ) -> impl Future<Output = Option<Result<Self::FluvioRecord, FluvioConnectorError>>> + Send;

    fn shutdown(&mut self) -> impl Future<Output = ()> + Send;
}

#[derive(Clone)]
pub struct FluvioIngressConsumer;

impl FluvioConsumer for FluvioIngressConsumer {
    type Client = FluvioStreamClient;

    async fn open(
        &self,
        config: FluvioIngressConfiguration,
    ) -> Result<Self::Client, FluvioConnectorError> {
        let FluvioIngressConfiguration {
            topic,
            fluvio,
            partition,
            offset,
            ..
        } = config;

        match Fluvio::connect_with_config(&fluvio).await {
            Ok(handle) => {
                let consumer_config = match ConsumerConfigExt::builder()
                    .topic(topic)
                    .offset_start(offset)
                    .partition(partition)
                    .build()
                {
                    Ok(config) => config,
                    Err(error) => {
                        error!(?error, "Failed to build consumer config");
                        return Err(FluvioConnectorError::Message(error.to_string()));
                    }
                };

                match handle.consumer_with_config(consumer_config).await {
                    Ok(consumer) => {
                        info!("Fluvio consumer successfully opened");
                        Ok(FluvioStreamClient {
                            fluvio: Some(handle),
                            stream: consumer.boxed(),
                        })
                    }
                    Err(error) => {
                        error!(?error, "Failed to create Fluvio consumer");
                        Err(FluvioConnectorError::Message(error.to_string()))
                    }
                }
            }
            Err(error) => {
                error!(?error, "Failed to connect to Fluvio cluster");
                Err(FluvioConnectorError::Message(error.to_string()))
            }
        }
    }
}

pub struct FluvioStreamClient {
    fluvio: Option<Fluvio>,
    stream: BoxStream<'static, Result<Record, ErrorCode>>,
}

impl FluvioClient for FluvioStreamClient {
    type FluvioRecord = ConsumerRecord;

    fn next(
        &mut self,
    ) -> impl Future<Output = Option<Result<Self::FluvioRecord, FluvioConnectorError>>> + Send {
        let stream = &mut self.stream;
        async move {
            match stream.next().await {
                Some(Ok(record)) => Some(Ok(record)),
                Some(Err(code)) => {
                    error!(%code, "Fluvio consumer failed to read");
                    Some(Err(FluvioConnectorError::Native(FluvioError::Other(
                        code.to_string(),
                    ))))
                }
                None => None,
            }
        }
    }

    fn shutdown(&mut self) -> impl Future<Output = ()> + Send {
        // drop the Fluvio instance to initiate a shutdown.
        // we have to keep it around until a shutdown is requested to prevent an early shutdown.
        let _ = self.fluvio.take();
        ready(())
    }
}

async fn load_deserializers(
    key: DataFormat,
    value: DataFormat,
) -> Result<(BoxMessageDeserializer, BoxMessageDeserializer), LoadError> {
    let key = key.load_deserializer().await?;
    let value = value.load_deserializer().await?;
    Ok((key, value))
}

async fn poll_dispatch<C, F>(
    mut consumer: C,
    topic: String,
    message_selector: MessageSelector,
) -> Option<(
    Result<impl EventHandler<ConnectorAgent> + Send + 'static, FluvioConnectorError>,
    ConnectorState<C, F>,
)>
where
    C: FluvioClient,
{
    match consumer.next().await {
        Some(Ok(record)) => {
            let offset = record.offset();
            let partition = record.partition_id();

            trace!(?offset, ?partition, topic=%topic, "Handling record");

            let view = MessageView {
                topic: topic.as_str(),
                key: record.key(),
                payload: record.value(),
            };

            let handle_result = message_selector.handle_message(&view).map_err(Into::into);
            if let Err(err) = &handle_result {
                error!(error = %err, "Failed to handle message");
            }

            Some((
                handle_result,
                ConnectorState::Running {
                    topic,
                    consumer,
                    message_selector,
                },
            ))
        }
        Some(Err(e)) => {
            consumer.shutdown().await;
            Some((Err(e), ConnectorState::Failed))
        }
        None => None,
    }
}
