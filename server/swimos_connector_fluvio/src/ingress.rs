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

use crate::config::FluvioIngressConfiguration;
use crate::FluvioConnectorError;
use fluvio::consumer::{ConsumerConfigExt, ConsumerStream, Record};
use fluvio::dataplane::link::ErrorCode;
use fluvio::dataplane::record::ConsumerRecord;
use fluvio::{Fluvio, FluvioError};
use futures::stream::unfold;
use futures::Stream;
use futures::StreamExt;
use std::cell::RefCell;
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::event_handler::{
    Either, EventHandler, HandlerActionExt, TryHandlerActionExt, UnitHandler,
};
use swimos_connector::config::format::DataFormat;
use swimos_connector::deser::{BoxMessageDeserializer, MessageView};
use swimos_connector::ingress::{Lanes, MessageSelector};
use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorStream, IngressConnector, LoadError,
};
use swimos_utilities::trigger::Sender;
use tracing::{debug, error, info, trace};

/// A Fluivo ingress [connector](`swimos_connector::IngressConnector`) to ingest a stream of Fluvio
/// records into a Swim application.
#[derive(Debug, Clone)]
pub struct FluvioIngressConnector {
    configuration: FluvioIngressConfiguration,
    lanes: RefCell<Lanes>,
}

impl FluvioIngressConnector {
    pub fn for_config(configuration: FluvioIngressConfiguration) -> Self {
        FluvioIngressConnector {
            configuration,
            lanes: RefCell::new(Default::default()),
        }
    }
}

impl BaseConnector for FluvioIngressConnector {
    fn on_start(&self, init_complete: Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let FluvioIngressConnector {
            lanes,
            configuration,
        } = self;
        let handler_context = HandlerContext::default();

        let result =
            Lanes::try_from_lane_specs(&configuration.value_lanes, &configuration.map_lanes);
        if let Err(err) = &result {
            error!(error = %err, "Failed to create lanes for a Fluvio connector.");
        }
        let handler = handler_context
            .value(result)
            .try_handler()
            .and_then(|l: Lanes| {
                let open_handler = l.open_lanes(init_complete);
                debug!("Successfully created lanes for a Fluvio connector.");
                *lanes.borrow_mut() = l;
                open_handler
            });

        handler
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

impl IngressConnector for FluvioIngressConnector {
    type StreamError = FluvioConnectorError;

    fn create_stream(&self) -> Result<impl ConnectorStream<Self::StreamError>, Self::StreamError> {
        let FluvioIngressConnector {
            configuration,
            lanes,
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
            ConnectorState::Uninit(configuration.clone()),
            move |state| {
                let topic = topic.clone();
                let key_deser = key_deser.clone();
                let value_deser = value_deser.clone();
                let lanes = lanes.clone();
                let relays = relays.clone();

                let fut = async move {
                    match state {
                        ConnectorState::Uninit(config) => match open(config).await {
                            Ok((handle, consumer)) => {
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
                                Some((
                                    Ok(Either::Left(UnitHandler::default())),
                                    ConnectorState::Running {
                                        fluvio: handle,
                                        topic,
                                        consumer,
                                        message_selector: MessageSelector::new(
                                            key, value, lanes, relays,
                                        ),
                                    },
                                ))
                            }
                            Err(e) => Some((Err(e), ConnectorState::Failed)),
                        },
                        ConnectorState::Running {
                            fluvio,
                            topic,
                            mut consumer,
                            message_selector,
                        } => match poll_dispatch(&mut consumer, topic.as_str(), &message_selector)
                            .await
                        {
                            Some(Ok(handler)) => Some((
                                Ok(Either::Right(handler)),
                                ConnectorState::Running {
                                    fluvio,
                                    topic,
                                    consumer,
                                    message_selector,
                                },
                            )),
                            Some(Err(e)) => Some((Err(e), ConnectorState::Failed)),
                            None => None,
                        },
                        ConnectorState::Failed => None,
                    }
                };
                Box::pin(fut)
            },
        ))
    }
}

enum ConnectorState<C> {
    Uninit(FluvioIngressConfiguration),
    Running {
        fluvio: Fluvio,
        topic: String,
        consumer: C,
        message_selector: MessageSelector,
    },
    Failed,
}

async fn poll_dispatch<C>(
    consumer: &mut C,
    topic: &str,
    message_selector: &MessageSelector,
) -> Option<Result<impl EventHandler<ConnectorAgent> + Send + 'static, FluvioConnectorError>>
where
    C: Stream<Item = Result<Record, ErrorCode>> + Unpin,
{
    match consumer.next().await {
        Some(Ok(record)) => {
            let ConsumerRecord {
                offset,
                partition,
                record,
                ..
            } = record;

            trace!(?offset, ?partition, topic=%topic, "Handling record");

            let view = MessageView {
                topic,
                key: record.key().map(|k| k.as_ref()).unwrap_or_default(),
                payload: record.value().as_ref(),
            };

            let handle_result = message_selector.handle_message(&view).map_err(Into::into);
            if let Err(err) = &handle_result {
                error!(error = %err, "Failed to handle message");
            }

            Some(handle_result)
        }
        Some(Err(code)) => {
            error!(%code, "Fluvio consumer failed to read");
            Some(Err(FluvioConnectorError::Native(FluvioError::Other(
                code.to_string(),
            ))))
        }
        None => None,
    }
}

async fn open(
    config: FluvioIngressConfiguration,
) -> Result<
    (
        Fluvio,
        impl ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + Sized,
    ),
    FluvioConnectorError,
> {
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
                    Ok((handle, consumer))
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

async fn load_deserializers(
    key: DataFormat,
    value: DataFormat,
) -> Result<(BoxMessageDeserializer, BoxMessageDeserializer), LoadError> {
    let key = key.load_deserializer().await?;
    let value = value.load_deserializer().await?;
    Ok((key, value))
}
