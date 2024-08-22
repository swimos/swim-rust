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

use fluvio::{
    consumer::{ConsumerConfigExt, Record},
    dataplane::link::ErrorCode,
    Fluvio, FluvioConfig, FluvioError, Offset,
};
use futures::stream::unfold;
use futures::{Stream, StreamExt};

use swimos_agent::event_handler::{Either, EventHandler, UnitHandler};
use swimos_connector::{
    simple::{
        ConnectorHandlerContext, Relay, RelayError, SimpleConnectorAgent, SimpleConnectorHandler,
    },
    Connector, ConnectorStream,
};
use swimos_utilities::trigger::Sender;

#[derive(thiserror::Error, Debug)]
pub enum FluvioConnectorError {
    #[error(transparent)]
    Native(FluvioError),
    #[error(transparent)]
    Relay(#[from] RelayError),
}

impl FluvioConnectorError {
    fn other(msg: impl ToString) -> FluvioConnectorError {
        FluvioConnectorError::Native(FluvioError::Other(msg.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct FluvioConnectorConfiguration {
    pub topic: String,
    pub relay: Relay,
    pub fluvio: FluvioConfig,
    pub partition: u32,
    pub offset: Offset,
}

#[derive(Debug, Clone)]
pub struct FluvioConnector {
    config: FluvioConnectorConfiguration,
}

impl FluvioConnector {
    pub fn new(config: FluvioConnectorConfiguration) -> FluvioConnector {
        FluvioConnector { config }
    }
}

impl Connector<SimpleConnectorAgent> for FluvioConnector {
    type StreamError = FluvioConnectorError;

    fn create_stream(
        &self,
    ) -> Result<impl ConnectorStream<SimpleConnectorAgent, Self::StreamError>, Self::StreamError>
    {
        let FluvioConnector { config } = self;
        Ok(unfold(ConnectorState::Uninit(config.clone()), |state| {
            let fut = async move {
                match state {
                    ConnectorState::Uninit(config) => {
                        let FluvioConnectorConfiguration {
                            topic,
                            relay,
                            fluvio,
                            partition,
                            offset,
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
                                    Err(e) => {
                                        return Some((
                                            Err(FluvioConnectorError::other(e)),
                                            ConnectorState::Failed,
                                        ));
                                    }
                                };

                                match handle.consumer_with_config(consumer_config).await {
                                    Ok(consumer) => Some((
                                        Ok(Either::Left(UnitHandler::default())),
                                        ConnectorState::Running {
                                            fluvio: handle,
                                            consumer,
                                            relay,
                                        },
                                    )),
                                    Err(e) => Some((
                                        Err(FluvioConnectorError::other(e)),
                                        ConnectorState::Failed,
                                    )),
                                }
                            }
                            Err(e) => {
                                Some((Err(FluvioConnectorError::other(e)), ConnectorState::Failed))
                            }
                        }
                    }
                    ConnectorState::Running {
                        fluvio,
                        mut consumer,
                        relay,
                    } => match poll_dispatch(&mut consumer, relay.clone()).await {
                        Some(Ok(handler)) => Some((
                            Ok(Either::Right(handler)),
                            ConnectorState::Running {
                                fluvio,
                                consumer,
                                relay,
                            },
                        )),
                        Some(Err(e)) => Some((Err(e), ConnectorState::Failed)),
                        None => None,
                    },
                    ConnectorState::Failed => None,
                }
            };
            Box::pin(fut)
        }))
    }

    fn on_start(&self, init_complete: Sender) -> impl EventHandler<SimpleConnectorAgent> + '_ {
        let handler_context = ConnectorHandlerContext::default();
        handler_context.effect(|| {
            init_complete.trigger();
        })
    }

    fn on_stop(&self) -> impl EventHandler<SimpleConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

async fn poll_dispatch<C>(
    consumer: &mut C,
    relay: Relay,
) -> Option<Result<impl SimpleConnectorHandler, FluvioConnectorError>>
where
    C: Stream<Item = Result<Record, ErrorCode>> + Unpin,
{
    match consumer.next().await {
        Some(Ok(record)) => {
            let handler_context = ConnectorHandlerContext::default();
            let result = relay
                .on_record(
                    record.key().unwrap_or_default(),
                    record.value(),
                    handler_context,
                )
                .map_err(Into::into);
            Some(result)
        }
        Some(Err(code)) => Some(Err(FluvioConnectorError::Native(FluvioError::Other(
            code.to_string(),
        )))),
        None => None,
    }
}

enum ConnectorState<C> {
    Uninit(FluvioConnectorConfiguration),
    Running {
        fluvio: Fluvio,
        consumer: C,
        relay: Relay,
    },
    Failed,
}
