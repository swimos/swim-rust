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

use fluvio::{consumer::Record, dataplane::link::ErrorCode, Fluvio, FluvioError};
use futures::{stream::unfold, Stream, StreamExt};

use crate::{open, FluvioConnector, FluvioConnectorConfiguration, FluvioConnectorError};
use swimos_agent::event_handler::{Either, EventHandler, UnitHandler};
use swimos_connector::{
    deserialization::MessageView,
    relay::Relay,
    relay::{ConnectorHandlerContext, RelayConnectorAgent},
    Connector, ConnectorStream,
};
use swimos_utilities::trigger::Sender;

/// Fluvio [`Relay`] model.
#[derive(Debug, Clone)]
pub struct RelayModel<R> {
    config: FluvioConnectorConfiguration,
    relay: R,
}

impl<R> RelayModel<R> {
    /// Constructs a Fluvio [`Relay`] model.
    ///
    /// # Arguments
    /// * `config` - Fluvio connector configuration.
    /// * `relay` - the [`Relay`] which will be invoked each time a record is received.
    pub fn new(config: FluvioConnectorConfiguration, relay: R) -> RelayModel<R> {
        RelayModel { config, relay }
    }
}

impl<R> Connector<RelayConnectorAgent> for FluvioConnector<RelayModel<R>>
where
    R: Relay + 'static,
{
    type StreamError = FluvioConnectorError;

    fn create_stream(
        &self,
    ) -> Result<impl ConnectorStream<RelayConnectorAgent, Self::StreamError>, Self::StreamError>
    {
        let FluvioConnector { inner } = self;
        let RelayModel { config, relay } = inner;
        let relay = relay.clone();

        Ok(unfold(
            ConnectorState::Uninit(config.clone()),
            move |state| {
                let relay = relay.clone();
                let fut = async move {
                    match state {
                        ConnectorState::Uninit(config) => {
                            let topic = config.topic.clone();
                            match open(config).await {
                                Ok((handle, consumer)) => Some((
                                    Ok(Either::Left(UnitHandler::default())),
                                    ConnectorState::Running {
                                        fluvio: handle,
                                        topic,
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
                        ConnectorState::Running {
                            fluvio,
                            topic,
                            mut consumer,
                            relay,
                        } => match poll_dispatch(&mut consumer, &relay, &topic).await {
                            Some(Ok(handler)) => Some((
                                Ok(Either::Right(handler)),
                                ConnectorState::Running {
                                    fluvio,
                                    topic,
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
            },
        ))
    }

    fn on_start(&self, init_complete: Sender) -> impl EventHandler<RelayConnectorAgent> + '_ {
        let handler_context = ConnectorHandlerContext::default();
        handler_context.effect(|| {
            init_complete.trigger();
        })
    }

    fn on_stop(&self) -> impl EventHandler<RelayConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

async fn poll_dispatch<C, R>(
    consumer: &mut C,
    relay: &R,
    topic: &str,
) -> Option<Result<R::Handler, FluvioConnectorError>>
where
    C: Stream<Item = Result<Record, ErrorCode>> + Unpin,
    R: Relay,
{
    match consumer.next().await {
        Some(Ok(record)) => {
            let handler_context = ConnectorHandlerContext::default();
            let inner = record.into_inner();
            let view = MessageView {
                topic,
                key: inner.key().map(|k| k.as_ref()).unwrap_or_default(),
                payload: inner.value().as_ref(),
            };

            Some(relay.on_record(view, handler_context).map_err(Into::into))
        }
        Some(Err(code)) => Some(Err(FluvioConnectorError::Native(FluvioError::Other(
            code.to_string(),
        )))),
        None => None,
    }
}

enum ConnectorState<C, R> {
    Uninit(FluvioConnectorConfiguration),
    Running {
        fluvio: Fluvio,
        topic: String,
        consumer: C,
        relay: R,
    },
    Failed,
}
