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

use std::{cell::RefCell, future::Future};

use futures::{stream::unfold, Stream, StreamExt, TryStream, TryStreamExt};
use rumqttc::ClientError;
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorStream, IngressConnector, IngressContext,
};
use swimos_utilities::trigger::Sender;

use crate::{
    error::{InvalidLanes, MqttConnectorError},
    facade::{ConsumerFactory, MqttConsumer, MqttMessage, MqttSubscriber},
    MqttIngressConfiguration, Subscription,
};

pub struct MqttIngressConnector<F> {
    factory: F,
    configuration: MqttIngressConfiguration,
    lanes: RefCell<Option<ConnectorLanes>>,
}

impl<F> BaseConnector for MqttIngressConnector<F> {
    fn on_start(&self, init_complete: Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let context: HandlerContext<ConnectorAgent> = Default::default();
        context.effect(move || {
            let _ = init_complete.trigger();
        })
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

impl<F> IngressConnector for MqttIngressConnector<F>
where
    F: ConsumerFactory,
{
    type Error = MqttConnectorError;

    fn create_stream(&self) -> Result<impl ConnectorStream<Self::Error>, Self::Error> {
        let MqttIngressConnector {
            factory,
            configuration,
            lanes,
        } = self;
        let lanes = lanes
            .borrow_mut()
            .take()
            .ok_or(MqttConnectorError::NotInitialized)?;

        let (client, consumer) = super::open_client2(
            factory,
            &configuration.url,
            configuration.keep_alive_secs,
            configuration.max_packet_size,
            None,
        )?;

        let sub_task = Box::pin(client.subscribe(configuration.subscription.clone()));
        let pub_stream =
            SubscriptionStream::new(Box::pin(consumer.into_stream()), sub_task).into_stream();
        let handler_stream =
            pub_stream.map(move |result| result.map(|publish| lanes.handle_message(publish)));

        Ok(Box::pin(handler_stream))
    }

    fn initialize(&self, context: &mut dyn IngressContext) -> Result<(), Self::Error> {
        let MqttIngressConnector {
            configuration,
            lanes,
            ..
        } = self;
        *lanes.borrow_mut() = Some(ConnectorLanes::try_from(configuration)?);
        let MqttIngressConfiguration {
            value_lanes,
            map_lanes,
            ..
        } = configuration;
        for lane in value_lanes {
            context.open_lane(&lane.name, WarpLaneKind::Value);
        }
        for lane in map_lanes {
            context.open_lane(&lane.name, WarpLaneKind::Map);
        }
        Ok(())
    }
}

pub enum SubscriptionState<F, AC> {
    Subscribing { sub_task: F },
    Running { _client: AC },
}

pub struct SubscriptionStream<F, AC, S> {
    state: SubscriptionState<F, AC>,
    consumer_stream: S,
}

impl<F, S, AC> SubscriptionStream<F, AC, S>
where
    F: Future<Output = Result<AC, ClientError>> + Unpin + Send + 'static,
    S: TryStream + Unpin + Send + 'static,
    S::Error: Into<MqttConnectorError>,
    AC: Send + 'static,
{
    fn new(consumer_stream: S, sub_task: F) -> Self {
        SubscriptionStream {
            state: SubscriptionState::Subscribing { sub_task },
            consumer_stream,
        }
    }

    fn into_stream(self) -> impl Stream<Item = Result<S::Ok, MqttConnectorError>> + Send + 'static {
        unfold(self, |mut s| async move {
            let result = s.next().await;
            result.map(move |r| (r, s))
        })
    }

    async fn next(&mut self) -> Option<Result<S::Ok, MqttConnectorError>> {
        let SubscriptionStream {
            state,
            consumer_stream,
        } = self;
        loop {
            match state {
                SubscriptionState::Subscribing { sub_task } => {
                    tokio::select! {
                        biased;
                        result = sub_task => {
                            match result {
                                Ok(client) => {
                                    *state = SubscriptionState::Running { _client: client };
                                },
                                Err(err) => break Some(Err(err.into())),
                            }
                        }
                        result = consumer_stream.try_next() => {
                            break result.map_err(Into::into).transpose();
                        }
                    }
                }
                SubscriptionState::Running { .. } => {
                    break consumer_stream
                        .try_next()
                        .await
                        .map_err(Into::into)
                        .transpose();
                }
            }
        }
    }
}

struct ConnectorLanes {}

impl ConnectorLanes {
    fn handle_message<M: MqttMessage>(
        &self,
        publish: M,
    ) -> impl EventHandler<ConnectorAgent> + Send + 'static {
        UnitHandler::default()
    }
}

impl TryFrom<&MqttIngressConfiguration> for ConnectorLanes {
    type Error = InvalidLanes;

    fn try_from(value: &MqttIngressConfiguration) -> Result<Self, Self::Error> {
        Ok(ConnectorLanes {})
    }
}
