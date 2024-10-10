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

use std::{cell::RefCell, future::Future, time::Duration};

use futures::{stream::unfold, Stream, StreamExt};
use rumqttc::{
    AsyncClient, ClientError, Event, EventLoop, Incoming, MqttOptions, Publish, QoS,
    SubscribeFilter,
};
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
    MqttIngressConfiguration, Subscription,
};

pub struct MqttIngressConnector {
    configuration: MqttIngressConfiguration,
    lanes: RefCell<Option<ConnectorLanes>>,
}

impl BaseConnector for MqttIngressConnector {
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

impl IngressConnector for MqttIngressConnector {
    type Error = MqttConnectorError;

    fn create_stream(&self) -> Result<impl ConnectorStream<Self::Error>, Self::Error> {
        let MqttIngressConnector {
            configuration,
            lanes,
        } = self;
        let lanes = lanes
            .borrow_mut()
            .take()
            .ok_or(MqttConnectorError::NotInitialized)?;

        let (client, event_loop) = super::open_client(
            &configuration.url,
            configuration.keep_alive_secs,
            configuration.max_packet_size,
            configuration.client_channel_size,
            None,
        )?;
        let sub_task = Box::pin(subscribe(configuration.subscription.clone(), client));
        let pub_stream = SubscriptionStream::new(event_loop, sub_task).into_stream();
        let handler_stream =
            pub_stream.map(move |result| result.map(|publish| lanes.handle_message(publish)));

        Ok(Box::pin(handler_stream))
    }

    fn initialize(&self, context: &mut dyn IngressContext) -> Result<(), Self::Error> {
        let MqttIngressConnector {
            configuration,
            lanes,
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

async fn subscribe(sub: Subscription, client: AsyncClient) -> Result<AsyncClient, ClientError> {
    match sub {
        Subscription::Topic(topic) => {
            client.subscribe(topic, QoS::AtLeastOnce).await?;
        }
        Subscription::Topics(topics) => {
            for topic in topics {
                client.subscribe(topic, QoS::AtLeastOnce).await?;
            }
        }
        Subscription::Filters(filters) => {
            let sub_filters = filters
                .into_iter()
                .map(|s| SubscribeFilter::new(s, QoS::AtLeastOnce));
            client.subscribe_many(sub_filters).await?;
        }
    }
    Ok(client)
}

pub enum SubscriptionState<F> {
    Subscribing { sub_task: F },
    Running { _client: AsyncClient },
}

pub struct SubscriptionStream<F> {
    state: SubscriptionState<F>,
    event_loop: EventLoop,
}

impl<F> SubscriptionStream<F>
where
    F: Future<Output = Result<AsyncClient, ClientError>> + Unpin + Send + 'static,
{
    fn new(event_loop: EventLoop, sub_task: F) -> Self {
        SubscriptionStream {
            state: SubscriptionState::Subscribing { sub_task },
            event_loop,
        }
    }

    fn into_stream(
        self,
    ) -> impl Stream<Item = Result<Publish, MqttConnectorError>> + Send + 'static {
        unfold(self, |mut s| async move {
            let result = s.next().await;
            Some((result, s))
        })
    }

    async fn next(&mut self) -> Result<Publish, MqttConnectorError> {
        let SubscriptionStream { state, event_loop } = self;
        let publish = loop {
            match state {
                SubscriptionState::Subscribing { sub_task } => {
                    tokio::select! {
                        biased;
                        result = sub_task => {
                            *state = SubscriptionState::Running { _client: result? };
                        }
                        result = event_loop.poll() => {
                            if let Event::Incoming(Incoming::Publish(publish)) = result? {
                                break publish;
                            }
                        }
                    }
                }
                SubscriptionState::Running { .. } => {
                    if let Event::Incoming(Incoming::Publish(publish)) = event_loop.poll().await? {
                        break publish;
                    }
                }
            }
        };

        Ok(publish)
    }
}

struct ConnectorLanes {}

impl ConnectorLanes {
    fn handle_message(
        &self,
        publish: Publish,
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
