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

use futures::{stream::unfold, Stream, StreamExt, TryStream, TryStreamExt};
use rumqttc::{ClientError, MqttOptions, Publish};
use selector::Lanes;
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::{
    deser::MessageView,
    selector::{BadSelector, InvalidLaneSpec, InvalidLanes, KeySelector},
    BaseConnector, ConnectorAgent, ConnectorStream, IngressConnector, IngressContext,
};
use swimos_utilities::trigger::Sender;
use tracing::error;

use crate::{
    error::MqttConnectorError,
    facade::{ConsumerFactory, MqttConsumer, MqttFactory, MqttMessage, MqttSubscriber},
    MqttIngressConfiguration,
};

mod selector;

use super::DEFAULT_CHANNEL_SIZE;

pub struct MqttIngressConnector<F> {
    factory: F,
    configuration: MqttIngressConfiguration,
    lanes: RefCell<Lanes>,
}

impl<F> MqttIngressConnector<F> {
    fn new(factory: F, configuration: MqttIngressConfiguration) -> Self {
        MqttIngressConnector {
            factory,
            configuration,
            lanes: Default::default(),
        }
    }
}

impl MqttIngressConnector<MqttFactory> {
    pub fn for_config(configuration: MqttIngressConfiguration) -> Self {
        let channel_size = configuration
            .client_channel_size
            .unwrap_or(DEFAULT_CHANNEL_SIZE);
        MqttIngressConnector::new(MqttFactory::new(channel_size), configuration)
    }
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

        let lanes = std::mem::take(&mut *lanes.borrow_mut());

        let (client, consumer) = open_client(
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
            pub_stream.map(move |result| result.map(|publish| handle_message(&lanes, publish)));

        Ok(Box::pin(handler_stream))
    }

    fn initialize(&self, context: &mut dyn IngressContext) -> Result<(), Self::Error> {
        let MqttIngressConnector {
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
                error!(error = %err, "Failed to create lanes for a MQTT connector.");
                return Err(err.into());
            }
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

fn handle_message<M: MqttMessage>(
    lanes: &Lanes,
    publish: M,
) -> impl EventHandler<ConnectorAgent> + Send + 'static {
    let view = view(&publish);

    UnitHandler::default()
}

fn open_client<F>(
    factory: &F,
    url: &str,
    keep_alive_secs: Option<u64>,
    max_packet_size: Option<usize>,
    max_inflight: Option<u32>,
) -> Result<(F::Subscriber, F::Consumer), MqttConnectorError>
where
    F: ConsumerFactory,
{
    let mut opts = MqttOptions::parse_url(url)?;
    if let Some(t) = keep_alive_secs {
        opts.set_keep_alive(Duration::from_secs(t));
    }
    if let Some(n) = max_packet_size {
        opts.set_max_packet_size(n, n);
    }
    if let Some(n) = max_inflight {
        let max = u16::try_from(n).unwrap_or(u16::MAX);
        opts.set_inflight(max);
    }
    Ok(factory.create(opts))
}

fn view<M: MqttMessage>(message: &M) -> MessageView<'_> {
    MessageView {
        topic: message.topic(),
        key: &[],
        payload: message.payload(),
    }
}
