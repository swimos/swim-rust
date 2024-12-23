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

use futures::{
    stream::{once, unfold},
    Stream, StreamExt, TryStream, TryStreamExt,
};
use rumqttc::{ClientError, MqttOptions};
use selector::{Lanes, MqttMessageSelector, Relays};
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, UnitHandler},
};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::{
    config::format::DataFormat, BaseConnector, ConnectorAgent, ConnectorStream, IngressConnector,
    IngressContext,
};
use swimos_utilities::trigger::Sender;
use tracing::error;

use crate::{
    config::Credentials,
    error::MqttConnectorError,
    facade::{ConsumerFactory, MqttConsumer, MqttFactory, MqttMessage, MqttSubscriber},
    MqttIngressConfiguration, Subscription,
};

mod selector;
#[cfg(test)]
mod tests;

use super::DEFAULT_CHANNEL_SIZE;

/// A [connector](IngressConnector) to ingest a stream of MQTT messages into a Swim application. This should be used to
/// provide a lifecycle for a [connector agent](ConnectorAgent).
///
/// The details of the MQTT broker and the topics to subscribe to are provided through the
/// [configuration](MqttIngressConfiguration) which also includes descriptors of the lanes that the agent should
/// expose. When the agent starts, the connector will register all of the lanes specified in the configuration and
/// then attempt to open an MQTT client, which will be spawned into the agent's own task. Each time an MQTT message
/// is received, an event handler will be executed in the agent which updates the states of the lanes and relays commands
/// to other agents, according to the specification provided in the configuration.
///
/// If a message is received that is invalid with respect to the configuration, the entire agent will fail with an
/// error.
pub struct MqttIngressConnector<F> {
    factory: F,
    configuration: MqttIngressConfiguration,
    lanes_and_relays: RefCell<(Lanes, Relays)>,
}

impl<F> MqttIngressConnector<F> {
    fn new(factory: F, configuration: MqttIngressConfiguration) -> Self {
        MqttIngressConnector {
            factory,
            configuration,
            lanes_and_relays: Default::default(),
        }
    }
}

impl MqttIngressConnector<MqttFactory> {
    /// Create an [`MqttIngressConnector`] with the provided configuration. The configuration is only validated when
    /// the agent attempts to start so this will never fail.
    ///
    /// # Arguments
    /// * `configuration` - The connector configuration, specifying the connection details for the MQTT client,
    ///   the lanes that the connector agent should expose and the remotes lanes that commands should be relayed to.
    pub fn for_config(configuration: MqttIngressConfiguration) -> Self {
        let channel_size = configuration.channel_size.unwrap_or(DEFAULT_CHANNEL_SIZE);
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
            lanes_and_relays,
        } = self;

        let (lanes, relays) = std::mem::take(&mut *lanes_and_relays.borrow_mut());

        let (client, consumer) = open_client(
            factory,
            &configuration.url,
            configuration.keep_alive_secs,
            configuration.max_packet_size,
            None,
            configuration.channel_size,
            configuration.credentials.clone(),
        )?;

        let handler_stream_fut = create_handler_stream(
            client,
            consumer,
            configuration.payload_deserializer.clone(),
            configuration.subscription.clone(),
            lanes,
            relays,
        );
        let handler_stream = once(handler_stream_fut).try_flatten();

        Ok(Box::pin(handler_stream))
    }

    fn initialize(&self, context: &mut dyn IngressContext) -> Result<(), Self::Error> {
        let MqttIngressConnector {
            configuration,
            lanes_and_relays,
            ..
        } = self;
        let mut guard = lanes_and_relays.borrow_mut();
        let relays = match Relays::try_from(configuration.relays.clone()) {
            Ok(relays) => relays,
            Err(err) => {
                error!(error = %err, "Failed to create relays for an MQTT connector.");
                return Err(err.into());
            }
        };
        match Lanes::try_from_lane_specs(&configuration.value_lanes, &configuration.map_lanes) {
            Ok(lanes_from_conf) => {
                for lane_spec in lanes_from_conf.value_lanes() {
                    context.open_lane(lane_spec.name(), WarpLaneKind::Value);
                }
                for lane_spec in lanes_from_conf.map_lanes() {
                    context.open_lane(lane_spec.name(), WarpLaneKind::Map);
                }
                *guard = (lanes_from_conf, relays);
            }
            Err(err) => {
                error!(error = %err, "Failed to create lanes for an MQTT connector.");
                return Err(err.into());
            }
        }
        Ok(())
    }
}

async fn create_handler_stream<M, Client, Consumer>(
    client: Client,
    consumer: Consumer,
    format: DataFormat,
    subscription: Subscription,
    lanes: Lanes,
    relays: Relays,
) -> Result<impl ConnectorStream<MqttConnectorError>, MqttConnectorError>
where
    M: MqttMessage + 'static,
    Client: MqttSubscriber + Send + 'static,
    Consumer: MqttConsumer<M> + Send + 'static,
{
    let sub_task = Box::pin(client.subscribe(subscription));
    let pub_stream =
        SubscriptionStream::new(Box::pin(consumer.into_stream()), sub_task).into_stream();

    let msg_deser = format.load_deserializer().await?;
    let message_sel = MqttMessageSelector::new(msg_deser, lanes, relays);
    let handlers = pub_stream.map(move |result| {
        result.and_then(|message| {
            message_sel
                .handle_message(&message)
                .map_err(MqttConnectorError::Selection)
        })
    });
    Ok(Box::pin(handlers))
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

fn open_client<F>(
    factory: &F,
    url: &str,
    keep_alive_secs: Option<u64>,
    max_packet_size: Option<usize>,
    max_inflight: Option<u32>,
    channel_size: Option<usize>,
    credentials: Option<Credentials>,
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
    if let Some(n) = channel_size {
        opts.set_request_channel_capacity(n);
    }
    if let Some(Credentials { username, password }) = credentials {
        opts.set_credentials(username, password);
    }
    Ok(factory.create(opts))
}
