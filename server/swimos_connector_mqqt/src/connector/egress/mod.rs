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

use std::{cell::RefCell, collections::HashMap, future::Future};

use bytes::{BufMut, BytesMut};
use futures::future::Ready;
use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop, QoS};
use std::io::Write;
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt, TryHandlerActionExt, UnitHandler},
};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorFuture, EgressConnector, EgressConnectorSender,
    EgressContext, MessageSource, SendResult,
};
use swimos_model::Value;
use swimos_recon::print_recon_compact;
use swimos_utilities::trigger;
use tracing::trace;

use crate::{MqttConnectorError, MqttEgressConfiguration};

pub struct MqttEgressConnector {
    configuration: MqttEgressConfiguration,
    inner: RefCell<Option<Inner>>,
}

struct Inner {
    sender: MqttSender,
    event_loop: Option<EventLoop>,
}

impl BaseConnector for MqttEgressConnector {
    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let MqttEgressConnector { inner, .. } = self;
        let context: HandlerContext<ConnectorAgent> = Default::default();
        let mut guard = inner.borrow_mut();
        let event_loop_result = guard
            .as_mut()
            .and_then(|inner| inner.event_loop.take())
            .ok_or(MqttConnectorError::NotInitialized);
        context
            .value(event_loop_result)
            .try_handler()
            .and_then(move |event_loop| {
                context.suspend(async move {
                    let _ = init_complete.trigger();
                    drive_events(event_loop).await
                })
            })
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

#[derive(Debug)]
pub struct MqttSender {
    client: AsyncClient,
    retain: bool,
    buffer: RefCell<BytesMut>,
}

impl MqttSender {
    fn new(client: AsyncClient) -> Self {
        MqttSender {
            client,
            retain: false,
            buffer: Default::default(),
        }
    }
}

impl Clone for MqttSender {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            retain: self.retain,
            buffer: Default::default(),
        }
    }
}

impl EgressConnector for MqttEgressConnector {
    type Error = MqttConnectorError;

    type Sender = MqttSender;

    fn initialize(&self, context: &mut dyn EgressContext) -> Result<(), Self::Error> {
        let MqttEgressConnector {
            configuration,
            inner,
        } = self;
        let MqttEgressConfiguration {
            url,
            value_lanes,
            map_lanes,
            value_downlinks,
            map_downlinks,
            keep_alive_secs,
            max_packet_size,
            max_inflight,
            client_channel_size,
            ..
        } = configuration;
        for lane in value_lanes {
            context.open_lane(&lane.name, WarpLaneKind::Value);
        }
        for lane in map_lanes {
            context.open_lane(&lane.name, WarpLaneKind::Map);
        }
        let (client, event_loop) = super::open_client(
            url,
            *keep_alive_secs,
            *max_packet_size,
            *client_channel_size,
            *max_inflight,
        )?;
        let mut guard = inner.borrow_mut();
        *guard = Some(Inner {
            sender: MqttSender::new(client),
            event_loop: Some(event_loop),
        });
        Ok(())
    }

    fn make_sender(
        &self,
        _agent_params: &HashMap<String, String>,
    ) -> Result<Self::Sender, Self::Error> {
        self.inner
            .borrow()
            .as_ref()
            .ok_or(MqttConnectorError::NotInitialized)
            .map(|inner| inner.sender.clone())
    }
}

type NoResponse =
    Option<SendResult<Ready<Result<UnitHandler, MqttConnectorError>>, MqttConnectorError>>;

impl EgressConnectorSender<MqttConnectorError> for MqttSender {
    fn send(
        &self,
        source: MessageSource<'_>,
        key: Option<&Value>,
        value: &Value,
    ) -> Option<SendResult<impl ConnectorFuture<MqttConnectorError>, MqttConnectorError>> {
        Some(SendResult::Suspend(Box::pin(
            self.publish(source, key, value),
        )))
    }

    fn timer_event(
        &self,
        _timer_id: u64,
    ) -> Option<SendResult<impl ConnectorFuture<MqttConnectorError>, MqttConnectorError>> {
        None as NoResponse
    }
}

impl MqttSender {
    fn select_topic<'a>(
        &'a self,
        source: MessageSource<'_>,
        key: Option<&'a Value>,
        value: &'a Value,
    ) -> &str {
        todo!()
    }

    fn select_payload<'a>(
        &'a self,
        source: MessageSource<'_>,
        key: Option<&'a Value>,
        value: &'a Value,
    ) -> &Value {
        todo!()
    }

    fn publish(
        &self,
        source: MessageSource<'_>,
        key: Option<&Value>,
        value: &Value,
    ) -> impl Future<Output = Result<UnitHandler, MqttConnectorError>> + Send + 'static {
        let MqttSender {
            client,
            retain,
            buffer,
        } = self;
        let topic = self.select_topic(source, key, value);
        let payload = self.select_payload(source, key, value);
        let bytes = {
            let mut guard = buffer.borrow_mut();
            let mut writer = (&mut *guard).writer();
            write!(&mut writer, "{}", print_recon_compact(&payload))
                .expect("Serailization should be infallible.");
            guard.split().freeze()
        };
        let topic_string = topic.to_string();
        let retain_msg = *retain;
        let client_cpy = client.clone();
        async move {
            let result = client_cpy
                .publish_bytes(topic_string, QoS::AtLeastOnce, retain_msg, bytes)
                .await;
            result.map(|_| UnitHandler::default()).map_err(Into::into)
        }
    }
}

async fn drive_events(
    mut event_loop: EventLoop,
) -> impl EventHandler<ConnectorAgent> + Send + 'static {
    let result = loop {
        match event_loop.poll().await {
            Ok(Event::Outgoing(event)) => {
                trace!(event = ?event, "Outgoing MQTT event.");
            }
            Err(ConnectionError::RequestsDone) => break Ok(()),
            Err(err) => break Err(err),
            _ => {}
        }
    };
    let context: HandlerContext<ConnectorAgent> = Default::default();
    context.value(result).try_handler()
}
