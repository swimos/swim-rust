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

use std::{cell::RefCell, collections::HashMap, future::Future, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::{future::Ready, FutureExt};
use rumqttc::{ClientError, MqttOptions};
use selector::MessageSelectors;
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt, TryHandlerActionExt, UnitHandler},
};
use swimos_api::agent::WarpLaneKind;
use swimos_connector::{
    ser::SharedMessageSerializer, BaseConnector, ConnectorAgent, ConnectorFuture, EgressConnector,
    EgressConnectorSender, EgressContext, LoadError, MessageSource, SendResult, SerializationError,
};
use swimos_model::Value;
use swimos_utilities::trigger;
use tokio::sync::oneshot;

use crate::{
    config::Credentials,
    facade::{MqttFactory, MqttPublisher, PublisherDriver, PublisherFactory},
    MqttConnectorError, MqttEgressConfiguration,
};

mod selector;

use super::DEFAULT_CHANNEL_SIZE;

pub struct MqttEgressConnector<F: PublisherFactory> {
    factory: F,
    configuration: MqttEgressConfiguration,
    inner: RefCell<Option<ConnectorState<F>>>,
}

impl<F: PublisherFactory> MqttEgressConnector<F> {
    fn new(factory: F, configuration: MqttEgressConfiguration) -> Self {
        MqttEgressConnector {
            factory,
            configuration,
            inner: Default::default(),
        }
    }
}

impl MqttEgressConnector<MqttFactory> {
    pub fn for_config(configuration: MqttEgressConfiguration) -> Self {
        let channel_size = configuration
            .client_channel_size
            .unwrap_or(DEFAULT_CHANNEL_SIZE);
        MqttEgressConnector::new(MqttFactory::new(channel_size), configuration)
    }
}

struct ConnectorState<F: PublisherFactory> {
    ser_tx: Option<oneshot::Sender<SharedMessageSerializer>>,
    serializer: Serializer,
    selectors: Arc<MessageSelectors>,
    sender: F::Publisher,
    driver: Option<F::Driver>,
}

impl<F: PublisherFactory> ConnectorState<F> {
    fn new(extractors: Arc<MessageSelectors>, sender: F::Publisher, driver: F::Driver) -> Self {
        let (ser_tx, ser_rx) = oneshot::channel();
        ConnectorState {
            ser_tx: Some(ser_tx),
            serializer: Serializer::Pending(ser_rx),
            selectors: extractors,
            sender,
            driver: Some(driver),
        }
    }
}

enum Serializer {
    Pending(oneshot::Receiver<SharedMessageSerializer>),
    Loaded(SharedMessageSerializer),
}

impl Serializer {
    fn get(&mut self) -> Option<&SharedMessageSerializer> {
        match self {
            Serializer::Pending(rx) => match rx.try_recv() {
                Ok(ser) => {
                    *self = Serializer::Loaded(ser);
                    match self {
                        Self::Loaded(loaded) => Some(loaded),
                        _ => None,
                    }
                }
                _ => None,
            },
            Serializer::Loaded(loaded) => Some(loaded),
        }
    }
}

impl<F> BaseConnector for MqttEgressConnector<F>
where
    F: PublisherFactory,
{
    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let MqttEgressConnector {
            inner,
            configuration,
            ..
        } = self;
        let context: HandlerContext<ConnectorAgent> = Default::default();
        let mut guard = inner.borrow_mut();
        let ConnectorState { ser_tx, driver, .. } = guard.as_mut().expect("Not initialized.");

        let ser_tx = ser_tx.take().expect("Sender taken twice.");
        let ser_fmt = configuration.payload_serializer.clone();
        let load_ser = async move { ser_fmt.load_serializer().await };
        let ser_fut = load_ser.map(move |loaded| {
            context
                .effect(move || match ser_tx.send(loaded?) {
                    Ok(_) => {
                        init_complete.trigger();
                        Ok(())
                    }
                    Err(_) => Err(LoadError::Cancelled),
                })
                .try_handler()
        });
        let suspend_ser = context.suspend(ser_fut);

        let driver_result = driver.take().ok_or(MqttConnectorError::NotInitialized);
        let suspend_driver =
            context
                .value(driver_result)
                .try_handler()
                .and_then(move |driver: F::Driver| {
                    context.suspend(async move {
                        let result = PublisherDriver::into_future(driver).await;
                        context.value(result).try_handler()
                    })
                });
        suspend_ser.followed_by(suspend_driver)
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

#[derive(Clone)]
pub struct MqttSender<P> {
    publisher: SerializingPublisher<P>,
    selectors: Arc<MessageSelectors>,
    retain: bool,
}

impl<P> MqttSender<P> {
    fn new(
        publisher: P,
        selectors: Arc<MessageSelectors>,
        payload_format: SharedMessageSerializer,
    ) -> Self {
        MqttSender {
            publisher: SerializingPublisher::new(publisher, payload_format),
            selectors,
            retain: false,
        }
    }
}

impl<F> EgressConnector for MqttEgressConnector<F>
where
    F: PublisherFactory,
{
    type Error = MqttConnectorError;

    type Sender = MqttSender<F::Publisher>;

    fn initialize(&self, context: &mut dyn EgressContext) -> Result<(), Self::Error> {
        let MqttEgressConnector {
            factory,
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
            credentials,
            ..
        } = configuration;
        for lane in value_lanes {
            context.open_lane(&lane.name, WarpLaneKind::Value);
        }
        for lane in map_lanes {
            context.open_lane(&lane.name, WarpLaneKind::Map);
        }
        for downlink in value_downlinks {
            context.open_event_downlink(downlink.address.borrow_parts());
        }
        for downlink in map_downlinks {
            context.open_map_downlink(downlink.address.borrow_parts());
        }
        let (publisher, driver) = open_client(
            factory,
            url,
            *keep_alive_secs,
            *max_packet_size,
            *max_inflight,
            credentials.clone(),
        )?;
        let mut guard = inner.borrow_mut();
        *guard = Some(ConnectorState::new(Default::default(), publisher, driver));
        Ok(())
    }

    fn make_sender(
        &self,
        _agent_params: &HashMap<String, String>,
    ) -> Result<Self::Sender, Self::Error> {
        let mut guard = self.inner.borrow_mut();
        let state = guard.as_mut().ok_or(MqttConnectorError::NotInitialized)?;
        let publisher = state.sender.clone();
        let payload_format = state
            .serializer
            .get()
            .ok_or(MqttConnectorError::NotInitialized)?
            .clone();
        let selectors = state.selectors.clone();
        Ok(MqttSender::new(publisher, selectors, payload_format))
    }
}

type NoResponse =
    Option<SendResult<Ready<Result<UnitHandler, MqttConnectorError>>, MqttConnectorError>>;

impl<P> EgressConnectorSender<MqttConnectorError> for MqttSender<P>
where
    P: MqttPublisher + Send + 'static,
{
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

impl<P> MqttSender<P>
where
    P: MqttPublisher + 'static,
{
    fn select_topic<'a>(
        &'a self,
        source: MessageSource<'_>,
        key: Option<&'a Value>,
        value: &'a Value,
    ) -> Option<&str> {
        let MqttSender { selectors, .. } = self;
        selectors
            .select_source(source)
            .and_then(|selector| selector.select_topic(key, value))
    }

    fn select_payload<'a>(
        &'a self,
        source: MessageSource<'_>,
        key: Option<&'a Value>,
        value: &'a Value,
    ) -> &Value {
        let MqttSender { selectors, .. } = self;
        selectors
            .select_source(source)
            .and_then(|selector| selector.select_payload(key, value))
            .unwrap_or(&Value::Extant)
    }

    fn publish(
        &self,
        source: MessageSource<'_>,
        key: Option<&Value>,
        value: &Value,
    ) -> impl Future<Output = Result<UnitHandler, MqttConnectorError>> + Send + 'static {
        let MqttSender {
            publisher, retain, ..
        } = self;
        let topic = self.select_topic(source, key, value).unwrap();
        let payload = self.select_payload(source, key, value);

        let topic_string = topic.to_string();
        let retain_msg = *retain;

        let fut_result = publisher.publish(topic_string, payload, retain_msg);
        async move {
            fut_result?.await?;
            Ok(UnitHandler::default())
        }
    }
}

fn open_client<F>(
    factory: &F,
    url: &str,
    keep_alive_secs: Option<u64>,
    max_packet_size: Option<usize>,
    max_inflight: Option<u32>,
    credentials: Option<Credentials>,
) -> Result<(F::Publisher, F::Driver), MqttConnectorError>
where
    F: PublisherFactory,
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
    if let Some(Credentials { username, password }) = credentials {
        opts.set_credentials(username, password);
    }
    Ok(factory.create(opts))
}

struct SerializingPublisher<P> {
    publisher: P,
    payload_format: SharedMessageSerializer,
    buffer: RefCell<BytesMut>,
}

impl<P: Clone> Clone for SerializingPublisher<P> {
    fn clone(&self) -> Self {
        Self {
            publisher: self.publisher.clone(),
            payload_format: self.payload_format.clone(),
            buffer: Default::default(),
        }
    }
}

impl<P> SerializingPublisher<P> {
    fn new(publisher: P, payload_format: SharedMessageSerializer) -> Self {
        SerializingPublisher {
            publisher,
            payload_format,
            buffer: Default::default(),
        }
    }
}

impl<P> SerializingPublisher<P>
where
    P: MqttPublisher + 'static,
{
    fn publish(
        &self,
        topic: String,
        payload: &Value,
        retain: bool,
    ) -> Result<impl Future<Output = Result<(), ClientError>> + Send + 'static, SerializationError>
    {
        let SerializingPublisher {
            publisher,
            payload_format,
            buffer,
        } = self;
        let mut guard = buffer.borrow_mut();
        let payload_buffer = &mut *guard;
        payload_buffer.clear();
        payload_format.serialize(payload, payload_buffer)?;
        Ok(publisher
            .clone()
            .publish(topic, payload_buffer.split().freeze(), retain))
    }
}
