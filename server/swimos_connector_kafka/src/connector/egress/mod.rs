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

use std::{cell::RefCell, collections::HashMap, sync::Arc, time::Duration};

use super::ConnHandlerContext;
use crate::selector::message::{MessageSelector, MessageSelectors};
use crate::{
    config::KafkaEgressConfiguration,
    facade::{KafkaFactory, KafkaProducer, ProduceResult, ProducerFactory},
    KafkaSenderError,
};
use bytes::BytesMut;
use futures::{channel::oneshot, FutureExt};
use swimos_agent::event_handler::{
    EventHandler, HandlerActionExt, TryHandlerActionExt, UnitHandler,
};
use swimos_connector::ser::SharedMessageSerializer;
use swimos_api::{address::Address, agent::WarpLaneKind};
use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorFuture, EgressConnector, EgressConnectorSender,
    EgressContext, LoadError, MessageSource, SendResult, SerializationError,
};
use swimos_model::Value;
use swimos_utilities::trigger;

#[cfg(test)]
mod tests;

/// A [connector](EgressConnector) to export a stream values from a Swim agent to one or more Kafka topics. This
/// should be used to provide a lifecycle for a [connector agent](ConnectorAgent).
///
/// The details of the Kafka brokers and the topics to subscribe to are provided through the
/// [configuration](KafkaEgressConfiguration) which also includes descriptors of the lanes that the agent should
/// expose and downlinks that it should open to remote lanes. When the agent starts, the connector will register all
/// of the lanes and open all of the downlinks, specified in the configuration, and then attempt to open a Kafka producer.
/// Each time the state of one of the lanes changes, or a message is received on one of the downlinks, a message will
/// be generated and sent via the producer.
///
/// If the producer fails or a message cannot be serialized using the provided configuration, the agent will stop with
/// an error.
pub struct KafkaEgressConnector<F: ProducerFactory> {
    factory: F,
    configuration: KafkaEgressConfiguration,
    state: RefCell<Option<ConnectorState>>,
}

impl<F: ProducerFactory> KafkaEgressConnector<F> {
    pub fn new(factory: F, configuration: KafkaEgressConfiguration) -> Self {
        KafkaEgressConnector {
            factory,
            configuration,
            state: Default::default(),
        }
    }
}

impl KafkaEgressConnector<KafkaFactory> {
    /// Create a [`KafkaEgressConnector`] with the provided configuration. The configuration is only validated when
    /// the agent attempts to start so this will never fail.
    ///
    /// # Arguments
    /// * `configuration` - The connector configuration, specifying the connection details for the Kafka consumer
    ///   an the lanes that the connector agent should expose.
    pub fn for_config(configuration: KafkaEgressConfiguration) -> Self {
        Self::new(KafkaFactory, configuration)
    }
}

struct ConnectorState {
    serializers: Serializers,
    extractors: Arc<MessageSelectors>,
}

impl ConnectorState {
    fn new(rx: oneshot::Receiver<LoadedSerializers>, extractors: MessageSelectors) -> Self {
        ConnectorState {
            serializers: Serializers::Pending(rx),
            extractors: Arc::new(extractors),
        }
    }
}

struct LoadedSerializers {
    key_serializer: SharedMessageSerializer,
    payload_serializer: SharedMessageSerializer,
}

enum Serializers {
    Pending(oneshot::Receiver<LoadedSerializers>),
    Loaded(LoadedSerializers),
}

impl Serializers {
    fn get(&mut self) -> Option<&LoadedSerializers> {
        match self {
            Serializers::Pending(rx) => match rx.try_recv() {
                Ok(Some(ser)) => {
                    *self = Serializers::Loaded(ser);
                    match self {
                        Self::Loaded(loaded) => Some(loaded),
                        _ => None,
                    }
                }
                _ => None,
            },
            Serializers::Loaded(loaded) => Some(loaded),
        }
    }
}

async fn load_serializers(
    config: KafkaEgressConfiguration,
) -> Result<LoadedSerializers, LoadError> {
    let KafkaEgressConfiguration {
        key_serializer,
        payload_serializer,
        ..
    } = config;
    let key = key_serializer.load_serializer().await?;
    let payload = payload_serializer.load_serializer().await?;
    Ok(LoadedSerializers {
        key_serializer: key,
        payload_serializer: payload,
    })
}

impl<F> BaseConnector for KafkaEgressConnector<F>
where
    F: ProducerFactory + Send + 'static,
{
    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let KafkaEgressConnector {
            configuration,
            state,
            ..
        } = self;
        let context: ConnHandlerContext = Default::default();
        let (ser_tx, ser_rx) = oneshot::channel();
        let ser_fut = load_serializers(configuration.clone()).map(move |loaded| {
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

        let setup_agent = context
            .value(MessageSelectors::try_from(configuration))
            .try_handler()
            .and_then(move |extractors: MessageSelectors| {
                context.effect(move || {
                    *(state.borrow_mut()) = Some(ConnectorState::new(ser_rx, extractors));
                })
            });
        suspend_ser.followed_by(setup_agent)
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

impl<F> EgressConnector for KafkaEgressConnector<F>
where
    F: ProducerFactory + Send + 'static,
{
    type Error = KafkaSenderError;

    type Sender = KafkaSender<F::Producer>;

    fn make_sender(
        &self,
        _agent_params: &HashMap<String, String>,
    ) -> Result<Self::Sender, Self::Error> {
        let KafkaEgressConnector {
            factory,
            configuration,
            state,
        } = self;
        let KafkaEgressConfiguration {
            properties,
            log_level,
            retry_timeout_ms,
            ..
        } = configuration;
        let mut guard = state.borrow_mut();
        let ConnectorState {
            serializers,
            extractors,
        } = guard.as_mut().ok_or(KafkaSenderError::NotInitialized)?;
        let LoadedSerializers {
            key_serializer,
            payload_serializer,
        } = match serializers.get() {
            Some(ser) => ser,
            None => return Err(KafkaSenderError::NotInitialized),
        };
        let producer = factory.create(properties, *log_level)?;
        let ser_producer =
            SerializingProducer::new(producer, key_serializer.clone(), payload_serializer.clone());
        let sender = KafkaSender::new(
            ser_producer,
            extractors.clone(),
            Duration::from_millis(*retry_timeout_ms),
        );
        Ok(sender)
    }

    fn initialize(&self, context: &mut dyn EgressContext) -> Result<(), Self::Error> {
        open_lanes(&self.configuration, context);
        open_downlinks(&self.configuration, context);
        Ok(())
    }
}

fn open_lanes(config: &KafkaEgressConfiguration, context: &mut dyn EgressContext) {
    let KafkaEgressConfiguration {
        value_lanes,
        map_lanes,
        ..
    } = config;
    for value_lane in value_lanes {
        context.open_lane(&value_lane.name, WarpLaneKind::Value);
    }
    for map_lane in map_lanes {
        context.open_lane(&map_lane.name, WarpLaneKind::Map);
    }
}

fn open_downlinks(config: &KafkaEgressConfiguration, context: &mut dyn EgressContext) {
    let KafkaEgressConfiguration {
        value_downlinks,
        map_downlinks,
        ..
    } = config;
    for value_dl in value_downlinks {
        context.open_event_downlink(value_dl.address.borrow_as_addr());
    }
    for map_dl in map_downlinks {
        context.open_map_downlink(map_dl.address.borrow_as_addr());
    }
}

impl MessageSelectors {
    pub fn select_source(&self, source: MessageSource<'_>) -> Option<&MessageSelector> {
        match source {
            MessageSource::Lane(name) => self
                .value_lanes()
                .get(name)
                .or_else(|| self.map_lanes().get(name)),
            MessageSource::Downlink(addr) => self
                .value_downlinks()
                .get(addr)
                .or_else(|| self.map_downlinks().get(addr)),
        }
    }
}

#[derive(Default)]
struct Buffers {
    key_buffer: BytesMut,
    payload_buffer: BytesMut,
}

struct SerializingProducer<P> {
    producer: P,
    key_format: SharedMessageSerializer,
    payload_format: SharedMessageSerializer,
    buffers: RefCell<Buffers>,
}

impl<P: Clone> Clone for SerializingProducer<P> {
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            key_format: self.key_format.clone(),
            payload_format: self.payload_format.clone(),
            buffers: Default::default(),
        }
    }
}

impl<P> SerializingProducer<P> {
    fn new(
        producer: P,
        key_format: SharedMessageSerializer,
        payload_format: SharedMessageSerializer,
    ) -> Self {
        SerializingProducer {
            producer,
            key_format,
            payload_format,
            buffers: Default::default(),
        }
    }
}

impl<P> SerializingProducer<P>
where
    P: KafkaProducer + 'static,
{
    fn send(
        &self,
        topic: &str,
        key: &Value,
        payload: &Value,
    ) -> Result<Option<P::Fut>, SerializationError> {
        let SerializingProducer {
            producer,
            key_format,
            payload_format,
            buffers,
        } = self;
        let mut guard = buffers.borrow_mut();
        let Buffers {
            key_buffer,
            payload_buffer,
        } = &mut *guard;
        key_buffer.clear();
        payload_buffer.clear();
        key_format.serialize(key, key_buffer)?;
        payload_format.serialize(payload, payload_buffer)?;
        match producer.send(topic, Some(key_buffer.as_ref()), payload_buffer.as_ref()) {
            ProduceResult::ResultFuture(fut) => Ok(Some(fut)),
            ProduceResult::QueueFull => Ok(None),
        }
    }
}

pub struct KafkaSender<P> {
    producer: SerializingProducer<P>,
    extractors: Arc<MessageSelectors>,
    timeout: Duration,
    pending: RefCell<Pending>,
}

impl<P> KafkaSender<P> {
    fn new(
        producer: SerializingProducer<P>,
        extractors: Arc<MessageSelectors>,
        timeout: Duration,
    ) -> Self {
        KafkaSender {
            producer,
            extractors,
            timeout,
            pending: Default::default(),
        }
    }
}

impl<P> Clone for KafkaSender<P>
where
    P: Clone,
{
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            extractors: self.extractors.clone(),
            timeout: self.timeout,
            pending: Default::default(),
        }
    }
}

impl<P> EgressConnectorSender<KafkaSenderError> for KafkaSender<P>
where
    P: KafkaProducer + Clone + Send + 'static,
{
    fn send(
        &self,
        source: MessageSource<'_>,
        key: Option<&Value>,
        value: &Value,
    ) -> Option<SendResult<impl ConnectorFuture<KafkaSenderError>, KafkaSenderError>> {
        let KafkaSender {
            producer,
            extractors,
            timeout,
            pending,
        } = self;
        extractors.select_source(source).and_then(|selector| {
            selector.select_topic(key, value).and_then(|topic| {
                let msg_key = selector.select_key(key, value).unwrap_or(&Value::Extant);
                let payload = selector
                    .select_payload(key, value)
                    .unwrap_or(&Value::Extant);
                match producer.send(topic, msg_key, payload) {
                    Ok(Some(fut)) => Some(SendResult::Suspend(Box::pin(fut.map(|r| match r {
                        Ok(_) => Ok(UnitHandler::default()),
                        Err(err) => Err(KafkaSenderError::Kafka(err)),
                    })))),
                    Ok(None) => pending
                        .borrow_mut()
                        .push(
                            topic.to_string(),
                            source.into(),
                            key.cloned(),
                            value.clone(),
                        )
                        .map(|id| SendResult::RequestCallback(*timeout, id)),
                    Err(err) => Some(SendResult::Fail(KafkaSenderError::Serialization(err))),
                }
            })
        })
    }

    fn timer_event(
        &self,
        timer_id: u64,
    ) -> Option<SendResult<impl ConnectorFuture<KafkaSenderError>, KafkaSenderError>> {
        if let Some(record) = self.pending.borrow_mut().take(timer_id) {
            let (name, key, value) = record;
            self.send_owned(name, key, value)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
enum OwnedSource {
    Lane(String),
    Downlink(Address<String>),
}

impl OwnedSource {
    fn borrow_parts(&self) -> MessageSource<'_> {
        match self {
            OwnedSource::Lane(s) => MessageSource::Lane(s.as_str()),
            OwnedSource::Downlink(addr) => MessageSource::Downlink(addr),
        }
    }
}

impl<'a> From<MessageSource<'a>> for OwnedSource {
    fn from(value: MessageSource<'a>) -> Self {
        match value {
            MessageSource::Lane(s) => OwnedSource::Lane(s.to_string()),
            MessageSource::Downlink(addr) => OwnedSource::Downlink(addr.clone()),
        }
    }
}

impl<P> KafkaSender<P>
where
    P: KafkaProducer + Clone + Send + 'static,
{
    fn send_owned(
        &self,
        source: OwnedSource,
        key: Option<Value>,
        value: Value,
    ) -> Option<SendResult<impl ConnectorFuture<KafkaSenderError>, KafkaSenderError>> {
        let KafkaSender {
            producer,
            extractors,
            timeout,
            pending,
        } = self;
        if let Some(selector) = extractors.select_source(source.borrow_parts()) {
            let topic = if let Some(topic) = selector.select_topic(key.as_ref(), &value) {
                let msg_key = selector
                    .select_key(key.as_ref(), &value)
                    .unwrap_or(&Value::Extant);
                let payload = selector
                    .select_payload(key.as_ref(), &value)
                    .unwrap_or(&Value::Extant);
                match producer.send(topic, msg_key, payload) {
                    Ok(Some(fut)) => {
                        return Some(SendResult::Suspend(Box::pin(fut.map(|r| match r {
                            Ok(_) => Ok(UnitHandler::default()),
                            Err(err) => Err(KafkaSenderError::Kafka(err)),
                        }))))
                    }
                    Ok(None) => topic.to_string(),
                    Err(err) => {
                        return Some(SendResult::Fail(KafkaSenderError::Serialization(err)))
                    }
                }
            } else {
                return None;
            };
            pending
                .borrow_mut()
                .push(topic, source, key, value)
                .map(|id| SendResult::RequestCallback(*timeout, id))
        } else {
            None
        }
    }
}

type PendingRecord = (OwnedSource, Option<Value>, Value);

#[derive(Default, Debug)]
struct Pending {
    counter: u64,
    topics: HashMap<u64, String>,
    records: HashMap<String, PendingRecord>,
}

impl Pending {
    fn push(
        &mut self,
        topic: String,
        source: OwnedSource,
        key: Option<Value>,
        value: Value,
    ) -> Option<u64> {
        let Pending {
            counter,
            topics,
            records,
        } = self;
        if let Some((rec_name, rec_key, rec_value)) = records.get_mut(&topic) {
            *rec_name = source;
            *rec_key = key;
            *rec_value = value;
            None
        } else {
            let id = *counter;
            *counter += 1;
            topics.insert(id, topic.clone());
            records.insert(topic, (source, key, value));
            Some(id)
        }
    }

    fn take(&mut self, id: u64) -> Option<PendingRecord> {
        let Pending {
            topics, records, ..
        } = self;
        topics.remove(&id).and_then(|topic| records.remove(&topic))
    }
}
