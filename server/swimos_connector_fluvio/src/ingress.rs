use crate::FluvioConnectorError;
use fluvio::consumer::{ConsumerConfigExt, ConsumerStream, Record};
use fluvio::dataplane::link::ErrorCode;
use fluvio::dataplane::record::ConsumerRecord;
use fluvio::{Fluvio, FluvioConfig, FluvioError, Offset};
use futures::stream::unfold;
use futures::Stream;
use futures::StreamExt;
use std::cell::RefCell;
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::event_handler::{
    Either, EventHandler, HandlerActionExt, TryHandlerActionExt, UnitHandler,
};
use swimos_connector::config::format::DataFormat;
use swimos_connector::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use swimos_connector::deser::{BoxMessageDeserializer, MessageView};
use swimos_connector::ingress::{Lanes, MessageSelector};
use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorStream, IngressConnector, LoadError, Relays,
};
use swimos_utilities::trigger::Sender;
use tracing::{debug, error};

/// Configuration parameters for the Fluvio connector.
#[derive(Debug, Clone)]
pub struct FluvioIngressConnectorConfiguration {
    /// The topic to consume from.
    pub topic: String,
    /// Fluvio library configuration.
    pub fluvio: FluvioConfig,
    /// The partition to consume from.
    pub partition: u32,
    /// The offset to start consuming from.
    pub offset: Offset,
    /// Specifications for the value lanes to define for the connector. This includes a pattern to
    /// define a selector that will pick out values to set to that lane, from a Fluvio message.
    pub value_lanes: Vec<IngressValueLaneSpec>,
    /// Specifications for the map lanes to define for the connector. This includes a pattern to
    /// define a selector that will pick out updates to apply to that lane, from a Fluvio message.
    pub map_lanes: Vec<IngressMapLaneSpec>,
    /// Deserialization format to use to interpret the contents of the keys of the Fluvio messages.
    pub key_deserializer: DataFormat,
    /// Deserialization format to use to interpret the contents of the payloads of the Fluvio
    /// messages.
    pub payload_deserializer: DataFormat,
    /// Collection of selectors used for forwarding messages to lanes on agents.
    pub relays: Relays,
}

/// A Fluivo ingress [connector](`swimos_connector::IngressConnector`) to ingest a stream of Fluvio
/// records into a Swim application.
#[derive(Debug, Clone)]
pub struct FluvioIngressConnector {
    configuration: FluvioIngressConnectorConfiguration,
    lanes: RefCell<Lanes>,
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
        let FluvioIngressConnectorConfiguration {
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
    Uninit(FluvioIngressConnectorConfiguration),
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
            let inner = record.into_inner();
            let view = MessageView {
                topic,
                key: inner.key().map(|k| k.as_ref()).unwrap_or_default(),
                payload: inner.value().as_ref(),
            };

            Some(message_selector.handle_message(&view).map_err(Into::into))
        }
        Some(Err(code)) => Some(Err(FluvioConnectorError::Native(FluvioError::Other(
            code.to_string(),
        )))),
        None => None,
    }
}

async fn open(
    config: FluvioIngressConnectorConfiguration,
) -> Result<
    (
        Fluvio,
        impl ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + Sized,
    ),
    FluvioConnectorError,
> {
    let FluvioIngressConnectorConfiguration {
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
                Err(e) => {
                    return Err(FluvioConnectorError::Message(e.to_string()));
                }
            };

            match handle.consumer_with_config(consumer_config).await {
                Ok(consumer) => Ok((handle, consumer)),
                Err(e) => Err(FluvioConnectorError::Message(e.to_string())),
            }
        }
        Err(e) => Err(FluvioConnectorError::Message(e.to_string())),
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
