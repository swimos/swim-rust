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

use crate::{open, FluvioConnector, FluvioConnectorConfiguration, FluvioConnectorError};
use fluvio::{consumer::Record, dataplane::link::ErrorCode, Fluvio, FluvioError};
use futures::StreamExt;
use futures::{stream::unfold, Stream};
use swimos_agent::event_handler::{Either, EventHandler, UnitHandler};
use swimos_connector::{
    deserialization::{BoxMessageDeserializer, MessageView},
    generic::GenericConnectorAgent,
    generic::{
        DerserializerLoadError, DeserializationFormat, InvalidLanes, Lanes, MapLaneSpec,
        MessageSelector, ValueLaneSpec,
    },
    Connector, ConnectorStream,
};
use swimos_utilities::trigger::Sender;

/// Fluvio [`GenericConnectorAgent`] model.
#[derive(Debug, Clone)]
pub struct GenericModel {
    config: FluvioConnectorConfiguration,
    key_deserializer: DeserializationFormat,
    value_deserializer: DeserializationFormat,
    lanes: Lanes,
}

impl GenericModel {
    /// Constructs a new generic model which will operate as a [`GenericConnectorAgent`]. Returns
    /// either a generic model or an error if there are too many lanes or overlapping lane URIs.
    ///
    /// # Arguments
    /// * `config` - Fluvio connector configuration.
    /// * `key_deserializer` - deserializer for keys.
    /// * `payload_deserializer` - deserializer for payloads.
    /// * `value_lanes` - specification of the value lanes for the connector.
    /// * `map_lanes` - specification of the map lane for the connector.
    pub fn new(
        config: FluvioConnectorConfiguration,
        key_deserializer: DeserializationFormat,
        value_deserializer: DeserializationFormat,
        value_lanes: Vec<ValueLaneSpec>,
        map_lanes: Vec<MapLaneSpec>,
    ) -> Result<GenericModel, InvalidLanes> {
        Ok(GenericModel {
            config,
            key_deserializer,
            value_deserializer,
            lanes: Lanes::try_from_lane_specs(&value_lanes, &map_lanes)?,
        })
    }
}

impl Connector<GenericConnectorAgent> for FluvioConnector<GenericModel> {
    type StreamError = FluvioConnectorError;

    fn create_stream(
        &self,
    ) -> Result<impl ConnectorStream<GenericConnectorAgent, Self::StreamError>, Self::StreamError>
    {
        let FluvioConnector { inner } = self;
        let GenericModel {
            config,
            key_deserializer,
            value_deserializer,
            lanes,
        } = inner;

        let key_deser = key_deserializer.clone();
        let value_deser = value_deserializer.clone();
        let lanes = lanes.clone();
        let topic = config.topic.clone();

        Ok(unfold(
            ConnectorState::Uninit(config.clone()),
            move |state| {
                let topic = topic.clone();
                let key_deser = key_deser.clone();
                let value_deser = value_deser.clone();
                let lanes = lanes.clone();

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
                                        message_selector: MessageSelector::new(key, value, lanes),
                                    },
                                ))
                            }
                            Err(e) => {
                                Some((Err(FluvioConnectorError::other(e)), ConnectorState::Failed))
                            }
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

    fn on_start(&self, init_complete: Sender) -> impl EventHandler<GenericConnectorAgent> + '_ {
        let FluvioConnector { inner } = self;
        let GenericModel { lanes, .. } = inner;

        lanes.open_lanes(init_complete)
    }

    fn on_stop(&self) -> impl EventHandler<GenericConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

async fn poll_dispatch<C>(
    consumer: &mut C,
    topic: &str,
    message_selector: &MessageSelector,
) -> Option<Result<impl EventHandler<GenericConnectorAgent> + Send + 'static, FluvioConnectorError>>
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

enum ConnectorState<C> {
    Uninit(FluvioConnectorConfiguration),
    Running {
        fluvio: Fluvio,
        topic: String,
        consumer: C,
        message_selector: MessageSelector,
    },
    Failed,
}

async fn load_deserializers(
    key: DeserializationFormat,
    value: DeserializationFormat,
) -> Result<(BoxMessageDeserializer, BoxMessageDeserializer), DerserializerLoadError> {
    let key = key.load().await?;
    let value = value.load().await?;
    Ok((key, value))
}
