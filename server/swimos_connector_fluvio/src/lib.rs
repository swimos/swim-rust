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

pub mod generic;
pub mod relay;

use crate::generic::GenericModel;
use crate::relay::RelayModel;
use fluvio::consumer::{ConsumerConfigExt, ConsumerStream};
use fluvio::dataplane::link::ErrorCode;
use fluvio::dataplane::record::ConsumerRecord;
use fluvio::{Fluvio, FluvioConfig, FluvioError, Offset};
use swimos_connector::generic::{
    DerserializerLoadError, DeserializationFormat, InvalidLanes, LaneSelectorError, MapLaneSpec,
    ValueLaneSpec,
};
use swimos_connector::relay::RelayError;

/// Errors that can be produced by the Fluvio connector.
#[derive(thiserror::Error, Debug)]
pub enum FluvioConnectorError {
    /// Fluvio Library Error.
    #[error(transparent)]
    Native(FluvioError),
    /// An error produced by the configured relay.
    #[error(transparent)]
    Relay(#[from] RelayError),
    /// Failed to load the deserializers required to interpret the Fluvio messages.
    #[error(transparent)]
    Configuration(#[from] DerserializerLoadError),
    /// Attempting to select the required components of a Fluvio message failed.
    #[error(transparent)]
    Lane(#[from] LaneSelectorError),
}

impl FluvioConnectorError {
    fn other(msg: impl ToString) -> FluvioConnectorError {
        FluvioConnectorError::Native(FluvioError::Other(msg.to_string()))
    }
}

/// Configuration parameters for the Fluvio connector.
#[derive(Debug, Clone)]
pub struct FluvioConnectorConfiguration {
    /// The topic to consume from.
    pub topic: String,
    /// Fluvio library configuration.
    pub fluvio: FluvioConfig,
    /// The partition to consume from.
    pub partition: u32,
    /// The offset to start consuming from.
    pub offset: Offset,
}

/// A [connector](`Connector`) to ingest a stream of Fluvio record into a Swim application. This
/// should be used to provide a lifecycle for either a [`GenericConnectorAgent`] or a [`RelayConnectorAgent`].
#[derive(Debug, Clone)]
pub struct FluvioConnector<R> {
    inner: R,
}

impl<I> FluvioConnector<I> {
    pub fn new(inner: I) -> FluvioConnector<I> {
        FluvioConnector { inner }
    }
}

impl<R> FluvioConnector<RelayModel<R>> {
    /// Constructs a new Fluvio connector which will operate as a [`Relay`].
    ///
    /// # Arguments
    /// * `config` - Fluvio connector configuration.
    /// * `relay` - the [`Relay`] which will be invoked each time a record is received.
    pub fn relay(config: FluvioConnectorConfiguration, relay: R) -> FluvioConnector<RelayModel<R>> {
        FluvioConnector::new(RelayModel::new(config, relay))
    }
}

impl FluvioConnector<GenericModel> {
    /// Constructs a new Fluvio connector which will operate as a [`GenericConnectorAgent`]. Returns
    /// either a Fluvio connector or an error if there are too many lanes or overlapping lane URIs.
    ///
    /// # Arguments
    /// * `config` - Fluvio connector configuration.
    /// * `key_deserializer` - deserializer for keys.
    /// * `payload_deserializer` - deserializer for payloads.
    /// * `value_lanes` - specification of the value lanes for the connector.
    /// * `map_lanes` - specification of the map lane for the connector.
    pub fn generic(
        config: FluvioConnectorConfiguration,
        key_deserializer: DeserializationFormat,
        payload_deserializer: DeserializationFormat,
        value_lanes: Vec<ValueLaneSpec>,
        map_lanes: Vec<MapLaneSpec>,
    ) -> Result<FluvioConnector<GenericModel>, InvalidLanes> {
        Ok(FluvioConnector::new(GenericModel::new(
            config,
            key_deserializer,
            payload_deserializer,
            value_lanes,
            map_lanes,
        )?))
    }
}

async fn open(
    config: FluvioConnectorConfiguration,
) -> Result<
    (
        Fluvio,
        impl ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + Sized,
    ),
    FluvioConnectorError,
> {
    let FluvioConnectorConfiguration {
        topic,
        fluvio,
        partition,
        offset,
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
                    return Err(FluvioConnectorError::other(e));
                }
            };

            match handle.consumer_with_config(consumer_config).await {
                Ok(consumer) => Ok((handle, consumer)),
                Err(e) => Err(FluvioConnectorError::other(e)),
            }
        }
        Err(e) => Err(FluvioConnectorError::other(e)),
    }
}
