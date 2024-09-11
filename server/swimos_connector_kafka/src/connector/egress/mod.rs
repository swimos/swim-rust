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

use std::collections::HashMap;

use rdkafka::error::KafkaError;
use swimos_agent::event_handler::{EventHandler, UnitHandler};
use swimos_api::address::Address;
use swimos_connector::{
    BaseConnector, ConnectorAgent, ConnectorFuture, EgressConnector, EgressConnectorSender,
};
use swimos_model::Value;
use swimos_utilities::trigger::Sender;

use crate::{
    config::{EgressValueLaneSpec, KafkaEgressConfiguration},
    facade::{KafkaProducer, ProducerFactory}, selector::MessageSelector, BadSelector,
};

pub struct KafkaEgressConnector<F> {
    factory: F,
    configuration: KafkaEgressConfiguration,
}

impl<F> BaseConnector for KafkaEgressConnector<F>
where
    F: ProducerFactory + Send + 'static,
{
    fn on_start(&self, init_complete: Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

impl<F> EgressConnector for KafkaEgressConnector<F>
where
    F: ProducerFactory + Send + 'static,
{
    type SendError = KafkaError;

    type Sender = ProducerSender<F::Producer>;

    fn make_sender(
        &self,
        agent_params: &HashMap<String, String>,
    ) -> Result<Self::Sender, Self::SendError> {
        todo!()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProducerSender<P> {
    producer: P,
}

impl<P> EgressConnectorSender<KafkaError> for ProducerSender<P>
where
    P: KafkaProducer + Clone + Send + Sync + 'static,
{
    fn send(
        &self,
        name: &str,
        key: Option<&Value>,
        value: &Value,
    ) -> impl ConnectorFuture<KafkaError> {
        Box::pin(async { Ok(UnitHandler::default()) })
    }
}

pub struct Extractors {
    value_lanes: HashMap<String, MessageSelector>,
    map_lanes: HashMap<String, MessageSelector>,
    value_downlinks: HashMap<Address<String>, MessageSelector>,
    map_downlinks: HashMap<Address<String>, MessageSelector>,
}

impl TryFrom<&KafkaEgressConfiguration> for Extractors {
    type Error = BadSelector;

    fn try_from(value: &KafkaEgressConfiguration) -> Result<Self, Self::Error> {
        let KafkaEgressConfiguration { fixed_topic, value_lanes, map_lanes, value_downlinks, map_downlinks, .. } = value;
        let top = fixed_topic.as_ref().map(|s| s.as_str()).unwrap_or_default();
        let value_lanes = value_lanes.iter().map(|EgressValueLaneSpec { name, extractor }| MessageSelector::try_from_ext_spec(extractor, top).map(|selector| (name.clone(), selector))).collect::<Result<HashMap<_, _>, _>>()?;
        todo!()
    }
}
