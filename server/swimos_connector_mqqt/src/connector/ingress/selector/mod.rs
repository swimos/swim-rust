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

use frunk::{hlist, Coprod};
use swimos_agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
use swimos_connector::{
    deser::{BoxMessageDeserializer, Deferred},
    selector::{PayloadSelector, SelectHandler, SelectorError, TopicSelector},
    ConnectorAgent,
};
use swimos_model::Value;
use tracing::trace;

use crate::facade::MqttMessage;

#[cfg(test)]
mod tests;

pub type MqttSelector = Coprod!(TopicSelector, PayloadSelector);

pub type Lanes = swimos_connector::ingress::Lanes<MqttSelector>;
pub type Relays = swimos_connector::selector::Relays<MqttSelector>;

pub struct MqttMessageSelector {
    payload_deserializer: BoxMessageDeserializer,
    lanes: Lanes,
    relays: Relays,
}

impl MqttMessageSelector {
    pub fn new(payload_deserializer: BoxMessageDeserializer, lanes: Lanes, relays: Relays) -> Self {
        MqttMessageSelector {
            payload_deserializer,
            lanes,
            relays,
        }
    }

    pub fn handle_message<M>(
        &self,
        message: &M,
    ) -> Result<impl EventHandler<ConnectorAgent> + Send + 'static, SelectorError>
    where
        M: MqttMessage + 'static,
    {
        let MqttMessageSelector {
            payload_deserializer,
            lanes,
            relays,
        } = self;

        let value_lanes = lanes.value_lanes();
        let map_lanes = lanes.map_lanes();

        trace!(topic = { message.topic() }, "Handling a message.");

        let mut value_lane_handlers = Vec::with_capacity(value_lanes.len());
        let mut map_lane_handlers = Vec::with_capacity(map_lanes.len());
        let mut relay_handlers = Vec::with_capacity(relays.len());

        {
            let topic = Value::text(message.topic());
            let payload = Deferred::new(message.payload(), payload_deserializer);
            let mut args = hlist![topic, payload];

            for value_lane in value_lanes {
                value_lane_handlers.push(value_lane.select_handler(&mut args)?);
            }
            for map_lane in map_lanes {
                map_lane_handlers.push(map_lane.select_handler(&mut args)?);
            }
            for relay in relays {
                relay_handlers.push(relay.select_handler(&mut args)?);
            }
        }

        let handler = Sequentially::new(value_lane_handlers)
            .followed_by(Sequentially::new(map_lane_handlers))
            .followed_by(Sequentially::new(relay_handlers));
        Ok(handler)
    }
}
