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

mod lanes;
#[cfg(test)]
mod tests;
use frunk::hlist;
pub use lanes::*;
use swimos_agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
use swimos_model::Value;
use tracing::trace;

use crate::{
    deser::{BoxMessageDeserializer, Deferred, MessageView},
    selector::{PubSubSelector, Relays, SelectHandler},
    ConnectorAgent, SelectorError,
};

// Uses the information about the lanes of the agent to convert messages into event handlers that update the lanes.
pub struct MessageSelector {
    key_deserializer: BoxMessageDeserializer,
    value_deserializer: BoxMessageDeserializer,
    lanes: Lanes<PubSubSelector>,
    relays: Relays<PubSubSelector>,
}

impl MessageSelector {
    pub fn new(
        key_deserializer: BoxMessageDeserializer,
        value_deserializer: BoxMessageDeserializer,
        lanes: Lanes<PubSubSelector>,
        relays: Relays<PubSubSelector>,
    ) -> Self {
        MessageSelector {
            key_deserializer,
            value_deserializer,
            lanes,
            relays,
        }
    }

    pub fn handle_message<'a>(
        &self,
        message: &'a MessageView<'a>,
    ) -> Result<impl EventHandler<ConnectorAgent> + Send + 'static, SelectorError> {
        let MessageSelector {
            key_deserializer,
            value_deserializer,
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
            let key = Deferred::new(message.key, key_deserializer);
            let value = Deferred::new(message.payload, value_deserializer);
            let mut args = hlist![topic, key, value];

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
