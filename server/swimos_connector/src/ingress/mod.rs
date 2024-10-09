mod lanes;

use crate::deser::{BoxMessageDeserializer, Deser, MessageView};
use crate::selector::{PubSubMapLaneSelector, PubSubValueLaneSelector, SelectHandler};
use crate::{ConnectorAgent, InvalidLanes, Relays, SelectorError};
use frunk::hlist;
pub use lanes::*;
use std::collections::HashSet;
use swimos_agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
use swimos_model::Value;
use tracing::trace;

fn check_selectors(
    value_selectors: &[PubSubValueLaneSelector],
    map_selectors: &[PubSubMapLaneSelector],
) -> Result<(), InvalidLanes> {
    let mut names = HashSet::new();
    for value_selector in value_selectors {
        let name = value_selector.name();
        if names.contains(name) {
            return Err(InvalidLanes::NameCollision(name.to_string()));
        } else {
            names.insert(name);
        }
    }
    for map_selector in map_selectors {
        let name = map_selector.name();
        if names.contains(name) {
            return Err(InvalidLanes::NameCollision(name.to_string()));
        } else {
            names.insert(name);
        }
    }
    Ok(())
}

// Uses the information about the lanes of the agent to convert messages into event handlers that update the lanes.
pub struct MessageSelector {
    key_deserializer: BoxMessageDeserializer,
    value_deserializer: BoxMessageDeserializer,
    lanes: Lanes,
    relays: Relays,
}

impl MessageSelector {
    pub fn new(
        key_deserializer: BoxMessageDeserializer,
        value_deserializer: BoxMessageDeserializer,
        lanes: Lanes,
        relays: Relays,
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
            let key = Deser::new(message.key, key_deserializer);
            let value = Deser::new(message.payload, value_deserializer);
            let args = hlist![topic, key, value];

            for value_lane in value_lanes {
                value_lane_handlers.push(value_lane.select_handler(&args)?);
            }
            for map_lane in map_lanes {
                map_lane_handlers.push(map_lane.select_handler(&args)?);
            }
            for relay in relays {
                relay_handlers.push(relay.select_handler(&args)?);
            }
        }

        let handler = Sequentially::new(value_lane_handlers)
            .followed_by(Sequentially::new(map_lane_handlers))
            .followed_by(Sequentially::new(relay_handlers));
        Ok(handler)
    }
}
