use crate::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use crate::deser::{BoxMessageDeserializer, MessagePart, MessageView};
use crate::relay::Relays;
use crate::selector::{Computed, MapLaneSelector, ValueLaneSelector};
use crate::{ConnectorAgent, InvalidLanes, SelectorError};
use std::collections::HashSet;
use swimos_agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
use swimos_model::Value;
use tracing::trace;

// Information about the lanes of the connector. These are computed from the configuration in the `on_start` handler
// and stored in the lifecycle to be used to start the consumer stream.
#[derive(Debug, Default, Clone)]
pub struct Lanes {
    value_lanes: Vec<ValueLaneSelector>,
    map_lanes: Vec<MapLaneSelector>,
}

impl Lanes {
    pub fn value_lanes(&self) -> &[ValueLaneSelector] {
        &self.value_lanes
    }

    pub fn map_lanes(&self) -> &[MapLaneSelector] {
        &self.map_lanes
    }

    pub fn try_from_lane_specs(
        value_lanes: &[IngressValueLaneSpec],
        map_lanes: &[IngressMapLaneSpec],
    ) -> Result<Lanes, InvalidLanes> {
        let value_selectors = value_lanes
            .iter()
            .map(ValueLaneSelector::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let map_selectors = map_lanes
            .iter()
            .map(MapLaneSelector::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        check_selectors(&value_selectors, &map_selectors)?;
        Ok(Lanes {
            value_lanes: value_selectors,
            map_lanes: map_selectors,
        })
    }
}

fn check_selectors(
    value_selectors: &[ValueLaneSelector],
    map_selectors: &[MapLaneSelector],
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
        let Lanes {
            value_lanes,
            map_lanes,
            ..
        } = lanes;
        trace!(topic = { message.topic() }, "Handling a message.");

        let mut value_lane_handlers = Vec::with_capacity(value_lanes.len());
        let mut map_lane_handlers = Vec::with_capacity(map_lanes.len());
        let mut relay_handlers = Vec::with_capacity(relays.len());

        {
            let topic = Value::text(message.topic());
            let mut key = Computed::new(|| key_deserializer.deserialize(message, MessagePart::Key));
            let mut value =
                Computed::new(|| value_deserializer.deserialize(message, MessagePart::Payload));

            for value_lane in value_lanes {
                value_lane_handlers.push(value_lane.select_handler(&topic, &mut key, &mut value)?);
            }
            for map_lane in map_lanes {
                map_lane_handlers.push(map_lane.select_handler(&topic, &mut key, &mut value)?);
            }
            for relay in relays {
                relay_handlers.push(relay.select_handler(&topic, &mut key, &mut value)?);
            }
        }

        let handler = Sequentially::new(value_lane_handlers)
            .followed_by(Sequentially::new(map_lane_handlers))
            .followed_by(Sequentially::new(relay_handlers));
        Ok(handler)
    }
}
