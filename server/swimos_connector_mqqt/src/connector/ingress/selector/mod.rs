use frunk::{hlist, Coprod};
use swimos_agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
use swimos_connector::{
    config::{IngressMapLaneSpec, IngressValueLaneSpec},
    deser::{BoxMessageDeserializer, Deferred},
    selector::{
        BadSelector, InvalidLaneSpec, InvalidLanes, KeySelector, MapLaneSelector, PayloadSelector,
        PubSubMapLaneSelector, PubSubSelector, PubSubValueLaneSelector, Relays, SelectHandler,
        SelectorError, TopicSelector, ValueLaneSelector,
    },
    ConnectorAgent,
};
use swimos_model::Value;
use tracing::trace;

use crate::facade::MqttMessage;

pub type MqttSelector = Coprod!(TopicSelector, PayloadSelector);

// Information about the lanes of the connector. These are computed from the configuration in the `on_start` handler
// and stored in the lifecycle to be used to start the consumer stream.
#[derive(Debug, Default, Clone)]
pub struct Lanes {
    value_lanes: Vec<ValueLaneSelector<MqttSelector>>,
    map_lanes: Vec<MapLaneSelector<MqttSelector, MqttSelector>>,
}

impl Lanes {
    pub fn value_lanes(&self) -> &[ValueLaneSelector<MqttSelector>] {
        &self.value_lanes
    }

    pub fn map_lanes(&self) -> &[MapLaneSelector<MqttSelector, MqttSelector>] {
        &self.map_lanes
    }

    pub fn try_from_lane_specs(
        value_lanes: &[IngressValueLaneSpec],
        map_lanes: &[IngressMapLaneSpec],
    ) -> Result<Lanes, InvalidLanes> {
        let value_selectors = value_lanes
            .iter()
            .map(try_from_value_selector)
            .collect::<Result<Vec<_>, _>>()?;
        let map_selectors = map_lanes
            .iter()
            .map(try_from_map_selector)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Lanes {
            value_lanes: value_selectors,
            map_lanes: map_selectors,
        })
    }
}

fn check_selector(selector: PubSubSelector) -> Result<MqttSelector, InvalidLaneSpec> {
    let split: Result<KeySelector, MqttSelector> = selector.uninject();
    match split {
        Ok(_) => Err(InvalidLaneSpec::Selector(BadSelector::UnsupportedRoot)),
        Err(sel) => Ok(sel),
    }
}

fn try_from_value_selector(
    selector: &IngressValueLaneSpec,
) -> Result<ValueLaneSelector<MqttSelector>, InvalidLanes> {
    let selector = ValueLaneSelector::try_from(selector)?;
    Ok(selector.try_map_selector(check_selector)?)
}

fn try_from_map_selector(
    selector: &IngressMapLaneSpec,
) -> Result<MapLaneSelector<MqttSelector, MqttSelector>, InvalidLanes> {
    let selector = MapLaneSelector::try_from(selector)?;
    Ok(selector.try_map_selectors(check_selector, check_selector)?)
}
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

    pub fn handle_message<'a, M>(
        &self,
        message: &'a M,
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
        //let mut relay_handlers = Vec::with_capacity(relays.len());

        {
            let topic = Value::text(message.topic());
            let payload = Deferred::new(message.payload(), payload_deserializer);
            let args = hlist![topic, payload];

            for value_lane in value_lanes {
                value_lane_handlers.push(value_lane.select_handler(&args)?);
            }
            for map_lane in map_lanes {
                map_lane_handlers.push(map_lane.select_handler(&args)?);
            }
            //for relay in relays {
            //    relay_handlers.push(relay.select_handler(&args)?);
            //}
        }

        let handler = Sequentially::new(value_lane_handlers)
            .followed_by(Sequentially::new(map_lane_handlers));
        //.followed_by(Sequentially::new(relay_handlers));
        Ok(handler)
    }
}
