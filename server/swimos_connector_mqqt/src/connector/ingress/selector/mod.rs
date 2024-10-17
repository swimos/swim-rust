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

use std::collections::HashSet;

use frunk::{hlist, Coprod};
use swimos_agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
use swimos_connector::{
    config::{IngressMapLaneSpec, IngressValueLaneSpec},
    deser::{BoxMessageDeserializer, Deferred},
    selector::{
        InvalidLaneSpec, InvalidLanes, KeySelector, MapLaneSelector, PayloadSelector,
        PubSubSelector, SelectHandler, SelectorError, TopicSelector, ValueLaneSelector,
    },
    BadSelector, ConnectorAgent,
};
use swimos_model::Value;
use tracing::trace;

use crate::facade::MqttMessage;

#[cfg(test)]
mod tests;

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
        check_selectors(&value_selectors, &map_selectors)?;
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

fn check_selectors(
    value_selectors: &[ValueLaneSelector<MqttSelector>],
    map_selectors: &[MapLaneSelector<MqttSelector, MqttSelector>],
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

pub struct MqttMessageSelector {
    payload_deserializer: BoxMessageDeserializer,
    lanes: Lanes,
    //relays: Relays,
}

impl MqttMessageSelector {
    pub fn new(
        payload_deserializer: BoxMessageDeserializer,
        lanes: Lanes,
        //relays: Relays
    ) -> Self {
        MqttMessageSelector {
            payload_deserializer,
            lanes,
            //relays,
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
            //relays,
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
