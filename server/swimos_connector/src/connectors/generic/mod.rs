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

#[cfg(test)]
mod tests;

mod config;
mod error;
pub mod selector;

pub use config::*;
pub use error::*;

use crate::connectors::generic::selector::{MapLaneSelector, ValueLaneSelector};
use crate::deserialization::{BoxMessageDeserializer, Computed, MessagePart, MessageView};
use bytes::BytesMut;
use frunk::Coprod;
use std::sync::Arc;
use std::{
    cell::{Cell, Ref, RefCell},
    collections::{HashMap, HashSet},
    ops::Deref,
};
use swimos_agent::agent_lifecycle::HandlerContext;
use swimos_agent::event_handler::{HandlerActionExt, TryHandlerActionExt};
use swimos_agent::{
    agent_model::{
        AgentSpec, ItemDescriptor, ItemSpec, MapLikeInitializer, ValueLikeInitializer, WriteResult,
    },
    event_handler::UnitHandler,
    event_handler::{EventHandler, Sequentially},
    lanes::{
        map::{decode_and_select_apply, DecodeAndSelectApply, MapLaneSelectSync},
        value::{decode_and_select_set, DecodeAndSelectSet, ValueLaneSelectSync},
        LaneItem, MapLane, Selector, SelectorFn, ValueLane,
    },
};
use swimos_agent_protocol::MapMessage;
use swimos_api::{
    agent::{HttpLaneRequest, WarpLaneKind},
    error::DynamicRegistrationError,
};
use swimos_model::Value;
use swimos_utilities::trigger;
use tokio::sync::Semaphore;
use tracing::{error, info, trace};

type ConnHandlerContext = HandlerContext<GenericConnectorAgent>;
type GenericValueLane = ValueLane<Value>;
type GenericMapLane = MapLane<Value, Value>;

/// A generic agent type to be used by implementations of [`crate::Connector`]. By default, a [`GenericConnectorAgent`] does
/// not define any lanes or stores and these most be opened dynamically by the agent lifecycle (which is determined by
/// the [`crate::Connector`] implementation). Only value and map lanes are supported and an attempt to open any other
/// kind of lane or store will result in the agent terminating with an error.
#[derive(Default, Debug)]
pub struct GenericConnectorAgent {
    id_counter: Cell<u64>,
    value_lanes: RefCell<HashMap<String, GenericValueLane>>,
    map_lanes: RefCell<HashMap<String, GenericMapLane>>,
}

type ValueHandler = DecodeAndSelectSet<GenericConnectorAgent, Value, ValueLaneSelectorFn>;
type MapHandler = DecodeAndSelectApply<GenericConnectorAgent, Value, Value, MapLaneSelectorFn>;
type ValueSync = ValueLaneSelectSync<GenericConnectorAgent, Value, ValueLaneSelectorFn>;
type MapSync = MapLaneSelectSync<GenericConnectorAgent, Value, Value, MapLaneSelectorFn>;

impl GenericConnectorAgent {
    /// Get a set of the names of the value lanes that have been registered.
    pub fn value_lanes(&self) -> HashSet<String> {
        self.value_lanes.borrow().keys().cloned().collect()
    }

    /// Get a set of the names of the map lanes that have been registered.
    pub fn map_lanes(&self) -> HashSet<String> {
        self.map_lanes.borrow().keys().cloned().collect()
    }

    /// Attempt to borrow a value lane from the agent.
    ///
    /// # Arguments
    /// * `name` - The name of the lane.
    pub fn value_lane<'a>(
        &'a mut self,
        name: &'a str,
    ) -> Option<impl Deref<Target = GenericValueLane> + 'a> {
        if self.value_lanes.borrow().contains_key(name) {
            Some(LaneRef {
                map: self.value_lanes.borrow(),
                key: name,
            })
        } else {
            None
        }
    }

    /// Attempt to borrow a map lane from the agent.
    ///
    /// # Arguments
    /// * `name` - The name of the lane.
    pub fn map_lane<'a>(
        &'a mut self,
        name: &'a str,
    ) -> Option<impl Deref<Target = GenericMapLane> + 'a> {
        if self.map_lanes.borrow().contains_key(name) {
            Some(LaneRef {
                map: self.map_lanes.borrow(),
                key: name,
            })
        } else {
            None
        }
    }
}

struct LaneRef<'a, L> {
    map: Ref<'a, HashMap<String, L>>,
    key: &'a str,
}

impl<'a, L> Deref for LaneRef<'a, L> {
    type Target = L;

    fn deref(&self) -> &Self::Target {
        &self.map[self.key]
    }
}

impl AgentSpec for GenericConnectorAgent {
    type ValCommandHandler = ValueHandler;

    type MapCommandHandler = MapHandler;

    type OnSyncHandler = Coprod!(ValueSync, MapSync);

    type HttpRequestHandler = UnitHandler;

    fn item_specs() -> HashMap<&'static str, ItemSpec> {
        HashMap::new()
    }

    fn on_value_command(&self, lane: &str, body: BytesMut) -> Option<Self::ValCommandHandler> {
        if self.value_lanes.borrow().contains_key(lane) {
            Some(decode_and_select_set(
                body,
                ValueLaneSelectorFn::new(lane.to_string()),
            ))
        } else {
            None
        }
    }

    fn init_value_like_item(&self, _item: &str) -> Option<ValueLikeInitializer<Self>>
    where
        Self: 'static,
    {
        None
    }

    fn init_map_like_item(&self, _item: &str) -> Option<MapLikeInitializer<Self>>
    where
        Self: 'static,
    {
        None
    }

    fn on_map_command(
        &self,
        lane: &str,
        body: MapMessage<BytesMut, BytesMut>,
    ) -> Option<Self::MapCommandHandler> {
        if self.map_lanes.borrow().contains_key(lane) {
            Some(decode_and_select_apply(
                body,
                MapLaneSelectorFn::new(lane.to_string()),
            ))
        } else {
            None
        }
    }

    fn on_sync(&self, lane: &str, id: uuid::Uuid) -> Option<Self::OnSyncHandler> {
        if self.value_lanes.borrow().contains_key(lane) {
            Some(Self::OnSyncHandler::inject(ValueLaneSelectSync::new(
                ValueLaneSelectorFn::new(lane.to_string()),
                id,
            )))
        } else if self.map_lanes.borrow().contains_key(lane) {
            Some(Self::OnSyncHandler::inject(MapLaneSelectSync::new(
                MapLaneSelectorFn::new(lane.to_string()),
                id,
            )))
        } else {
            None
        }
    }

    fn on_http_request(
        &self,
        _lane: &str,
        request: HttpLaneRequest,
    ) -> Result<Self::HttpRequestHandler, HttpLaneRequest> {
        Err(request)
    }

    fn write_event(&self, lane_name: &str, buffer: &mut BytesMut) -> Option<WriteResult> {
        self.value_lanes
            .borrow()
            .get(lane_name)
            .map(|lane| lane.write_to_buffer(buffer))
            .or_else(|| {
                self.map_lanes
                    .borrow()
                    .get(lane_name)
                    .map(|lane| lane.write_to_buffer(buffer))
            })
    }

    fn register_dynamic_item(
        &self,
        name: &str,
        descriptor: ItemDescriptor,
    ) -> Result<u64, DynamicRegistrationError> {
        match descriptor {
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Value,
                ..
            } => {
                let mut guard = self.value_lanes.borrow_mut();
                if guard.contains_key(name) || self.map_lanes.borrow().contains_key(name) {
                    error!(name, "Duplicate lane name.");
                    Err(DynamicRegistrationError::DuplicateName(name.to_string()))
                } else {
                    let id = self.id_counter.get();
                    info!(name, id, "Registering value lane.");
                    self.id_counter.set(id + 1);
                    let lane = GenericValueLane::new(id, Value::Extant);
                    guard.insert(name.to_string(), lane);
                    Ok(id)
                }
            }
            ItemDescriptor::WarpLane {
                kind: WarpLaneKind::Map,
                ..
            } => {
                let mut guard = self.map_lanes.borrow_mut();
                if guard.contains_key(name) || self.value_lanes.borrow().contains_key(name) {
                    error!(name, "Duplicate lane name.");
                    Err(DynamicRegistrationError::DuplicateName(name.to_string()))
                } else {
                    let id = self.id_counter.get();
                    info!(name, id, "Registering map lane.");
                    self.id_counter.set(id + 1);
                    let lane = GenericMapLane::new(id, Default::default());
                    guard.insert(name.to_string(), lane);
                    Ok(id)
                }
            }
            ItemDescriptor::WarpLane { kind, .. } => {
                error!(name, kind = %kind, "Dynamic registration for unsupported lane kind.");
                Err(DynamicRegistrationError::LaneKindUnsupported(kind))
            }
            ItemDescriptor::Store { kind, .. } => {
                error!(name, kind = %kind, "Dynamic registration for unsupported store kind.");
                Err(DynamicRegistrationError::StoreKindUnsupported(kind))
            }
            ItemDescriptor::Http => {
                error!(name, "Dynamic HTTP lanes are not supported.");
                Err(DynamicRegistrationError::HttpLanesUnsupported)
            }
        }
    }
}

struct LaneSelector<'a, L> {
    map: Ref<'a, HashMap<String, L>>,
    name: String,
}

impl<'a, L> LaneSelector<'a, L> {
    fn new(map: Ref<'a, HashMap<String, L>>, name: String) -> Self {
        LaneSelector { map, name }
    }
}

impl<'a, L> Selector for LaneSelector<'a, L> {
    type Target = L;

    fn select(&self) -> Option<&Self::Target> {
        let LaneSelector { map, name } = self;
        map.get(name.as_str())
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }
}

/// An implementation of [`SelectorFn`] that will attempt to select a [`ValueLane`] from a [`GenericConnectorAgent`].
#[derive(Debug)]
pub struct ValueLaneSelectorFn {
    name: String,
}

impl ValueLaneSelectorFn {
    /// # Arguments
    /// * `name` - The name of the lane to select.
    pub fn new(name: String) -> Self {
        ValueLaneSelectorFn { name }
    }
}

impl SelectorFn<GenericConnectorAgent> for ValueLaneSelectorFn {
    type Target = GenericValueLane;

    fn selector(
        self,
        context: &GenericConnectorAgent,
    ) -> impl Selector<Target = Self::Target> + '_ {
        let GenericConnectorAgent { value_lanes, .. } = context;
        let map = value_lanes.borrow();
        LaneSelector::new(map, self.name)
    }
}

/// An implementation of [`SelectorFn`] that will attempt to select a [`MapLane`] from a [`GenericConnectorAgent`].
#[derive(Debug)]
pub struct MapLaneSelectorFn {
    name: String,
}

impl MapLaneSelectorFn {
    /// # Arguments
    /// * `name` - The name of the lane to select.
    pub fn new(name: String) -> Self {
        MapLaneSelectorFn { name }
    }
}

impl SelectorFn<GenericConnectorAgent> for MapLaneSelectorFn {
    type Target = GenericMapLane;

    fn selector(
        self,
        context: &GenericConnectorAgent,
    ) -> impl Selector<Target = Self::Target> + '_ {
        let GenericConnectorAgent { map_lanes, .. } = context;
        let map = map_lanes.borrow();
        LaneSelector::new(map, self.name)
    }
}

// Information about the lanes of the connector. These are computed from the configuration in the `on_start` handler
// and stored in the lifecycle to be used to start the consumer stream.
#[derive(Debug, Default, Clone)]
pub struct Lanes {
    total_lanes: u32,
    value_lanes: Vec<ValueLaneSelector>,
    map_lanes: Vec<MapLaneSelector>,
}

impl Lanes {
    pub fn total(&self) -> u32 {
        self.total_lanes
    }

    pub fn value(&self) -> &[ValueLaneSelector] {
        &self.value_lanes
    }

    pub fn map(&self) -> &[MapLaneSelector] {
        &self.map_lanes
    }

    pub fn try_from_lane_specs(
        value_lanes: &[ValueLaneSpec],
        map_lanes: &[MapLaneSpec],
    ) -> Result<Self, InvalidLanes> {
        let value_selectors = value_lanes
            .iter()
            .map(ValueLaneSelector::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let map_selectors = map_lanes
            .iter()
            .map(MapLaneSelector::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let total = value_selectors.len() + map_selectors.len();
        let total_lanes = if let Ok(n) = u32::try_from(total) {
            n
        } else {
            return Err(InvalidLanes::TooManyLanes(total));
        };
        check_selectors(&value_selectors, &map_selectors)?;
        Ok(Lanes {
            value_lanes: value_selectors,
            map_lanes: map_selectors,
            total_lanes,
        })
    }

    // Opens the lanes that are defined in the configuration.
    pub fn open_lanes(
        &self,
        init_complete: trigger::Sender,
    ) -> impl EventHandler<GenericConnectorAgent> + 'static {
        let handler_context = ConnHandlerContext::default();
        let Lanes {
            value_lanes,
            map_lanes,
            total_lanes,
        } = self;

        let semaphore = Arc::new(Semaphore::new(0));

        let wait_handle = semaphore.clone();
        let total = *total_lanes;
        let await_done = async move {
            let result = wait_handle.acquire_many(total).await.map(|_| ());
            handler_context
                .value(result)
                .try_handler()
                .followed_by(handler_context.effect(|| {
                    let _ = init_complete.trigger();
                }))
        };

        let mut open_value_lanes = Vec::with_capacity(value_lanes.len());
        let mut open_map_lanes = Vec::with_capacity(map_lanes.len());

        for selector in value_lanes {
            let sem_cpy = semaphore.clone();
            open_value_lanes.push(handler_context.open_value_lane(selector.name(), move |_| {
                handler_context.effect(move || sem_cpy.add_permits(1))
            }));
        }

        for selector in map_lanes {
            let sem_cpy = semaphore.clone();
            open_map_lanes.push(handler_context.open_map_lane(selector.name(), move |_| {
                handler_context.effect(move || sem_cpy.add_permits(1))
            }));
        }

        handler_context
            .suspend(await_done)
            .followed_by(Sequentially::new(open_value_lanes))
            .followed_by(Sequentially::new(open_map_lanes))
            .discard()
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
}

impl MessageSelector {
    pub fn new(
        key_deserializer: BoxMessageDeserializer,
        value_deserializer: BoxMessageDeserializer,
        lanes: Lanes,
    ) -> Self {
        MessageSelector {
            key_deserializer,
            value_deserializer,
            lanes,
        }
    }

    pub fn handle_message<'a>(
        &self,
        message: &'a MessageView<'a>,
    ) -> Result<impl EventHandler<GenericConnectorAgent> + Send + 'static, LaneSelectorError> {
        let MessageSelector {
            key_deserializer,
            value_deserializer,
            lanes,
        } = self;
        let Lanes {
            value_lanes,
            map_lanes,
            ..
        } = lanes;
        trace!(topic = { message.topic() }, "Handling a message.");
        let mut value_lane_handlers = Vec::with_capacity(value_lanes.len());
        let mut map_lane_handlers = Vec::with_capacity(map_lanes.len());
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
        }

        Ok(
            Sequentially::new(value_lane_handlers)
                .followed_by(Sequentially::new(map_lane_handlers)),
        )
    }
}
