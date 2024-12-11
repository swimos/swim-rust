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

use std::{
    borrow::Cow,
    cell::{Cell, Ref, RefCell},
    collections::{HashMap, HashSet},
    ops::Deref,
};

use bytes::BytesMut;
use frunk::Coprod;
use swimos_agent::{
    agent_lifecycle::item_event::{BorrowItem, DynamicAgent, DynamicItem},
    agent_model::{
        AgentDescription, AgentSpec, ItemDescriptor, ItemSpec, MapLikeInitializer,
        ValueLikeInitializer, WriteResult,
    },
    event_handler::{ActionContext, HandlerAction, StepResult, UnitHandler},
    lanes::{
        map::{decode_and_select_apply, DecodeAndSelectApply, MapLaneSelectSync},
        value::{decode_and_select_set, DecodeAndSelectSet, ValueLaneSelectSync},
        LaneItem, MapLane, Selector, SelectorFn, ValueLane,
    },
    AgentItem, AgentMetadata,
};
use swimos_agent_protocol::MapMessage;
use swimos_api::{
    agent::{HttpLaneRequest, WarpLaneKind},
    error::DynamicRegistrationError,
};
use swimos_model::Value;
use tracing::{error, info};

type GenericValueLane = ValueLane<Value>;
type GenericMapLane = MapLane<Value, Value>;

/// A generic agent type to be used by implementations of [`crate::IngressConnector`] and [`crate::EgressConnector`].
/// By default, a [`ConnectorAgent`] does not define any lanes or stores and these most be opened dynamically by the agent
/// lifecycle (which is determined by the [`crate::IngressConnector`] or [`crate::EgressConnector`] implementation). Only
/// value and map lanes are supported and an attempt to open any other kind of lane or store will result in the agent
/// terminating with an error.
#[derive(Default, Debug)]
pub struct ConnectorAgent {
    id_counter: Cell<u64>,
    value_lanes: RefCell<HashMap<String, GenericValueLane>>,
    map_lanes: RefCell<HashMap<String, GenericMapLane>>,
    flags: Cell<u64>,
}

type ValueHandler = DecodeAndSelectSet<ConnectorAgent, Value, ValueLaneSelectorFn>;
type MapHandler = DecodeAndSelectApply<ConnectorAgent, Value, Value, MapLaneSelectorFn>;
type ValueSync = ValueLaneSelectSync<ConnectorAgent, Value, ValueLaneSelectorFn>;
type MapSync = MapLaneSelectSync<ConnectorAgent, Value, Value, MapLaneSelectorFn>;

impl ConnectorAgent {
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

    /// Read the flags associated with the agent instance (the meaning of the flags is determined by the
    /// agent lifecycle).
    pub fn read_flags(&self) -> u64 {
        self.flags.get()
    }

    /// Set the flags associated with the agent instance (the meaning of the flags is determined by the
    /// agent lifecycle). This returns an [event handler](`swimos_agent::event_handler::EventHandler`)
    /// that must be executed to set the flags.
    ///
    /// # Arguments
    /// * `flags` - The new value for the flags.
    pub fn set_flags(flags: u64) -> SetFlags {
        SetFlags(Some(flags))
    }
}

struct LaneRef<'a, L> {
    map: Ref<'a, HashMap<String, L>>,
    key: &'a str,
}

impl<L> Deref for LaneRef<'_, L> {
    type Target = L;

    fn deref(&self) -> &Self::Target {
        &self.map[self.key]
    }
}

impl AgentDescription for ConnectorAgent {
    fn item_name(&self, id: u64) -> Option<Cow<'_, str>> {
        let ConnectorAgent {
            value_lanes,
            map_lanes,
            ..
        } = self;
        let value_guard = value_lanes.borrow();
        value_guard
            .iter()
            .find_map(|(name, l)| {
                if l.id() == id {
                    Some(Cow::Owned(name.clone()))
                } else {
                    None
                }
            })
            .or_else(|| {
                let map_guard = map_lanes.borrow();
                map_guard.iter().find_map(|(name, l)| {
                    if l.id() == id {
                        Some(Cow::Owned(name.clone()))
                    } else {
                        None
                    }
                })
            })
    }
}

impl AgentSpec for ConnectorAgent {
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
    name: &'a str,
}

impl<'a, L> LaneSelector<'a, L> {
    fn new(map: Ref<'a, HashMap<String, L>>, name: &'a str) -> Self {
        LaneSelector { map, name }
    }
}

impl<L> Selector for LaneSelector<'_, L> {
    type Target = L;

    fn select(&self) -> Option<&Self::Target> {
        let LaneSelector { map, name } = self;
        map.get(*name)
    }

    fn name(&self) -> &str {
        self.name
    }
}

/// An implementation of [`SelectorFn`] that will attempt to select a [`ValueLane`] from a [`ConnectorAgent`].
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

impl SelectorFn<ConnectorAgent> for ValueLaneSelectorFn {
    type Target = GenericValueLane;

    fn name(&self) -> &str {
        &self.name
    }

    fn selector<'a>(
        &'a self,
        context: &'a ConnectorAgent,
    ) -> impl Selector<Target = Self::Target> + 'a {
        let ConnectorAgent { value_lanes, .. } = context;
        let map = value_lanes.borrow();
        LaneSelector::new(map, &self.name)
    }
}

/// An implementation of [`SelectorFn`] that will attempt to select a [`MapLane`] from a [`ConnectorAgent`].
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

impl SelectorFn<ConnectorAgent> for MapLaneSelectorFn {
    type Target = GenericMapLane;

    fn selector<'a>(
        &'a self,
        context: &'a ConnectorAgent,
    ) -> impl Selector<Target = Self::Target> + 'a {
        let ConnectorAgent { map_lanes, .. } = context;
        let map = map_lanes.borrow();
        LaneSelector::new(map, &self.name)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// An [event handler](`swimos_agent::event_handler::EventHandler`) that will set the flags associated with a
/// [`ConnectorAgent`].
#[derive(Default, Debug)]
pub struct SetFlags(Option<u64>);

impl HandlerAction<ConnectorAgent> for SetFlags {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<ConnectorAgent>,
        _meta: AgentMetadata,
        context: &ConnectorAgent,
    ) -> StepResult<Self::Completion> {
        let SetFlags(flags) = self;
        if let Some(flags) = flags.take() {
            context.flags.set(flags);
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

enum BorrowedLaneInner<'a> {
    Value(Ref<'a, HashMap<String, GenericValueLane>>),
    Map(Ref<'a, HashMap<String, GenericMapLane>>),
}

pub struct BorrowedLane<'a> {
    name: &'a str,
    inner: BorrowedLaneInner<'a>,
}

impl BorrowItem for BorrowedLane<'_> {
    fn borrow_item(&self) -> DynamicItem<'_> {
        let BorrowedLane { name, inner } = self;
        match inner {
            BorrowedLaneInner::Value(lanes) => DynamicItem::ValueLane(&lanes[*name]),
            BorrowedLaneInner::Map(lanes) => DynamicItem::MapLane(&lanes[*name]),
        }
    }
}

impl DynamicAgent for ConnectorAgent {
    type Borrowed<'a> = BorrowedLane<'a>
        where
            Self: 'a;

    fn item<'a>(&'a self, name: &'a str) -> Option<Self::Borrowed<'a>> {
        let ConnectorAgent {
            value_lanes,
            map_lanes,
            ..
        } = self;
        let values = value_lanes.borrow();
        if values.contains_key(name) {
            Some(BorrowedLane {
                name,
                inner: BorrowedLaneInner::Value(values),
            })
        } else {
            let maps = map_lanes.borrow();
            if maps.contains_key(name) {
                Some(BorrowedLane {
                    name,
                    inner: BorrowedLaneInner::Map(maps),
                })
            } else {
                None
            }
        }
    }
}
