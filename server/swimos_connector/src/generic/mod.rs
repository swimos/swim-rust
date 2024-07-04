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

use std::{
    cell::{Ref, RefCell},
    collections::HashMap,
};

use bytes::BytesMut;
use frunk::Coprod;
use swimos_agent::{
    agent_model::{AgentSpec, ItemSpec, MapLikeInitializer, ValueLikeInitializer, WriteResult},
    event_handler::UnitHandler,
    lanes::{
        map::{decode_and_select_apply, DecodeAndSelectApply, MapLaneSelectSync},
        value::{decode_and_select_set, DecodeAndSelectSet, ValueLaneSelectSync},
        LaneItem, MapLane, Selector, SelectorFn, ValueLane,
    },
};
use swimos_agent_protocol::MapMessage;
use swimos_api::agent::HttpLaneRequest;
use swimos_model::Value;

type GenericValueLane = ValueLane<Value>;
type GenericMapLane = MapLane<Value, Value>;

#[derive(Default, Debug)]
pub struct GenericConnectorAgent {
    value_lanes: RefCell<HashMap<String, GenericValueLane>>,
    map_lanes: RefCell<HashMap<String, GenericMapLane>>,
}

type ValueHandler = DecodeAndSelectSet<GenericConnectorAgent, Value, ValueLaneSelectorFn>;
type MapHandler = DecodeAndSelectApply<GenericConnectorAgent, Value, Value, MapLaneSelectorFn>;
type ValueSync = ValueLaneSelectSync<GenericConnectorAgent, Value, ValueLaneSelectorFn>;
type MapSync = MapLaneSelectSync<GenericConnectorAgent, Value, Value, MapLaneSelectorFn>;

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

pub struct ValueLaneSelectorFn {
    name: String,
}

impl ValueLaneSelectorFn {
    fn new(name: String) -> Self {
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

pub struct MapLaneSelectorFn {
    name: String,
}

impl MapLaneSelectorFn {
    fn new(name: String) -> Self {
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
