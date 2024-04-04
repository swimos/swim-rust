// Copyright 2015-2023 Swim Inc.
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

pub mod command;
pub mod demand;
pub mod demand_map;
pub mod http;
mod join;
pub mod map;
mod queues;

pub mod supply;
pub mod value;

pub use join::map as join_map;
pub use join::value as join_value;
pub use join::LinkClosedResponse;

use bytes::BytesMut;

use crate::{agent_model::WriteResult, item::AgentItem};

pub use self::{
    command::CommandLane,
    demand::DemandLane,
    demand_map::DemandMapLane,
    http::{HttpLane, SimpleHttpLane},
    join::JoinLaneKind,
    join_map::JoinMapLane,
    join_value::JoinValueLane,
    map::MapLane,
    supply::SupplyLane,
    value::ValueLane,
};

/// Wrapper to allow projection function pointers to be exposed as event handler transforms
/// for different types of lanes.
pub struct ProjTransform<C, L> {
    projection: fn(&C) -> &L,
}

impl<C, L> ProjTransform<C, L> {
    pub fn new(projection: fn(&C) -> &L) -> Self {
        ProjTransform { projection }
    }
}

/// Base trait for all agent items that model lanes.
pub trait LaneItem: AgentItem {
    /// If the state of the lane has changed, write an event into the buffer.
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult;
}
