// Copyright 2015-2021 Swim Inc.
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
pub mod map;
pub mod value;

use bytes::BytesMut;

use crate::agent_model::WriteResult;

pub use self::{command::CommandLane, map::MapLane, value::ValueLane};

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

/// Common functionality shared by all lane types.
pub trait Lane {
    /// If the state of the lane has changed, write an event into the buffer.
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult;
}
