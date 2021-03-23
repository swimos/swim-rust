// Copyright 2015-2020 SWIM.AI inc.
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

use crate::stores::lane::map::{MapLaneStore, MapStore};
use crate::stores::lane::value::{ValueLaneStore, ValueStore};
use crate::StoreError;

pub mod map;
pub mod value;

pub trait LaneStore<'a> {
    type MapLaneStore: MapStore<'a>;
    type ValueLaneStore: ValueStore<'a>;

    fn map_lane_store<I>(&mut self, lane: I) -> Result<Self::MapLaneStore, StoreError>
    where
        I: ToString;

    fn value_lane_store<I>(&mut self, lane: I) -> Result<Self::ValueLaneStore, StoreError>
    where
        I: ToString;
}

pub enum LaneKey {
    Map { lane_uri: String, key: String },
    Value { lane_uri: String },
}

pub struct SwimLaneStore<D> {
    delegate: D,
}

impl<'a, D> LaneStore<'a> for SwimLaneStore<D> {
    type MapLaneStore = MapLaneStore<Self>;
    type ValueLaneStore = ValueLaneStore<Self>;

    fn map_lane_store<I>(&mut self, _lane: I) -> Result<Self::MapLaneStore, StoreError>
    where
        I: ToString,
    {
        unimplemented!()
    }

    fn value_lane_store<I>(&mut self, _lane: I) -> Result<Self::ValueLaneStore, StoreError>
    where
        I: ToString,
    {
        unimplemented!()
    }
}
