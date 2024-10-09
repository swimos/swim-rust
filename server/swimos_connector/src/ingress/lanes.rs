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

use crate::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use crate::ingress::check_selectors;
use crate::selector::{
    MapLaneSelector, PubSubMapLaneSelector, PubSubValueLaneSelector, ValueLaneSelector,
};
use crate::InvalidLanes;

// Information about the lanes of the connector. These are computed from the configuration in the `on_start` handler
// and stored in the lifecycle to be used to start the consumer stream.
#[derive(Debug, Default, Clone)]
pub struct Lanes {
    value_lanes: Vec<PubSubValueLaneSelector>,
    map_lanes: Vec<PubSubMapLaneSelector>,
}

impl Lanes {
    pub fn value_lanes(&self) -> &[PubSubValueLaneSelector] {
        &self.value_lanes
    }

    pub fn map_lanes(&self) -> &[PubSubMapLaneSelector] {
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
