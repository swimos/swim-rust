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
use crate::selector::{InvalidLaneSpec, InvalidLanes, MapLaneSelector, ValueLaneSelector};
use std::collections::HashSet;

// Information about the lanes of the connector. These are computed from the configuration in the `on_start` handler
// and stored in the lifecycle to be used to start the consumer stream.
#[derive(Debug, Clone)]
pub struct Lanes<S> {
    value_lanes: Vec<ValueLaneSelector<S>>,
    map_lanes: Vec<MapLaneSelector<S, S>>,
}

impl<S> Default for Lanes<S> {
    fn default() -> Self {
        Lanes {
            value_lanes: vec![],
            map_lanes: vec![],
        }
    }
}

impl<S> Lanes<S> {
    pub fn value_lanes(&self) -> &[ValueLaneSelector<S>] {
        &self.value_lanes
    }

    pub fn map_lanes(&self) -> &[MapLaneSelector<S, S>] {
        &self.map_lanes
    }

    pub fn try_from_lane_specs<'a>(
        value_lanes: &'a [IngressValueLaneSpec],
        map_lanes: &'a [IngressMapLaneSpec],
    ) -> Result<Lanes<S>, InvalidLanes>
    where
        ValueLaneSelector<S>: TryFrom<&'a IngressValueLaneSpec, Error = InvalidLaneSpec>,
        MapLaneSelector<S, S>: TryFrom<&'a IngressMapLaneSpec, Error = InvalidLaneSpec>,
    {
        let value_selectors = value_lanes
            .iter()
            .map(ValueLaneSelector::<S>::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let map_selectors = map_lanes
            .iter()
            .map(MapLaneSelector::<S, S>::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        check_selectors(&value_selectors, &map_selectors)?;
        Ok(Lanes {
            value_lanes: value_selectors,
            map_lanes: map_selectors,
        })
    }
}

fn check_selectors<S>(
    value_selectors: &[ValueLaneSelector<S>],
    map_selectors: &[MapLaneSelector<S, S>],
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
