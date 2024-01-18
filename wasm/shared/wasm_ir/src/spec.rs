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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PlaneSpec {
    pub name: String,
    pub agent_specs: HashMap<String, AgentSpec>,
}

impl PlaneSpec {
    pub fn new(name: String, agent_specs: HashMap<String, AgentSpec>) -> PlaneSpec {
        PlaneSpec { name, agent_specs }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct AgentSpec {
    pub name: String,
    // laneUri -> spec
    pub lane_specs: HashMap<String, LaneSpec>,
}

impl AgentSpec {
    pub fn new(name: String, lane_specs: HashMap<String, LaneSpec>) -> AgentSpec {
        AgentSpec { name, lane_specs }
    }

    pub fn add_lane(&mut self, uri: &str, spec: LaneSpec) {
        self.lane_specs.insert(uri.to_string(), spec);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct LaneSpec {
    pub is_transient: bool,
    pub lane_idx: u64,
    pub lane_kind_repr: LaneKindRepr,
}

impl LaneSpec {
    pub fn new(is_transient: bool, lane_idx: u64, lane_kind_repr: LaneKindRepr) -> LaneSpec {
        LaneSpec {
            is_transient,
            lane_idx,
            lane_kind_repr,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum LaneKindRepr {
    Command,
    Demand,
    DemandMap,
    Map,
    JoinMap,
    JoinValue,
    Supply,
    Spatial,
    Value,
}

impl LaneKindRepr {
    pub fn map_like(&self) -> bool {
        matches!(
            self,
            LaneKindRepr::Map
                | LaneKindRepr::DemandMap
                | LaneKindRepr::JoinMap
                | LaneKindRepr::JoinValue
        )
    }
}
