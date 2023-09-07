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

use std::fmt::{Display, Formatter};

use swim_form::structural::Tag;

use crate::{agent::UplinkKind, meta::lane::LaneKind};

/// An enumeration representing the kinds of Warp lanes.
#[derive(Tag, Debug, PartialEq, Eq, Clone, Copy, Hash)]
#[form_root(::swim_form)]
pub enum WarpLaneKind {
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

impl From<WarpLaneKind> for LaneKind {
    fn from(value: WarpLaneKind) -> Self {
        match value {
            WarpLaneKind::Command => LaneKind::Command,
            WarpLaneKind::Demand => LaneKind::Demand,
            WarpLaneKind::DemandMap => LaneKind::DemandMap,
            WarpLaneKind::Map => LaneKind::Map,
            WarpLaneKind::JoinMap => LaneKind::JoinMap,
            WarpLaneKind::JoinValue => LaneKind::JoinValue,
            WarpLaneKind::Supply => LaneKind::Supply,
            WarpLaneKind::Spatial => LaneKind::Spatial,
            WarpLaneKind::Value => LaneKind::Value,
        }
    }
}

impl WarpLaneKind {
    pub fn map_like(&self) -> bool {
        matches!(
            self,
            WarpLaneKind::Map
                | WarpLaneKind::DemandMap
                | WarpLaneKind::JoinMap
                | WarpLaneKind::JoinValue
        )
    }

    pub fn uplink_kind(&self) -> UplinkKind {
        match self {
            WarpLaneKind::Map | WarpLaneKind::DemandMap | WarpLaneKind::JoinMap => UplinkKind::Map,
            WarpLaneKind::Supply => UplinkKind::Supply,
            WarpLaneKind::Spatial => todo!("Spatial uplinks not supported."),
            _ => UplinkKind::Value,
        }
    }
}

impl Display for WarpLaneKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let as_str: &str = self.as_ref();
        write!(f, "{}", as_str)
    }
}
