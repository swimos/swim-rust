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

use crate::agent::UplinkKind;

use super::WarpLaneKind;

// spatial lanes are omitted as they are unimplemented
const LANE_KINDS: [WarpLaneKind; 8] = [
    WarpLaneKind::Command,
    WarpLaneKind::Demand,
    WarpLaneKind::DemandMap,
    WarpLaneKind::Map,
    WarpLaneKind::JoinMap,
    WarpLaneKind::JoinValue,
    WarpLaneKind::Supply,
    WarpLaneKind::Value,
];

#[test]
fn uplink_kinds() {
    for kind in LANE_KINDS {
        let uplink_kind = kind.uplink_kind();
        if kind.map_like() {
            assert_eq!(uplink_kind, UplinkKind::Map);
        } else if matches!(kind, WarpLaneKind::Supply) {
            assert_eq!(uplink_kind, UplinkKind::Supply);
        } else {
            assert_eq!(uplink_kind, UplinkKind::Value)
        }
    }
}

// this is here for when spatial lanes are implemented as the test will no longer panic
#[test]
#[should_panic]
fn spatial_uplink_kind() {
    WarpLaneKind::Spatial.uplink_kind();
}
