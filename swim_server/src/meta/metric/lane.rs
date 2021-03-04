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

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;

use crate::meta::metric::aggregator::{AddressedMetric, Metric};
use crate::meta::metric::uplink::WarpUplinkPulse;
use crate::meta::metric::{MetricKind, WarpUplinkProfile};

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct LaneProfile {
    // todo: WarpDownlinkProfile aggregation
    // todo: LaneAddress
    pub uplink_event_delta: i32,
    pub uplink_event_rate: u64,
    pub uplink_event_count: u64,
    pub uplink_command_delta: i32,
    pub uplink_command_rate: u64,
    pub uplink_command_count: u64,
    pub open_delta: i32,
    pub open_count: u64,
    pub close_delta: i32,
    pub close_count: u64,
}

impl LaneProfile {
    pub fn accumulate(&mut self, _profile: &WarpUplinkProfile) {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct TaggedLaneProfile {
    pub path: RelativePath,
    pub profile: LaneProfile,
}

impl AddressedMetric for TaggedLaneProfile {
    type Metric = LaneProfile;

    fn unpack(self) -> (RelativePath, Self::Metric) {
        let TaggedLaneProfile { path, profile } = self;
        (path, profile)
    }

    fn path(&self) -> RelativePath {
        self.path.clone()
    }

    fn pack(payload: Self::Metric, path: RelativePath) -> Self {
        TaggedLaneProfile {
            path,
            profile: payload,
        }
    }
}

impl Metric<WarpUplinkProfile> for TaggedLaneProfile {
    const METRIC_KIND: MetricKind = MetricKind::Lane;
    type Pulse = LanePulse;

    fn accumulate(&mut self, _new: WarpUplinkProfile) {
        // let TaggedLaneProfile {
        //     path,
        //     profile:
        //         LaneProfile {
        //             uplink_event_delta,
        //             uplink_event_rate,
        //             uplink_event_count,
        //             uplink_command_delta,
        //             uplink_command_rate,
        //             uplink_command_count,
        //             open_delta,
        //             open_count,
        //             close_delta,
        //             close_count,
        //         },
        // } = old;
        //
        // WarpUplinkProfile {
        //     event_delta,
        //     event_rate,
        //     event_count,
        //     command_delta,
        //     command_rate,
        //     command_count,
        //     open_delta,
        //     open_count,
        //     close_delta,
        //     close_count,
        // } = new.profile;

        unimplemented!()
        // let WarpUplinkProfile {
        //     event_delta: new_event_delta,
        //     event_rate: new_event_rate,
        //     event_count: new_event_count,
        //     command_delta: new_command_delta,
        //     command_rate: new_command_rate,
        //     command_count: new_command_count,
        //     open_delta: new_open_delta,
        //     open_count: new_open_count,
        //     close_delta: new_close_delta,
        //     close_count: new_close_count,
        // } = new;
        //
        // let LaneProfile {
        //     uplink_event_delta,
        //     uplink_event_rate,
        //     uplink_event_count,
        //     uplink_command_delta,
        //     uplink_command_rate,
        //     uplink_command_count,
        //     open_delta,
        //     open_count,
        //     close_delta,
        //     close_count,
        // } = old;
        //
        // LaneProfile {
        //     uplink_event_delta: uplink_event_delta + new_event_delta,
        //     uplink_event_rate: uplink_event_rate + new_event_rate,
        //     uplink_event_count: uplink_event_count + new_event_count,
        //     uplink_command_delta: uplink_command_delta + new_command_delta,
        //     uplink_command_rate: uplink_command_rate + new_command_rate,
        //     uplink_command_count: uplink_command_count + new_command_count,
        //     open_delta: open_delta + new_open_delta,
        //     open_count: open_count + new_open_count,
        //     close_delta: close_delta + new_close_delta,
        //     close_count: close_count + new_close_count,
        // }
    }

    fn reset(&mut self) {
        let LaneProfile {
            uplink_event_delta,
            uplink_event_rate,
            uplink_command_delta,
            uplink_command_rate,
            open_delta,
            close_delta,
            ..
        } = &mut self.profile;

        *uplink_event_delta = 0;
        *uplink_event_rate = 0;
        *uplink_command_delta = 0;
        *uplink_command_rate = 0;
        *open_delta = 0;
        *close_delta = 0;
    }

    fn as_pulse(&self) -> Self::Pulse {
        let LaneProfile {
            uplink_event_delta,
            uplink_event_rate,
            uplink_event_count,
            uplink_command_delta,
            uplink_command_rate,
            uplink_command_count,
            ..
        } = self.profile;

        let uplink_pulse = WarpUplinkPulse {
            event_delta: uplink_event_delta,
            event_rate: uplink_event_rate,
            event_count: uplink_event_count,
            command_delta: uplink_command_delta,
            command_rate: uplink_command_rate,
            command_count: uplink_command_count,
        };

        LanePulse { uplink_pulse }
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct LanePulse {
    #[form(name = "uplinkPulse")]
    uplink_pulse: WarpUplinkPulse,
}

#[derive(Clone, PartialEq, Debug)]
pub struct TaggedLanePulse {
    path: RelativePath,
    pulse: LanePulse,
}
