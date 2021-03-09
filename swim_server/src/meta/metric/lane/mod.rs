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

#[cfg(test)]
mod tests;

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;

use crate::meta::metric::aggregator::{AddressedMetric, Metric};
use crate::meta::metric::uplink::WarpUplinkPulse;
use crate::meta::metric::{MetricKind, WarpUplinkProfile};

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct WarpLaneProfile {
    // todo: WarpDownlinkProfile aggregation
    // todo: LaneAddress
    pub uplink_event_delta: u32,
    pub uplink_event_rate: u64,
    pub uplink_event_count: u64,
    pub uplink_command_delta: u32,
    pub uplink_command_rate: u64,
    pub uplink_command_count: u64,
    pub uplink_open_delta: u32,
    pub uplink_open_count: u32,
    pub uplink_close_delta: u32,
    pub uplink_close_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedLaneProfile {
    pub path: RelativePath,
    pub profile: WarpLaneProfile,
}

impl AddressedMetric for TaggedLaneProfile {
    type Metric = WarpLaneProfile;

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

    fn accumulate(&mut self, new: WarpUplinkProfile) {
        let WarpLaneProfile {
            uplink_event_delta,
            uplink_event_rate,
            uplink_command_delta,
            uplink_command_rate,
            uplink_open_delta,
            uplink_open_count,
            uplink_close_delta,
            uplink_close_count,
            ..
        } = &mut self.profile;

        let WarpUplinkProfile {
            event_delta: new_event_delta,
            event_rate: new_event_rate,
            command_delta: new_command_delta,
            command_rate: new_command_rate,
            open_delta: new_open_delta,
            open_count: new_open_count,
            close_delta: new_close_delta,
            close_count: new_close_count,
            ..
        } = new;

        *uplink_event_delta += new_event_delta;
        *uplink_event_rate += new_event_rate;

        *uplink_command_delta += new_command_delta;
        *uplink_command_rate += new_command_rate;

        *uplink_open_delta += new_open_delta;
        *uplink_open_count += new_open_count;

        *uplink_close_delta += new_close_delta;
        *uplink_close_count += new_close_count;
    }

    fn collect(&mut self) -> TaggedLaneProfile {
        let WarpLaneProfile {
            uplink_event_delta,
            uplink_event_rate,
            uplink_event_count,
            uplink_command_delta,
            uplink_command_rate,
            uplink_command_count,
            uplink_open_delta,
            uplink_open_count,
            uplink_close_delta,
            uplink_close_count,
        } = &mut self.profile;

        let new_profile = WarpLaneProfile {
            uplink_event_delta: *uplink_event_delta,
            uplink_event_rate: *uplink_event_rate,
            uplink_event_count: *uplink_event_count,
            uplink_command_delta: *uplink_command_delta,
            uplink_command_rate: *uplink_command_rate,
            uplink_command_count: *uplink_command_count,
            uplink_open_delta: *uplink_open_delta,
            uplink_open_count: *uplink_open_count,
            uplink_close_delta: *uplink_close_delta,
            uplink_close_count: *uplink_close_count,
        };

        *uplink_event_delta = 0;
        *uplink_event_rate = 0;
        *uplink_command_delta = 0;
        *uplink_command_rate = 0;
        *uplink_open_delta = 0;
        *uplink_close_delta = 0;

        TaggedLaneProfile {
            path: self.path.clone(),
            profile: new_profile,
        }
    }

    fn as_pulse(&self) -> Self::Pulse {
        let WarpLaneProfile {
            uplink_event_delta,
            uplink_event_rate,
            uplink_event_count,
            uplink_command_delta,
            uplink_command_rate,
            uplink_command_count,
            uplink_open_count,
            uplink_close_count,
            ..
        } = self.profile;

        let link_count = uplink_open_count.saturating_sub(uplink_close_count);
        let event_count = uplink_event_count.wrapping_add(uplink_event_delta as u64);
        let command_count = uplink_command_count.wrapping_add(uplink_command_delta as u64);

        let uplink_pulse = WarpUplinkPulse {
            link_count,
            event_rate: uplink_event_rate,
            event_count,
            command_rate: uplink_command_rate,
            command_count,
        };

        LanePulse { uplink_pulse }
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct LanePulse {
    #[form(name = "uplinkPulse")]
    pub uplink_pulse: WarpUplinkPulse,
}

#[derive(Clone, PartialEq, Debug)]
pub struct TaggedLanePulse {
    path: RelativePath,
    pulse: LanePulse,
}
