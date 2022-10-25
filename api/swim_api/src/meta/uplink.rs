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

use swim_form::Form;

/// A lane pulse detailing accumulated metrics for all uplinks on a lane.
#[derive(Default, Form, Clone, Copy, PartialEq, Eq, Debug)]
#[form_root(::swim_form)]
pub struct WarpUplinkPulse {
    /// Uplink open count - close count.
    pub link_count: u64,
    /// The rate at which events are being produced.
    pub event_rate: u64,
    /// The total number of events that have occurred.
    pub event_count: u64,
    /// The rate at which command messages are being produced.
    pub command_rate: u64,
    /// The total number of command messages that have occurred.
    pub command_count: u64,
}

#[derive(Default, Form, Clone, PartialEq, Eq, Debug)]
#[form_root(::swim_form)]
pub struct LanePulse {
    /// Accumulated WARP uplink pulse.
    #[form(name = "uplinkPulse")]
    pub uplink_pulse: WarpUplinkPulse,
}
