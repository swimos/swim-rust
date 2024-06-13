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

use std::{num::NonZeroUsize, time::Duration};

use swimos_utilities::non_zero_usize;

const DEFAULT_PULSE_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_REG_CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(8);

/// Configuration for the introspection meta agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntrospectionConfig {
    /// Frequency at which the node meta agents will generate an uplink pulse.
    pub node_pulse_interval: Duration,
    /// Frequency at which the lane meta agents will generate an uplink pulse.
    pub lane_pulse_interval: Duration,
    /// Size of the buffer for registering new lanes with the introspection system.
    pub registration_channel_size: NonZeroUsize,
}

impl Default for IntrospectionConfig {
    fn default() -> Self {
        Self {
            node_pulse_interval: DEFAULT_PULSE_INTERVAL,
            lane_pulse_interval: DEFAULT_PULSE_INTERVAL,
            registration_channel_size: DEFAULT_REG_CHANNEL_SIZE,
        }
    }
}
