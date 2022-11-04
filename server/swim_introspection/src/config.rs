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

use std::time::Duration;

const DEFAULT_PULSE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntrospectionConfig {
    pub node_pulse_interval: Duration,
    pub lane_pulse_interval: Duration,
}

impl Default for IntrospectionConfig {
    fn default() -> Self {
        Self {
            node_pulse_interval: DEFAULT_PULSE_INTERVAL,
            lane_pulse_interval: DEFAULT_PULSE_INTERVAL,
        }
    }
}
