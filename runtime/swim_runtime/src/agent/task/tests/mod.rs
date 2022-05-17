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

use std::{num::NonZeroUsize, time::Duration};

use swim_api::agent::LaneConfig;
use swim_utilities::algebra::non_zero_usize;

use crate::agent::AgentRuntimeConfig;

mod read;
mod write;

const QUEUE_SIZE: NonZeroUsize = non_zero_usize!(8);
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

fn make_config(inactive_timeout: Duration) -> AgentRuntimeConfig {
    AgentRuntimeConfig {
        default_lane_config: LaneConfig {
            input_buffer_size: BUFFER_SIZE,
            output_buffer_size: BUFFER_SIZE,
        },
        attachment_queue_size: non_zero_usize!(8),
        inactive_timeout,
        prune_remote_delay: inactive_timeout,
        shutdown_timeout: SHUTDOWN_TIMEOUT,
    }
}

const VAL_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const INACTIVE_TEST_TIMEOUT: Duration = Duration::from_millis(100);
