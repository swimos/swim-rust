// Copyright 2015-2021 SWIM.AI inc.
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

use crate::uplink::MetricBackpressureConfig;
use std::num::NonZeroUsize;
use swim_utilities::algebra::non_zero_usize;
use tokio::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricAggregatorConfig {
    /// Sample report rate of events.
    pub sample_rate: Duration,
    /// Observer channel buffer size.
    pub buffer_size: NonZeroUsize,
    /// The number of events to process before yielding execution back to the runtime.
    pub yield_after: NonZeroUsize,
    /// Backpressure relief configuration for WARP Uplink profile ingress.
    pub backpressure_config: MetricBackpressureConfig,
}

impl Default for MetricAggregatorConfig {
    fn default() -> Self {
        MetricAggregatorConfig {
            sample_rate: Duration::from_secs(1),
            buffer_size: non_zero_usize!(10),
            yield_after: non_zero_usize!(256),
            backpressure_config: MetricBackpressureConfig {
                buffer_size: non_zero_usize!(2),
                yield_after: non_zero_usize!(256),
                bridge_buffer_size: non_zero_usize!(4),
                cache_size: non_zero_usize!(4),
            },
        }
    }
}
