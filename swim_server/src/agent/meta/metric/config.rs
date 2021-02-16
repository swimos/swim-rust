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

use std::num::NonZeroUsize;
use swim_warp::backpressure::KeyedBackpressureConfig;
use tokio::time::Duration;

const DEFAULT_YIELD: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(256) };
const DEFAULT_BUFFER: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(64) };

#[derive(Debug, Eq, PartialEq, Copy, Clone, Default)]
pub struct MetricCollectorConfig {
    /// Configuration for the metric collector task.
    pub task_config: MetricCollectorTaskConfig,
    /// Backpressure release configuration.
    pub backpressure_config: KeyedBackpressureConfig,
}

impl MetricCollectorConfig {
    pub fn split(self) -> (MetricCollectorTaskConfig, KeyedBackpressureConfig) {
        let MetricCollectorConfig {
            task_config,
            backpressure_config,
        } = self;

        (task_config, backpressure_config)
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct MetricCollectorTaskConfig {
    /// Sample rate.
    pub sample_rate: Duration,
    /// Observer channel buffer size.
    pub buffer_size: NonZeroUsize,
    /// The number of events to process before yielding execution back to the runtime.
    pub yield_after: NonZeroUsize,
}

impl Default for MetricCollectorTaskConfig {
    fn default() -> Self {
        MetricCollectorTaskConfig {
            sample_rate: Duration::from_secs(1),
            buffer_size: DEFAULT_BUFFER,
            yield_after: DEFAULT_YIELD,
        }
    }
}
