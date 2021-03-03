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

use crate::agent::lane::channels::uplink::backpressure::KeyedBackpressureConfig;
use std::num::NonZeroUsize;
use tokio::time::Duration;

#[derive(Debug, Clone)]
pub struct MetricAggregatorConfig {
    /// Sample rate.
    pub sample_rate: Duration,
    /// Observer channel buffer size.
    pub buffer_size: NonZeroUsize,
    /// The number of events to process before yielding execution back to the runtime.
    pub yield_after: NonZeroUsize,
    pub backpressure_config: KeyedBackpressureConfig,
}

impl Default for MetricAggregatorConfig {
    fn default() -> Self {
        MetricAggregatorConfig {
            sample_rate: Duration::from_secs(1),
            buffer_size: NonZeroUsize::new(10).unwrap(),
            yield_after: NonZeroUsize::new(256).unwrap(),
            backpressure_config: KeyedBackpressureConfig {
                buffer_size: NonZeroUsize::new(2).unwrap(),
                yield_after: NonZeroUsize::new(256).unwrap(),
                bridge_buffer_size: NonZeroUsize::new(4).unwrap(),
                cache_size: NonZeroUsize::new(4).unwrap(),
            },
        }
    }
}
