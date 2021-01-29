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

use pin_utils::core_reexport::num::NonZeroUsize;
use tokio::time::Duration;

#[derive(Debug)]
pub struct MetricCollectorConfig {
    /// Sample rate.
    pub sample_rate: Duration,
    /// Observer channel buffer size.
    pub buffer_size: NonZeroUsize,
}

impl Default for MetricCollectorConfig {
    fn default() -> Self {
        MetricCollectorConfig {
            sample_rate: Duration::from_secs(1),
            buffer_size: NonZeroUsize::new(10).unwrap(),
        }
    }
}
