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
use std::time::Duration;
use utilities::future::retryable::strategy::RetryStrategy;

#[derive(Debug, Clone, Copy)]
pub struct ConnectionConfig {
    pub router_buffer_size: NonZeroUsize,
    pub channel_buffer_size: NonZeroUsize,
    pub activity_timeout: Duration,
    pub connection_retries: RetryStrategy,
}
