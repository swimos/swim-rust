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

use std::num::NonZeroUsize;
use std::time::Duration;
use utilities::future::retryable::strategy::RetryStrategy;

/// Configuration parameters for remote connection management.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionConfig {
    /// Buffer size for sending routing requests for a router instance.
    pub router_buffer_size: NonZeroUsize,
    /// Buffer size for the channel to send data to the task managing a single connection.
    pub channel_buffer_size: NonZeroUsize,
    /// Time after which to close an inactive connection.
    pub activity_timeout: Duration,
    /// Strategy for retrying a connection.
    pub connection_retries: RetryStrategy,
    /// The number of events to process before yielding execution back to the runtime.
    pub yield_after: NonZeroUsize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            router_buffer_size: NonZeroUsize::new(10).unwrap(),
            channel_buffer_size: NonZeroUsize::new(10).unwrap(),
            activity_timeout: Duration::new(30, 00),
            connection_retries: Default::default(),
            yield_after: NonZeroUsize::new(256).unwrap(),
        }
    }
}
