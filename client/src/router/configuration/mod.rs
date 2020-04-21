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

use crate::router::outgoing::retry::RetryStrategy;

// TODO: This should be moved up to the `configuration` module. Or the 'configuration' module should pull the various configurations together from modules
#[derive(Clone, Copy)]
pub struct RouterConfig {
    retry_strategy: RetryStrategy,
    /// The maximum amount of time (in seconds) a connection can be inactive for before it will be culled.
    idle_timeout: NonZeroUsize,
    /// How frequently (in seconds) inactive connections will be culled
    conn_reaper_frequency: NonZeroUsize,
    buffer_size: NonZeroUsize,
}

impl RouterConfig {
    pub fn buffer_size(&self) -> NonZeroUsize {
        self.buffer_size
    }

    pub fn retry_strategy(&self) -> RetryStrategy {
        self.retry_strategy
    }

    pub fn idle_timeout(&self) -> NonZeroUsize {
        self.idle_timeout
    }

    pub fn conn_reaper_frequency(&self) -> NonZeroUsize {
        self.conn_reaper_frequency
    }
}

pub struct RouterConfigBuilder {
    retry_strategy: Option<RetryStrategy>,
    idle_timeout: Option<usize>,
    buffer_size: Option<usize>,
    conn_reaper_frequency: Option<usize>,
}

impl Default for RouterConfigBuilder {
    fn default() -> Self {
        RouterConfigBuilder {
            retry_strategy: None,
            idle_timeout: None,
            buffer_size: None,
            conn_reaper_frequency: None,
        }
    }
}

impl RouterConfigBuilder {
    pub fn build(self) -> RouterConfig {
        let build_usize = |u: Option<usize>, m: &str| match u {
            Some(u) => match NonZeroUsize::new(u) {
                Some(u) => u,
                _ => panic!(m.to_owned() + "s must be positive"),
            },
            None => panic!("{} must be provided", m.to_owned()),
        };

        RouterConfig {
            retry_strategy: self
                .retry_strategy
                .unwrap_or_else(|| panic!("Router retry strategy must be provided")),
            idle_timeout: { build_usize(self.idle_timeout, "Idle timeout") },
            buffer_size: { build_usize(self.idle_timeout, "Buffer size") },
            conn_reaper_frequency: {
                build_usize(self.conn_reaper_frequency, "Connection reaper frequency")
            },
        }
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> RouterConfigBuilder {
        self.buffer_size = Some(buffer_size);
        self
    }

    pub fn with_idle_timeout(mut self, idle_timeout: usize) -> RouterConfigBuilder {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    pub fn with_conn_reaper_frequency(
        mut self,
        conn_reaper_frequency: usize,
    ) -> RouterConfigBuilder {
        self.conn_reaper_frequency = Some(conn_reaper_frequency);
        self
    }

    pub fn with_retry_stategy(mut self, retry_strategy: RetryStrategy) -> RouterConfigBuilder {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
