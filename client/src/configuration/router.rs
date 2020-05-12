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

use tokio::time::Duration;

use crate::connections::factory::tungstenite::TungsteniteWsFactory;
use crate::connections::Pool;
use utilities::future::retryable::strategy::RetryStrategy;

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_CONN_REAPER_FREQUENCY: Duration = Duration::from_secs(60);
const DEFAULT_BUFFER_SIZE: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouterParams {
    retry_strategy: RetryStrategy,
    /// The maximum amount of time a connection can be inactive for before it will be culled.
    idle_timeout: Duration,
    /// How frequently inactive connections will be culled
    conn_reaper_frequency: Duration,
    buffer_size: NonZeroUsize,
}

impl Default for RouterParams {
    fn default() -> Self {
        RouterParams {
            retry_strategy: RetryStrategy::default(),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            conn_reaper_frequency: DEFAULT_CONN_REAPER_FREQUENCY,
            buffer_size: NonZeroUsize::new(DEFAULT_BUFFER_SIZE).unwrap(),
        }
    }
}

impl RouterParams {
    pub fn buffer_size(&self) -> NonZeroUsize {
        self.buffer_size
    }

    pub fn retry_strategy(&self) -> RetryStrategy {
        self.retry_strategy
    }

    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    pub fn conn_reaper_frequency(&self) -> Duration {
        self.conn_reaper_frequency
    }
}

pub struct RouterParamBuilder {
    retry_strategy: Option<RetryStrategy>,
    idle_timeout: Option<Duration>,
    buffer_size: Option<NonZeroUsize>,
    conn_reaper_frequency: Option<Duration>,
}

impl Default for RouterParamBuilder {
    fn default() -> Self {
        RouterParamBuilder::new()
    }
}

impl RouterParamBuilder {
    pub fn empty() -> RouterParamBuilder {
        RouterParamBuilder {
            retry_strategy: None,
            idle_timeout: None,
            buffer_size: None,
            conn_reaper_frequency: None,
        }
    }

    pub fn new() -> RouterParamBuilder {
        RouterParamBuilder {
            retry_strategy: Some(RetryStrategy::default()),
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            conn_reaper_frequency: Some(DEFAULT_CONN_REAPER_FREQUENCY),
            buffer_size: Some(NonZeroUsize::new(DEFAULT_BUFFER_SIZE).unwrap()),
        }
    }

    pub async fn build<P>(self) -> (RouterParams, P)
    where
        P: Pool + Clone + Send + 'static,
    {
        let buffer_size = self.buffer_size.expect("Buffer size must be provided");

        (
            RouterParams {
                retry_strategy: self
                    .retry_strategy
                    .expect("Retry strategy must be provided"),
                idle_timeout: self.idle_timeout.expect("Idle timeout must be provided"),
                buffer_size,
                conn_reaper_frequency: self
                    .idle_timeout
                    .expect("Idle connection reaper frequency must be provided"),
            },
            P::new(
                buffer_size.get(),
                TungsteniteWsFactory::new(buffer_size.get()).await,
            ),
        )
    }

    pub fn with_buffer_size(mut self, buffer_size: NonZeroUsize) -> RouterParamBuilder {
        self.buffer_size = Some(buffer_size);
        self
    }

    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> RouterParamBuilder {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    pub fn with_conn_reaper_frequency(
        mut self,
        conn_reaper_frequency: Duration,
    ) -> RouterParamBuilder {
        self.conn_reaper_frequency = Some(conn_reaper_frequency);
        self
    }

    pub fn with_retry_stategy(mut self, retry_strategy: RetryStrategy) -> RouterParamBuilder {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
