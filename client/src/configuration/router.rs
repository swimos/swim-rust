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

use crate::configuration::downlink::ConfigParseError;
use common::model::{Attr, Item, Value};
use std::convert::TryFrom;
use swim_form::Form;
use utilities::future::retryable::strategy::RetryStrategy;

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_CONN_REAPER_FREQUENCY: Duration = Duration::from_secs(60);
const DEFAULT_BUFFER_SIZE: usize = 100;

const BUFFER_SIZE_TAG: &str = "buffer_size";
const RETRY_STRATEGY_TAG: &str = "retry_strategy";
const IDLE_TIMEOUT_TAG: &str = "idle_timeout";
const CONN_REAPER_FREQ_TAG: &str = "conn_reaper_frequency";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouterParams {
    /// The retry strategy that will be used when attempting to make a request to a Web Agent.
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
    pub fn new(
        retry_strategy: RetryStrategy,
        idle_timeout: Duration,
        conn_reaper_frequency: Duration,
        buffer_size: NonZeroUsize,
    ) -> RouterParams {
        RouterParams {
            retry_strategy,
            idle_timeout,
            conn_reaper_frequency,
            buffer_size,
        }
    }

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

    pub fn connection_pool_params(&self) -> ConnectionPoolParams {
        ConnectionPoolParams::new(
            self.idle_timeout,
            self.conn_reaper_frequency,
            self.buffer_size,
        )
    }

    pub fn try_from_items(items: Vec<Item>) -> Result<Self, ConfigParseError> {
        let mut retry_strategy: Option<RetryStrategy> = None;
        let mut idle_timeout: Option<Duration> = None;
        let mut conn_reaper_frequency: Option<Duration> = None;
        let mut buffer_size: Option<NonZeroUsize> = None;

        for item in items {
            match item {
                Item::Slot(Value::Text(name), value) => match name.as_str() {
                    RETRY_STRATEGY_TAG => {
                        if let Value::Record(attrs, _) = value {
                            retry_strategy = Some(try_retry_strat_from_value(attrs)?);
                        } else {
                            return Err(ConfigParseError {});
                        }
                    }
                    IDLE_TIMEOUT_TAG => {
                        //Todo replace with direct conversion
                        let timeout_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let timeout =
                            u64::try_from(timeout_as_i32).map_err(|_| ConfigParseError {})?;
                        idle_timeout = Some(Duration::from_secs(timeout))
                    }
                    CONN_REAPER_FREQ_TAG => {
                        //Todo replace with direct conversion
                        let freq_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let freq = u64::try_from(freq_as_i32).map_err(|_| ConfigParseError {})?;
                        conn_reaper_frequency = Some(Duration::from_secs(freq))
                    }
                    BUFFER_SIZE_TAG => {
                        //Todo replace with direct conversion
                        let size_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let size = usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                        buffer_size = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }
                    _ => return Err(ConfigParseError {}),
                },
                _ => return Err(ConfigParseError {}),
            }
        }

        //Todo add defaults
        return match (
            retry_strategy,
            idle_timeout,
            conn_reaper_frequency,
            buffer_size,
        ) {
            (
                Some(retry_strategy),
                Some(idle_timeout),
                Some(conn_reaper_frequency),
                Some(buffer_size),
            ) => Ok(RouterParams::new(
                retry_strategy,
                idle_timeout,
                conn_reaper_frequency,
                buffer_size,
            )),
            _ => Err(ConfigParseError {}),
        };
    }
}

const RETRY_IMMEDIATE_TAG: &str = "immediate";
const RETRY_INTERVAL_TAG: &str = "interval";
const RETRY_EXPONENTIAL_TAG: &str = "exponential";
const RETRY_NONE_TAG: &str = "none";
const RETRIES_TAG: &str = "retries";
const DELAY_TAG: &str = "delay";
const MAX_INTERVAL_TAG: &str = "max_interval";
const MAX_BACKOFF_TAG: &str = "max_backoff";

fn try_retry_strat_from_value(mut attrs: Vec<Attr>) -> Result<RetryStrategy, ConfigParseError> {
    let Attr { name, value } = attrs.pop().ok_or(ConfigParseError {})?;

    match name.as_str() {
        RETRY_IMMEDIATE_TAG => {
            if let Value::Record(_, items) = value {
                try_immediate_strat_from_items(items)
            } else {
                return Err(ConfigParseError {});
            }
        }
        RETRY_INTERVAL_TAG => {
            if let Value::Record(_, items) = value {
                try_interval_strat_from_items(items)
            } else {
                return Err(ConfigParseError {});
            }
        }
        RETRY_EXPONENTIAL_TAG => {
            if let Value::Record(_, items) = value {
                try_exponential_strat_from_items(items)
            } else {
                return Err(ConfigParseError {});
            }
        }
        RETRY_NONE_TAG => Ok(RetryStrategy::none()),
        _ => Err(ConfigParseError {}),
    }
}

fn try_immediate_strat_from_items(items: Vec<Item>) -> Result<RetryStrategy, ConfigParseError> {
    let mut retries: Option<NonZeroUsize> = None;

    for item in items {
        match item {
            Item::Slot(Value::Text(name), value) => match name.as_str() {
                RETRIES_TAG => {
                    //Todo replace with direct conversion
                    let retries_as_i32 =
                        i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                    let num_tries =
                        usize::try_from(retries_as_i32).map_err(|_| ConfigParseError {})?;
                    retries = Some(NonZeroUsize::new(num_tries).ok_or(ConfigParseError {})?);
                }
                _ => return Err(ConfigParseError {}),
            },
            _ => return Err(ConfigParseError {}),
        }
    }

    //Todo add defaults
    match retries {
        Some(retries) => Ok(RetryStrategy::immediate(retries)),
        _ => Err(ConfigParseError {}),
    }
}

fn try_interval_strat_from_items(items: Vec<Item>) -> Result<RetryStrategy, ConfigParseError> {
    let mut delay: Option<Duration> = None;
    let mut retries: Option<NonZeroUsize> = None;

    for item in items {
        match item {
            Item::Slot(Value::Text(name), value) => match name.as_str() {
                DELAY_TAG => {
                    //Todo replace with direct conversion
                    let delay_as_i32 =
                        i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                    let delay_len = u64::try_from(delay_as_i32).map_err(|_| ConfigParseError {})?;
                    delay = Some(Duration::from_secs(delay_len));
                }
                RETRIES_TAG => {
                    // Todo INF if None
                    //Todo replace with direct conversion
                    let retries_as_i32 =
                        i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                    let num_tries =
                        usize::try_from(retries_as_i32).map_err(|_| ConfigParseError {})?;
                    retries = Some(NonZeroUsize::new(num_tries).ok_or(ConfigParseError {})?);
                }
                _ => return Err(ConfigParseError {}),
            },
            _ => return Err(ConfigParseError {}),
        }
    }

    //Todo add defaults
    match (retries, delay) {
        (Some(retries), Some(delay)) => Ok(RetryStrategy::interval(delay, Some(retries))),
        _ => Err(ConfigParseError {}),
    }
}

fn try_exponential_strat_from_items(items: Vec<Item>) -> Result<RetryStrategy, ConfigParseError> {
    let mut max_interval: Option<Duration> = None;
    let mut max_backoff: Option<Duration> = None;

    for item in items {
        match item {
            Item::Slot(Value::Text(name), value) => match name.as_str() {
                MAX_INTERVAL_TAG => {
                    //Todo replace with direct conversion
                    let interval_as_i32 =
                        i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                    let interval =
                        u64::try_from(interval_as_i32).map_err(|_| ConfigParseError {})?;
                    max_interval = Some(Duration::from_secs(interval));
                }
                MAX_BACKOFF_TAG => {
                    // INF if None
                    //Todo replace with direct conversion
                    let backoff_as_i32 =
                        i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                    let backoff = u64::try_from(backoff_as_i32).map_err(|_| ConfigParseError {})?;
                    max_backoff = Some(Duration::from_secs(backoff));
                }
                _ => return Err(ConfigParseError {}),
            },
            _ => return Err(ConfigParseError {}),
        }
    }

    //Todo add defaults
    match (max_interval, max_backoff) {
        (Some(max_interval), Some(max_backoff)) => {
            Ok(RetryStrategy::exponential(max_interval, Some(max_backoff)))
        }
        _ => Err(ConfigParseError {}),
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
    /// Returns a router parameter builder with empty parameters.
    pub fn empty() -> RouterParamBuilder {
        RouterParamBuilder {
            retry_strategy: None,
            idle_timeout: None,
            buffer_size: None,
            conn_reaper_frequency: None,
        }
    }

    /// Returns a new router paremter builder that is initialised with the default values.
    pub fn new() -> RouterParamBuilder {
        RouterParamBuilder {
            retry_strategy: Some(RetryStrategy::default()),
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            conn_reaper_frequency: Some(DEFAULT_CONN_REAPER_FREQUENCY),
            buffer_size: Some(NonZeroUsize::new(DEFAULT_BUFFER_SIZE).unwrap()),
        }
    }

    pub fn build(self) -> RouterParams {
        RouterParams {
            retry_strategy: self
                .retry_strategy
                .expect("Retry strategy must be provided"),
            idle_timeout: self.idle_timeout.expect("Idle timeout must be provided"),
            buffer_size: self.buffer_size.expect("Buffer size must be provided"),
            conn_reaper_frequency: self
                .idle_timeout
                .expect("Idle connection reaper frequency must be provided"),
        }
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

/// Connection pool parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionPoolParams {
    /// How long a connection can be inactive for before it will be pruned.
    idle_timeout: Duration,
    /// How frequently the connection pool reaper will run. Connections that have not been used for
    /// [`idle_timeout`] will be removed.
    conn_reaper_frequency: Duration,
    /// The size of the connection pool request buffer.
    buffer_size: NonZeroUsize,
}

impl ConnectionPoolParams {
    pub fn default() -> ConnectionPoolParams {
        ConnectionPoolParams {
            idle_timeout: Duration::from_secs(60),
            conn_reaper_frequency: Duration::from_secs(60),
            buffer_size: NonZeroUsize::new(5).unwrap(),
        }
    }

    fn new(
        idle_timeout: Duration,
        conn_reaper_frequency: Duration,
        buffer_size: NonZeroUsize,
    ) -> ConnectionPoolParams {
        ConnectionPoolParams {
            idle_timeout,
            conn_reaper_frequency,
            buffer_size,
        }
    }

    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    pub fn conn_reaper_frequency(&self) -> Duration {
        self.conn_reaper_frequency
    }

    pub fn buffer_size(&self) -> NonZeroUsize {
        self.buffer_size
    }
}
