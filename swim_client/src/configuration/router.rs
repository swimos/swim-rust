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

use tokio::time::Duration;

use crate::configuration::downlink::ConfigParseError;
use crate::configuration::downlink::ROUTER_TAG;
use swim_common::form::Form;
use swim_common::model::{Attr, Item, Value};
use utilities::future::retryable::strategy::{Quantity, RetryStrategy};

const DEFAULT_BUFFER_SIZE: usize = 100;
const DEFAULT_DL_REQUEST_BUFFER_SIZE: usize = 8;
const DEFAULT_YIELD_AFTER: usize = 256;

const BUFFER_SIZE_TAG: &str = "buffer_size";
const RETRY_STRATEGY_TAG: &str = "retry_strategy";
const IDLE_TIMEOUT_TAG: &str = "idle_timeout";
const CONN_REAPER_FREQ_TAG: &str = "conn_reaper_frequency";

/// Configuration parameters for the router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DownlinkConnectionsConfig {
    /// Buffer size for servicing requests for new downlinks.
    pub dl_req_buffer_size: NonZeroUsize,
    /// Size of the internal buffers of the router.
    pub buffer_size: NonZeroUsize,
    /// Number of values to process before yielding to the runtime.
    pub yield_after: NonZeroUsize,
    /// The retry strategy that will be used when attempting to make a request to a Web Agent.
    pub retry_strategy: RetryStrategy,
}

impl Default for DownlinkConnectionsConfig {
    fn default() -> Self {
        DownlinkConnectionsConfig {
            dl_req_buffer_size: NonZeroUsize::new(DEFAULT_DL_REQUEST_BUFFER_SIZE).unwrap(),
            retry_strategy: RetryStrategy::default(),
            buffer_size: NonZeroUsize::new(DEFAULT_BUFFER_SIZE).unwrap(),
            yield_after: NonZeroUsize::new(DEFAULT_YIELD_AFTER).unwrap(),
        }
    }
}

impl DownlinkConnectionsConfig {
    pub fn new(
        dl_req_buffer_size: NonZeroUsize,
        buffer_size: NonZeroUsize,
        yield_after: NonZeroUsize,
        retry_strategy: RetryStrategy,
    ) -> DownlinkConnectionsConfig {
        DownlinkConnectionsConfig {
            dl_req_buffer_size,
            buffer_size,
            yield_after,
            retry_strategy,
        }
    }
    //     pub fn try_from_items(items: Vec<Item>, use_defaults: bool) -> Result<Self, ConfigParseError> {
    //         let mut retry_strategy: Option<RetryStrategy> = None;
    //         let mut idle_timeout: Option<Duration> = None;
    //         let mut conn_reaper_frequency: Option<Duration> = None;
    //         let mut buffer_size: Option<NonZeroUsize> = None;
    //
    //         for item in items {
    //             match item {
    //                 Item::Slot(Value::Text(name), value) => match name.as_str() {
    //                     RETRY_STRATEGY_TAG => match value {
    //                         Value::Record(attrs, items) if attrs.len() <= 1 => {
    //                             retry_strategy =
    //                                 Some(try_retry_strat_from_value(attrs, items, use_defaults)?)
    //                         }
    //                         _ => return Err(ConfigParseError::InvalidValue(value, RETRY_STRATEGY_TAG)),
    //                     },
    //                     IDLE_TIMEOUT_TAG => {
    //                         let timeout = u64::try_from_value(&value)
    //                             .map_err(|_| ConfigParseError::InvalidValue(value, IDLE_TIMEOUT_TAG))?;
    //                         idle_timeout = Some(Duration::from_secs(timeout))
    //                     }
    //                     CONN_REAPER_FREQ_TAG => {
    //                         let freq = u64::try_from_value(&value).map_err(|_| {
    //                             ConfigParseError::InvalidValue(value, CONN_REAPER_FREQ_TAG)
    //                         })?;
    //                         conn_reaper_frequency = Some(Duration::from_secs(freq))
    //                     }
    //                     BUFFER_SIZE_TAG => {
    //                         let size = usize::try_from_value(&value)
    //                             .map_err(|_| ConfigParseError::InvalidValue(value, BUFFER_SIZE_TAG))?;
    //                         buffer_size = Some(NonZeroUsize::new(size).unwrap());
    //                     }
    //                     _ => {
    //                         return Err(ConfigParseError::UnexpectedKey(
    //                             name.to_string(),
    //                             ROUTER_TAG,
    //                         ))
    //                     }
    //                 },
    //                 Item::Slot(value, _) => {
    //                     return Err(ConfigParseError::UnexpectedValue(value, Some(ROUTER_TAG)))
    //                 }
    //                 Item::ValueItem(value) => {
    //                     return Err(ConfigParseError::UnexpectedValue(value, Some(ROUTER_TAG)))
    //                 }
    //             }
    //         }
    //
    //         if use_defaults {
    //             retry_strategy = retry_strategy.or_else(|| Some(RetryStrategy::default()));
    //             idle_timeout = idle_timeout.or(Some(DEFAULT_IDLE_TIMEOUT));
    //             conn_reaper_frequency = conn_reaper_frequency.or(Some(DEFAULT_CONN_REAPER_FREQUENCY));
    //             buffer_size =
    //                 buffer_size.or_else(|| Some(NonZeroUsize::new(DEFAULT_BUFFER_SIZE).unwrap()));
    //         }
    //
    //         Ok(RouterParams::new(
    //             retry_strategy.ok_or(ConfigParseError::MissingKey(RETRY_STRATEGY_TAG, ROUTER_TAG))?,
    //             idle_timeout.ok_or(ConfigParseError::MissingKey(
    //                 RETRY_STRATEGY_TAG,
    //                 IDLE_TIMEOUT_TAG,
    //             ))?,
    //             conn_reaper_frequency.ok_or(ConfigParseError::MissingKey(
    //                 RETRY_STRATEGY_TAG,
    //                 CONN_REAPER_FREQ_TAG,
    //             ))?,
    //             buffer_size.ok_or(ConfigParseError::MissingKey(
    //                 RETRY_STRATEGY_TAG,
    //                 BUFFER_SIZE_TAG,
    //             ))?,
    //         ))
    //     }
}

const RETRY_IMMEDIATE_TAG: &str = "immediate";
const RETRY_INTERVAL_TAG: &str = "interval";
const RETRY_EXPONENTIAL_TAG: &str = "exponential";
const RETRY_NONE_TAG: &str = "none";
const RETRIES_TAG: &str = "retries";
const DELAY_TAG: &str = "delay";
const MAX_INTERVAL_TAG: &str = "max_interval";
const MAX_BACKOFF_TAG: &str = "max_backoff";
const INDEFINITE_TAG: &str = "indefinite";

const DEFAULT_RETRIES: usize = 5;
const DEFAULT_INTERVAL: u64 = 5;
const DEFAULT_MAX_INTERVAL: u64 = 16;
const DEFAULT_BACKOFF: u64 = 32;

fn try_retry_strat_from_value(
    mut attrs: Vec<Attr>,
    items: Vec<Item>,
    use_defaults: bool,
) -> Result<RetryStrategy, ConfigParseError> {
    if let Some(Attr { name, value }) = attrs.pop() {
        match name.as_str() {
            RETRY_IMMEDIATE_TAG => {
                if let Value::Record(_, items) = value {
                    try_immediate_strat_from_items(items, use_defaults)
                } else {
                    Err(ConfigParseError::InvalidValue(value, RETRY_IMMEDIATE_TAG))
                }
            }
            RETRY_INTERVAL_TAG => {
                if let Value::Record(_, items) = value {
                    try_interval_strat_from_items(items, use_defaults)
                } else {
                    Err(ConfigParseError::InvalidValue(value, RETRY_INTERVAL_TAG))
                }
            }
            RETRY_EXPONENTIAL_TAG => {
                if let Value::Record(_, items) = value {
                    try_exponential_strat_from_items(items, use_defaults)
                } else {
                    Err(ConfigParseError::InvalidValue(value, RETRY_EXPONENTIAL_TAG))
                }
            }
            RETRY_NONE_TAG => {
                if let Value::Extant = value {
                    Ok(RetryStrategy::none())
                } else {
                    Err(ConfigParseError::UnexpectedValue(
                        value,
                        Some(RETRY_NONE_TAG),
                    ))
                }
            }
            _ => Err(ConfigParseError::UnexpectedAttribute(
                name.to_string(),
                Some(RETRY_STRATEGY_TAG),
            )),
        }
    } else {
        Err(ConfigParseError::UnnamedRecord(
            Value::Record(attrs, items),
            Some(RETRY_STRATEGY_TAG),
        ))
    }
}

fn try_immediate_strat_from_items(
    items: Vec<Item>,
    use_defaults: bool,
) -> Result<RetryStrategy, ConfigParseError> {
    let mut retries: Option<NonZeroUsize> = None;

    for item in items {
        match item {
            Item::Slot(Value::Text(name), value) => match name.as_str() {
                RETRIES_TAG => {
                    let num_tries = usize::try_from_value(&value)
                        .map_err(|_| ConfigParseError::InvalidValue(value, RETRIES_TAG))?;
                    retries = Some(NonZeroUsize::new(num_tries).unwrap());
                }
                _ => {
                    return Err(ConfigParseError::UnexpectedKey(
                        name.to_string(),
                        RETRY_IMMEDIATE_TAG,
                    ))
                }
            },
            Item::Slot(value, _) => {
                return Err(ConfigParseError::UnexpectedValue(
                    value,
                    Some(RETRY_IMMEDIATE_TAG),
                ))
            }
            Item::ValueItem(value) => {
                return Err(ConfigParseError::UnexpectedValue(
                    value,
                    Some(RETRY_IMMEDIATE_TAG),
                ))
            }
        }
    }

    if use_defaults {
        retries = retries.or_else(|| Some(NonZeroUsize::new(DEFAULT_RETRIES).unwrap()));
    }

    Ok(RetryStrategy::immediate(retries.ok_or(
        ConfigParseError::MissingKey(RETRIES_TAG, RETRY_IMMEDIATE_TAG),
    )?))
}

fn try_interval_strat_from_items(
    items: Vec<Item>,
    use_defaults: bool,
) -> Result<RetryStrategy, ConfigParseError> {
    let mut delay: Option<Duration> = None;
    let mut retries: Option<Quantity<NonZeroUsize>> = None;

    for item in items {
        match item {
            Item::Slot(Value::Text(name), value) => match name.as_str() {
                DELAY_TAG => {
                    let delay_len = u64::try_from_value(&value)
                        .map_err(|_| ConfigParseError::InvalidValue(value, DELAY_TAG))?;
                    delay = Some(Duration::from_secs(delay_len));
                }
                RETRIES_TAG => {
                    if let Value::Text(text_value) = value {
                        if text_value == INDEFINITE_TAG {
                            retries = Some(Quantity::Infinite)
                        } else {
                            return Err(ConfigParseError::InvalidValue(
                                Value::Text(text_value),
                                RETRIES_TAG,
                            ));
                        }
                    } else {
                        let num_tries = usize::try_from_value(&value)
                            .map_err(|_| ConfigParseError::InvalidValue(value, RETRIES_TAG))?;
                        retries = Some(Quantity::Finite(NonZeroUsize::new(num_tries).unwrap()));
                    }
                }
                _ => {
                    return Err(ConfigParseError::UnexpectedKey(
                        name.to_string(),
                        RETRY_INTERVAL_TAG,
                    ))
                }
            },
            Item::Slot(value, _) => {
                return Err(ConfigParseError::UnexpectedValue(
                    value,
                    Some(RETRY_INTERVAL_TAG),
                ))
            }
            Item::ValueItem(value) => {
                return Err(ConfigParseError::UnexpectedValue(
                    value,
                    Some(RETRY_INTERVAL_TAG),
                ))
            }
        }
    }

    if use_defaults {
        retries = retries.or_else(|| {
            Some(Quantity::Finite(
                NonZeroUsize::new(DEFAULT_RETRIES).unwrap(),
            ))
        });
        delay = delay.or_else(|| Some(Duration::from_secs(DEFAULT_INTERVAL)));
    }

    Ok(RetryStrategy::interval(
        delay.ok_or(ConfigParseError::MissingKey(DELAY_TAG, RETRY_INTERVAL_TAG))?,
        retries.ok_or(ConfigParseError::MissingKey(
            RETRIES_TAG,
            RETRY_INTERVAL_TAG,
        ))?,
    ))
}

fn try_exponential_strat_from_items(
    items: Vec<Item>,
    use_defaults: bool,
) -> Result<RetryStrategy, ConfigParseError> {
    let mut max_interval: Option<Duration> = None;
    let mut max_backoff: Option<Quantity<Duration>> = None;

    for item in items {
        match item {
            Item::Slot(Value::Text(name), value) => match name.as_str() {
                MAX_INTERVAL_TAG => {
                    let interval = u64::try_from_value(&value)
                        .map_err(|_| ConfigParseError::InvalidValue(value, MAX_INTERVAL_TAG))?;
                    max_interval = Some(Duration::from_secs(interval));
                }
                MAX_BACKOFF_TAG => {
                    if let Value::Text(text_value) = value {
                        if text_value == INDEFINITE_TAG {
                            max_backoff = Some(Quantity::Infinite)
                        } else {
                            return Err(ConfigParseError::InvalidValue(
                                Value::Text(text_value),
                                MAX_BACKOFF_TAG,
                            ));
                        }
                    } else {
                        let backoff = u64::try_from_value(&value)
                            .map_err(|_| ConfigParseError::InvalidValue(value, MAX_BACKOFF_TAG))?;
                        max_backoff = Some(Quantity::Finite(Duration::from_secs(backoff)));
                    }
                }
                _ => {
                    return Err(ConfigParseError::UnexpectedKey(
                        name.to_string(),
                        RETRY_EXPONENTIAL_TAG,
                    ))
                }
            },
            Item::Slot(value, _) => {
                return Err(ConfigParseError::UnexpectedValue(
                    value,
                    Some(RETRY_EXPONENTIAL_TAG),
                ))
            }
            Item::ValueItem(value) => {
                return Err(ConfigParseError::UnexpectedValue(
                    value,
                    Some(RETRY_EXPONENTIAL_TAG),
                ))
            }
        }
    }

    if use_defaults {
        max_interval = max_interval.or_else(|| Some(Duration::from_secs(DEFAULT_MAX_INTERVAL)));
        max_backoff =
            max_backoff.or_else(|| Some(Quantity::Finite(Duration::from_secs(DEFAULT_BACKOFF))));
    }

    Ok(RetryStrategy::exponential(
        max_interval.ok_or(ConfigParseError::MissingKey(
            MAX_INTERVAL_TAG,
            RETRY_EXPONENTIAL_TAG,
        ))?,
        max_backoff.ok_or(ConfigParseError::MissingKey(
            MAX_BACKOFF_TAG,
            RETRY_EXPONENTIAL_TAG,
        ))?,
    ))
}

//Todo dm this needs to be changed after the new client configuration is finalised.
// pub struct RouterParamBuilder {
//     retry_strategy: Option<RetryStrategy>,
//     idle_timeout: Option<Duration>,
//     buffer_size: Option<NonZeroUsize>,
//     conn_reaper_frequency: Option<Duration>,
// }
//
// impl Default for RouterParamBuilder {
//     fn default() -> Self {
//         RouterParamBuilder::new()
//     }
// }
//
// impl RouterParamBuilder {
//     /// Returns a router parameter builder with empty parameters.
//     pub fn empty() -> RouterParamBuilder {
//         RouterParamBuilder {
//             retry_strategy: None,
//             idle_timeout: None,
//             buffer_size: None,
//             conn_reaper_frequency: None,
//         }
//     }
//
//     /// Returns a new router paremter builder that is initialised with the default values.
//     pub fn new() -> RouterParamBuilder {
//         RouterParamBuilder {
//             retry_strategy: Some(RetryStrategy::default()),
//             idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
//             conn_reaper_frequency: Some(DEFAULT_CONN_REAPER_FREQUENCY),
//             buffer_size: Some(NonZeroUsize::new(DEFAULT_BUFFER_SIZE).unwrap()),
//         }
//     }
//
//     pub fn build(self) -> RouterParams {
//         RouterParams {
//             retry_strategy: self
//                 .retry_strategy
//                 .expect("Retry strategy must be provided"),
//             idle_timeout: self.idle_timeout.expect("Idle timeout must be provided"),
//             buffer_size: self.buffer_size.expect("Buffer size must be provided"),
//             conn_reaper_frequency: self
//                 .idle_timeout
//                 .expect("Idle connection reaper frequency must be provided"),
//         }
//     }
//
//     pub fn with_buffer_size(mut self, buffer_size: NonZeroUsize) -> RouterParamBuilder {
//         self.buffer_size = Some(buffer_size);
//         self
//     }
//
//     pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> RouterParamBuilder {
//         self.idle_timeout = Some(idle_timeout);
//         self
//     }
//
//     pub fn with_conn_reaper_frequency(
//         mut self,
//         conn_reaper_frequency: Duration,
//     ) -> RouterParamBuilder {
//         self.conn_reaper_frequency = Some(conn_reaper_frequency);
//         self
//     }
//
//     pub fn with_retry_stategy(mut self, retry_strategy: RetryStrategy) -> RouterParamBuilder {
//         self.retry_strategy = Some(retry_strategy);
//         self
//     }
// }
