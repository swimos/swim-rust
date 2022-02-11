// Copyright 2015-2021 Swim Inc.
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

use rand::Rng;
use std::num::NonZeroUsize;
use std::time::Duration;

pub const DEFAULT_EXPONENTIAL_MAX_INTERVAL: Duration = Duration::from_secs(16);
pub const DEFAULT_EXPONENTIAL_MAX_BACKOFF: Duration = Duration::from_secs(300);
pub const DEFAULT_IMMEDIATE_RETRIES: usize = 16;
pub const DEFAULT_INTERVAL_RETRIES: usize = 8;
pub const DEFAULT_INTERVAL_DELAY: u64 = 10;

/// The retry strategy that a ['RetryableRequest`] uses to determine when to perform the next
/// request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryStrategy {
    /// An immediate retry with no sleep time in between the requests.
    Immediate(IntervalStrategy),
    /// A retry with a defined delay in between the requests.
    Interval(IntervalStrategy),
    /// A truncated exponential retry strategy.
    Exponential(ExponentialStrategy),
    /// Only attempt the request once
    None(IntervalStrategy),
}

/// Interval strategy parameters with either a defined number of retries to attempt or an indefinite
/// number of retries. Sleeping for the given `delay` in between each request. Immediate retry
/// strategies are backed by this.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntervalStrategy {
    pub retry: Quantity<usize>,
    pub delay: Option<Duration>,
}

/// Truncated exponential retry strategy parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExponentialStrategy {
    /// The maximum interval between a retry, generated intervals will be truncated to this duration
    /// if they exceed it.
    max_interval: Duration,
    /// The maximum backoff time that the strategy will run for. Typically 32 or 64 seconds.
    max_backoff: Quantity<Duration>,
    /// The time that the first request was attempted.
    start: Option<std::time::Instant>,
    /// The current retry number.
    retry_no: u64,
}

impl ExponentialStrategy {
    pub fn get_max_interval(&self) -> Duration {
        self.max_interval
    }

    pub fn get_max_backoff(&self) -> Quantity<Duration> {
        self.max_backoff
    }
}

/// Wrapper around a type that can have finite and infinite values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Quantity<T> {
    Finite(T),
    Infinite,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        RetryStrategy::Exponential(ExponentialStrategy {
            start: None,
            max_interval: DEFAULT_EXPONENTIAL_MAX_INTERVAL,
            max_backoff: Quantity::Finite(DEFAULT_EXPONENTIAL_MAX_BACKOFF),
            retry_no: 0,
        })
    }
}

impl RetryStrategy {
    /// Builds a truncated exponential retry strategy with a `max_interval` between the requests and
    /// a `max_backoff` duration that the requests will be attempted for.
    pub fn exponential(max_interval: Duration, max_backoff: Quantity<Duration>) -> RetryStrategy {
        RetryStrategy::Exponential(ExponentialStrategy {
            start: None,
            max_interval,
            max_backoff,
            retry_no: 0,
        })
    }

    pub fn default_exponential() -> RetryStrategy {
        RetryStrategy::Exponential(ExponentialStrategy {
            start: None,
            max_interval: DEFAULT_EXPONENTIAL_MAX_INTERVAL,
            max_backoff: Quantity::Finite(DEFAULT_EXPONENTIAL_MAX_BACKOFF),
            retry_no: 0,
        })
    }

    /// Builds an immediate retry strategy that will attempt (`retries`) requests with no delay
    /// in between the requests.
    pub fn immediate(retries: NonZeroUsize) -> RetryStrategy {
        RetryStrategy::Immediate(IntervalStrategy {
            retry: Quantity::Finite(retries.get()),
            delay: None,
        })
    }

    pub fn default_immediate() -> RetryStrategy {
        RetryStrategy::Immediate(IntervalStrategy {
            retry: Quantity::Finite(DEFAULT_IMMEDIATE_RETRIES),
            delay: None,
        })
    }

    /// Builds an interval retry strategy that will attempt (`retries`) requests with `delay`
    /// sleep in between the requests.
    pub fn interval(delay: Duration, retries: Quantity<NonZeroUsize>) -> RetryStrategy {
        let retries = {
            if let Quantity::Finite(retries) = retries {
                Quantity::Finite(retries.get())
            } else {
                Quantity::Infinite
            }
        };

        RetryStrategy::Interval(IntervalStrategy {
            retry: retries,
            delay: Some(delay),
        })
    }

    pub fn default_interval() -> RetryStrategy {
        RetryStrategy::Immediate(IntervalStrategy {
            retry: Quantity::Finite(DEFAULT_INTERVAL_RETRIES),
            delay: Some(Duration::from_secs(DEFAULT_INTERVAL_DELAY)),
        })
    }

    /// No retry strategy. Only the initial request is attempted.
    pub const fn none() -> RetryStrategy {
        RetryStrategy::None(IntervalStrategy {
            retry: Quantity::Finite(0),
            delay: None,
        })
    }
}

impl Iterator for RetryStrategy {
    type Item = Option<Duration>;

    fn next(&mut self) -> Option<Self::Item> {
        let decrement_retries = |retry: &mut usize, delay| {
            if *retry == 0 {
                None
            } else {
                *retry -= 1;
                delay
            }
        };

        match self {
            RetryStrategy::Exponential(ref mut strategy) => {
                match (strategy.start, &strategy.max_backoff) {
                    (None, _) => {
                        strategy.start = Some(std::time::Instant::now());
                    }
                    (Some(start), Quantity::Finite(max_backoff)) => {
                        if start.elapsed() > *max_backoff {
                            return None;
                        }
                    }
                    (Some(_), Quantity::Infinite) => {}
                }

                strategy.retry_no += 1;

                // Thread local RNG is used as it will live for the duration of the retry strategy
                let wait = rand::thread_rng().gen_range(0, 1000);
                let duration = Duration::from_millis((2 ^ strategy.retry_no) + wait);
                let sleep_time = std::cmp::min(duration, strategy.max_interval);

                Some(Some(sleep_time))
            }
            RetryStrategy::Immediate(strategy) => match strategy.retry {
                Quantity::Finite(ref mut retry) => decrement_retries(retry, Some(None)),
                Quantity::Infinite => Some(None),
            },
            RetryStrategy::Interval(strategy) => match strategy.retry {
                Quantity::Finite(ref mut retry) => decrement_retries(retry, Some(strategy.delay)),
                Quantity::Infinite => Some(strategy.delay),
            },
            RetryStrategy::None(strategy) => match strategy.retry {
                Quantity::Finite(ref mut retry) => decrement_retries(retry, Some(strategy.delay)),
                Quantity::Infinite => unreachable!(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::retryable::strategy::{Quantity, RetryStrategy};
    use std::time::Duration;
    use swim_algebra::non_zero_usize;

    #[tokio::test]
    async fn test_exponential() {
        use std::time::Instant;

        let max_interval = Duration::from_secs(2);
        let max_backoff = Duration::from_secs(8);
        let strategy = RetryStrategy::exponential(max_interval, Quantity::Finite(max_backoff));
        let start = Instant::now();

        for duration in strategy {
            assert!(duration.is_some());
            let duration = duration.unwrap();

            assert!(duration <= max_interval);
            std::thread::sleep(duration);
        }

        let duration = start.elapsed();
        assert!(duration >= max_backoff && duration <= max_backoff + max_interval);
    }

    #[tokio::test]
    async fn test_immediate() {
        let retries = 5;
        let strategy = RetryStrategy::immediate(non_zero_usize!(retries));
        let mut it = strategy;
        let count = it.count();

        assert_eq!(count, retries);

        for duration in &mut it {
            assert!(duration.is_none());
        }

        assert!(it.next().is_none());
    }

    #[tokio::test]
    async fn test_interval() {
        let retries = 5;
        let expected_duration = Duration::from_secs(1);
        let strategy = RetryStrategy::interval(
            expected_duration,
            Quantity::Finite(non_zero_usize!(retries)),
        );
        let mut it = strategy;
        let count = it.count();

        assert_eq!(count, retries);

        for duration in &mut it {
            assert!(duration.is_some());
            let duration = duration.unwrap();
            assert_eq!(expected_duration, duration);
        }

        assert!(it.next().is_none());
    }
}
