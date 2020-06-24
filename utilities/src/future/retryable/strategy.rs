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

use std::time::Duration;

use rand::Rng;
use std::num::NonZeroUsize;

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

/// Interval strategy parameters with either a defined number of retries to attempt or an indefinate number of
/// retries. Sleeping for the given [`delay`] in between each request. Immediate retry strategies are
/// backed by this.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntervalStrategy {
    /// Indefinate if [`None`]
    retry: Option<usize>,
    delay: Option<Duration>,
}

/// Truncated exponential retry strategy parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExponentialStrategy {
    /// The time that the first request was attempted.
    start: Option<std::time::Instant>,
    /// The maximum interval between a retry, generated intervals will be truncated to this duration
    /// if they exceed it.
    max_interval: Duration,
    /// The maximum backoff time that the strategy will run for. Typically 32 or 64 seconds. Indefinate if
    /// [`None`]
    max_backoff: Option<Duration>,
    /// The current retry number.
    retry_no: u64,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        RetryStrategy::Exponential(ExponentialStrategy {
            start: None,
            max_interval: Duration::from_secs(16),
            max_backoff: None,
            retry_no: 0,
        })
    }
}

impl RetryStrategy {
    /// Builds a truncated exponential retry strategy with a [`max_interval`] between the requests and
    /// a [`max_backoff`] duration that the requests will be attempted for.
    pub fn exponential(max_interval: Duration, max_backoff: Option<Duration>) -> RetryStrategy {
        RetryStrategy::Exponential(ExponentialStrategy {
            start: None,
            max_interval,
            max_backoff,
            retry_no: 0,
        })
    }

    /// Builds an immediate retry strategy that will attempt ([`retries`]) requests with no delay
    /// in between the requests.
    pub fn immediate(retries: NonZeroUsize) -> RetryStrategy {
        RetryStrategy::Immediate(IntervalStrategy {
            retry: Some(retries.get()),
            delay: None,
        })
    }

    /// Builds an interval retry strategy that will attempt ([`retries`]) requests with [`delay`]
    /// sleep in between the requests.
    pub fn interval(delay: Duration, retries: Option<NonZeroUsize>) -> RetryStrategy {
        let retries = {
            if let Some(retries) = retries {
                Some(retries.get())
            } else {
                None
            }
        };

        RetryStrategy::Interval(IntervalStrategy {
            retry: retries,
            delay: Some(delay),
        })
    }

    /// No retry strategy. Only the initial request is attempted.
    pub fn none() -> RetryStrategy {
        RetryStrategy::Interval(IntervalStrategy {
            retry: Some(0),
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
                match (strategy.start, strategy.max_backoff) {
                    (None, _) => {
                        strategy.start = Some(std::time::Instant::now());
                    }
                    (Some(start), Some(max_backoff)) => {
                        if start.elapsed() > max_backoff {
                            return None;
                        }
                    }
                    (Some(_), None) => {}
                }

                strategy.retry_no += 1;

                // Thread local RNG is used as it will live for the duration of the retry strategy
                let wait = rand::thread_rng().gen_range(0, 1000);
                let duration = Duration::from_millis((2 ^ strategy.retry_no) + wait);
                let sleep_time = std::cmp::min(duration, strategy.max_interval);

                Some(Some(sleep_time))
            }
            RetryStrategy::Immediate(strategy) => match strategy.retry {
                Some(ref mut retry) => decrement_retries(retry, Some(None)),
                None => Some(None),
            },
            RetryStrategy::Interval(strategy) => match strategy.retry {
                Some(ref mut retry) => decrement_retries(retry, Some(strategy.delay)),
                None => Some(strategy.delay),
            },
            RetryStrategy::None(strategy) => match strategy.retry {
                Some(ref mut retry) => decrement_retries(retry, Some(strategy.delay)),
                None => unreachable!(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::future::retryable::strategy::RetryStrategy;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    #[tokio::test]
    async fn test_exponential() {
        use std::time::Instant;

        let max_interval = Duration::from_secs(2);
        let max_backoff = Duration::from_secs(8);
        let strategy = RetryStrategy::exponential(max_interval, Some(max_backoff));
        let start = Instant::now();
        let mut it = strategy.into_iter();

        while let Some(duration) = it.next() {
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
        let strategy = RetryStrategy::immediate(NonZeroUsize::new(retries).unwrap());
        let mut it = strategy.into_iter();
        let count = it.count();

        assert_eq!(count, retries);

        while let Some(duration) = it.next() {
            assert!(duration.is_none());
        }

        assert!(it.next().is_none());
    }

    #[tokio::test]
    async fn test_interval() {
        let retries = 5;
        let expected_duration = Duration::from_secs(1);
        let strategy =
            RetryStrategy::interval(expected_duration, Some(NonZeroUsize::new(retries).unwrap()));
        let mut it = strategy.into_iter();
        let count = it.count();

        assert_eq!(count, retries);

        while let Some(duration) = it.next() {
            assert!(duration.is_some());
            let duration = duration.unwrap();
            assert_eq!(expected_duration, duration);
        }

        assert!(it.next().is_none());
    }
}
