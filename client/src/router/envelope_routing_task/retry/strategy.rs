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

use rand;
use rand::Rng;

/// The retry strategy that a ['RetryableRequest`] should use to determine when to perform the next
/// request.
#[derive(Clone, Copy)]
pub enum RetryStrategy {
    /// An immediate retry with no sleep time in between the requests.
    Immediate(IntervalStrategy),
    /// A retry with a defined delay in between the requests.
    Interval(IntervalStrategy),
    /// A truncated exponential retry strategy.
    Exponential(ExponentialStrategy),
}

/// Interval strategy parameters with either a defined number of retries to attempt or an indefinate number of
/// retries. Sleeping for the given [`delay`] in between each request. Immediate retry strategies are
/// backed by this.
#[derive(Clone, Copy)]
pub struct IntervalStrategy {
    /// Indefinate if [`None`]
    retry: Option<usize>,
    delay: Option<Duration>,
}

/// Truncated exponential retry strategy parameters.
#[derive(Clone, Copy)]
pub struct ExponentialStrategy {
    /// The time that the first request was attempted.
    start: Option<std::time::Instant>,
    /// The maximum interval between a retry, generated intervals will be truncated to this duration
    /// if they exceed it.
    max_interval: Duration,
    /// The maximum backoff time that the strategy will run for. Typically 32 or 64 seconds
    max_backoff: Duration,
    /// The current retry number.
    retry_no: u64,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        RetryStrategy::Exponential(ExponentialStrategy {
            start: None,
            max_interval: Duration::from_millis(500),
            max_backoff: Duration::from_secs(32),
            retry_no: 0,
        })
    }
}

impl RetryStrategy {
    /// Builds a truncated exponential retry strategy with a [`max_interval`] between the requests and
    /// a [`max_backoff`] duration that the requests will be attempted for.
    pub fn exponential(max_interval: Duration, max_backoff: Duration) -> RetryStrategy {
        RetryStrategy::Exponential(ExponentialStrategy {
            start: None,
            max_interval,
            max_backoff,
            retry_no: 0,
        })
    }

    /// Builds an immediate retry strategy that will attempt ([`retries`]) requests with no delay
    /// in between the requests. Panics is [`retries`] is less than 1.
    pub fn immediate(retries: usize) -> RetryStrategy {
        if retries < 1 {
            panic!("Retry count must be positive")
        }

        RetryStrategy::Immediate(IntervalStrategy {
            retry: Some(retries),
            delay: None,
        })
    }

    /// Builds an interval retry strategy that will attempt ([`retries`]) requests with [`delay`]
    /// sleep in between the requests. Panics is [`retries`] is some and less than 1.
    pub fn interval(delay: Duration, retries: Option<usize>) -> RetryStrategy {
        if let Some(retries) = retries {
            if retries < 1 {
                panic!("Retry count must be positive")
            }
        };

        RetryStrategy::Interval(IntervalStrategy {
            retry: retries,
            delay: Some(delay),
        })
    }
}

impl Iterator for RetryStrategy {
    type Item = Option<Duration>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RetryStrategy::Exponential(ref mut strategy) => {
                match strategy.start {
                    Some(t) => {
                        if t.elapsed() > strategy.max_backoff {
                            return None;
                        }
                    }
                    None => {
                        strategy.start = Some(std::time::Instant::now());
                    }
                }

                strategy.retry_no += 1;
                // Thread local RNG is used as it will live for the duration of the retry strategy
                let wait = rand::thread_rng().gen_range(0, 1000);
                let duration = Duration::from_millis((2 ^ strategy.retry_no) + wait);
                let sleep_time = std::cmp::min(duration, strategy.max_interval);

                Some(Some(sleep_time))
            }
            RetryStrategy::Immediate(strategy) => match strategy.retry {
                Some(ref mut retry) => {
                    if *retry == 0 {
                        None
                    } else {
                        *retry -= 1;
                        Some(None)
                    }
                }
                None => Some(None),
            },
            RetryStrategy::Interval(strategy) => match strategy.retry {
                Some(ref mut retry) => {
                    if *retry == 0 {
                        None
                    } else {
                        *retry -= 1;
                        Some(strategy.delay)
                    }
                }
                None => Some(strategy.delay),
            },
        }
    }
}

#[tokio::test]
async fn test_exponential() {
    use std::time::Instant;

    let max_interval = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(8);
    let strategy = RetryStrategy::exponential(max_interval, max_backoff);
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
    let strategy = RetryStrategy::immediate(retries);
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
    let strategy = RetryStrategy::interval(expected_duration, Some(retries));
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
