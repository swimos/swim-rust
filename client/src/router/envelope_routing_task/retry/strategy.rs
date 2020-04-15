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

#[derive(Clone, Copy)]
pub enum RetryStrategy {
    Immediate(IntervalStrategy),
    Interval(IntervalStrategy),
    Exponential(ExponentialStrategy),
}

#[derive(Clone, Copy)]
pub struct IntervalStrategy {
    /// Indefinate if `None`
    retry: Option<usize>,
    delay: Duration,
}

#[derive(Clone, Copy)]
pub struct ExponentialStrategy {
    start: Option<std::time::Instant>,
    max_interval: Duration,
    /// Typically 32 or 64 seconds
    max_backoff: Duration,
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
    pub fn exponential(max_interval: Duration, max_backoff: Duration) -> RetryStrategy {
        RetryStrategy::Exponential(ExponentialStrategy {
            start: None,
            max_interval,
            max_backoff,
            retry_no: 0,
        })
    }

    pub fn immediate(retries: usize) -> RetryStrategy {
        if retries < 1 {
            panic!("Retry count must be positive")
        }

        RetryStrategy::Immediate(IntervalStrategy {
            retry: Some(retries),
            delay: Duration::from_secs(0),
        })
    }

    pub fn interval(delay: Duration, retries: usize) -> RetryStrategy {
        if retries < 1 {
            panic!("Retry count must be positive")
        }

        RetryStrategy::Interval(IntervalStrategy {
            retry: Some(retries),
            delay,
        })
    }
}

impl Iterator for RetryStrategy {
    type Item = Duration;

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
                let wait = rand::thread_rng().gen_range(0, 1000);
                let duration = Duration::from_millis((2 ^ strategy.retry_no) + wait);
                let sleep_time = std::cmp::min(duration, strategy.max_interval);

                Some(sleep_time)
            }
            RetryStrategy::Interval(strategy) | RetryStrategy::Immediate(strategy) => {
                match strategy.retry {
                    Some(ref mut retry) => {
                        if *retry == 0 {
                            None
                        } else {
                            *retry -= 1;
                            Some(strategy.delay)
                        }
                    }
                    None => Some(strategy.delay),
                }
            }
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
    let expected_duration = Duration::from_secs(0);
    let mut it = strategy.into_iter();
    let count = it.count();

    assert_eq!(count, retries);

    while let Some(duration) = it.next() {
        assert_eq!(expected_duration, duration);
    }

    assert!(it.next().is_none());
}

#[tokio::test]
async fn test_interval() {
    let retries = 5;
    let expected_duration = Duration::from_secs(1);
    let strategy = RetryStrategy::interval(expected_duration, retries);
    let mut it = strategy.into_iter();
    let count = it.count();

    assert_eq!(count, retries);

    while let Some(duration) = it.next() {
        assert_eq!(expected_duration, duration);
    }

    assert!(it.next().is_none());
}
