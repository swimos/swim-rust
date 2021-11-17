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

use std::pin::Pin;

use futures::task::{Context, Poll};
use futures::Future;
use futures::{ready, TryFuture};

pub use strategy::{ExponentialStrategy, IntervalStrategy, Quantity, RetryStrategy};

#[cfg(test)]
mod tests;

pub mod factory;
pub mod request;
pub mod strategy;
use pin_project::pin_project;
use swim_async_runtime::time::delay::{delay_for, Delay};

/// A future that can be reset back to its initial state and retried once again.
pub trait ResettableFuture: Future {
    /// Reset the future back to its initial state. Returns true if the future successfully reset or
    /// false otherwise.
    ///
    /// Implementations should track errors that occur and use them to determine
    /// whether or not the future can be reset and retried once again.
    fn reset(self: Pin<&mut Self>) -> bool;
}

enum RetryState<Err> {
    Polling,
    Retrying(Option<Err>),
    Sleeping(Pin<Box<Delay>>),
}

/// A future that can be retried with a [`RetryStrategy`].
#[pin_project]
pub struct RetryableFuture<Fut, Err> {
    #[pin]
    future: Fut,
    strategy: RetryStrategy,
    state: RetryState<Err>,
}

impl<Fut> RetryableFuture<Fut, Fut::Error>
where
    Fut: ResettableFuture + TryFuture,
{
    pub fn new(future: Fut, strategy: RetryStrategy) -> Self {
        RetryableFuture {
            future,
            strategy,
            state: RetryState::Polling,
        }
    }
}

impl<Fut> Future for RetryableFuture<Fut, Fut::Error>
where
    Fut: ResettableFuture + TryFuture,
{
    type Output = Result<Fut::Ok, Fut::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            let future = this.future.as_mut();

            let new_state = match &mut this.state {
                RetryState::Polling => match ready!(future.try_poll(cx)) {
                    Ok(out) => return Poll::Ready(Ok(out)),
                    Err(e) => RetryState::Retrying(Some(e)),
                },
                RetryState::Retrying(e) => match this.strategy.next() {
                    Some(s) => {
                        if future.reset() {
                            match s {
                                Some(dur) => RetryState::Sleeping(Box::pin(delay_for(dur))),
                                None => RetryState::Polling,
                            }
                        } else if let Some(e) = e.take() {
                            return Poll::Ready(Err(e));
                        } else {
                            unreachable!()
                        }
                    }
                    None => {
                        if let Some(e) = e.take() {
                            return Poll::Ready(Err(e));
                        } else {
                            unreachable!()
                        }
                    }
                },
                RetryState::Sleeping(delay) => {
                    ready!(delay.as_mut().poll(cx));
                    RetryState::Polling
                }
            };
            *this.state = new_state;
        }
    }
}
