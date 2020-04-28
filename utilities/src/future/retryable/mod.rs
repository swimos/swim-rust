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

use std::pin::Pin;

use futures::task::{Context, Poll};
use futures::{ready, TryFuture};
use futures::{Future, FutureExt};
use tokio::time::{delay_for, Delay};

use crate::future::retryable::strategy::RetryStrategy;

#[cfg(test)]
mod tests;

pub mod strategy;

/// A future that can be reset back to its initial state and retried once again.
pub trait ResettableFuture {
    /// Reset the future back to its initial state. Returns true if the future successfully reset or
    /// false otherwise.
    /// Implementations should track errors that occur and use them to determine
    /// whether or not the future can be reset and retried once again.
    fn reset(self: Pin<&mut Self>) -> bool;
}

enum RetryState {
    Polling,
    Sleeping(Delay),
}

/// A future that can be retried with a [`RetryStrategy`].
pub struct RetryableFuture<Fut> {
    future: Fut,
    strategy: RetryStrategy,
    state: RetryState,
}

impl<Fut> RetryableFuture<Fut> {
    pub fn new(future: Fut, strategy: RetryStrategy) -> Self {
        RetryableFuture {
            future,
            strategy,
            state: RetryState::Polling,
        }
    }
}

impl<Fut> Future for RetryableFuture<Fut>
where
    Fut: ResettableFuture + TryFuture + Unpin,
{
    type Output = Result<Fut::Ok, Fut::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                RetryState::Polling => match ready!(Pin::new(&mut this.future).try_poll(cx)) {
                    Ok(out) => return Poll::Ready(Ok(out)),
                    Err(e) => match this.strategy.next() {
                        Some(s) => {
                            if Pin::new(&mut this.future).reset() {
                                match s {
                                    Some(dur) => {
                                        this.state = RetryState::Sleeping(delay_for(dur));
                                    }
                                    None => {
                                        this.state = RetryState::Polling;
                                    }
                                }
                            } else {
                                return Poll::Ready(Err(e));
                            }
                        }
                        None => return Poll::Ready(Err(e)),
                    },
                },
                RetryState::Sleeping(delay) => {
                    ready!(delay.poll_unpin(cx));
                    this.state = RetryState::Polling;
                }
            }
        }
    }
}
