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

use crate::future::retryable::strategy::RetryStrategy;
use tokio::time::{delay_for, Delay};

#[cfg(test)]
mod tests;

pub mod strategy;

pub trait ResettableFuture {
    fn reset(self: Pin<&mut Self>) -> bool;
}

pub enum RetryState {
    Polling,
    Sleeping(Delay),
}

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
                    Err(e) => {
                        if Pin::new(&mut this.future).reset() {
                            match this.strategy.next() {
                                Some(Some(dur)) => {
                                    this.state = RetryState::Sleeping(delay_for(dur));
                                }
                                Some(None) => {
                                    this.state = RetryState::Polling;
                                }
                                _ => return Poll::Ready(Err(e)),
                            }
                        } else {
                            return Poll::Ready(Err(e));
                        }
                    }
                },
                RetryState::Sleeping(delay) => {
                    ready!(delay.poll_unpin(cx));
                    this.state = RetryState::Polling;
                }
            }
        }
    }
}
