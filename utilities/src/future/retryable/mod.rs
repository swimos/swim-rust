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

use futures::ready;
use futures::task::{Context, Poll};
use futures::{Future, FutureExt};
use tokio::time;

use pin_project::{pin_project, project};

use crate::future::retryable::strategy::RetryStrategy;

#[cfg(test)]
mod tests;

pub mod strategy;

#[pin_project]
pub struct Retry<Retryable, Err, Fut> {
    f: Retryable,
    #[pin]
    state: RetryState<Fut, Err>,
    ctx: RetryContext<Err>,
    strategy: RetryStrategy,
}

#[pin_project]
enum RetryState<Fut, Err> {
    NotStarted,
    Pending(#[pin] Fut),
    Retrying(Err),
    Sleeping(time::Delay),
}

pub struct RetryContext<Err> {
    last_err: Option<Err>,
}

impl<Retryable, Err, Fut> Retry<Retryable, Err, Fut> {
    pub fn new(f: Retryable, strategy: RetryStrategy) -> Retry<Retryable, Err, Fut> {
        Retry {
            f,
            state: RetryState::NotStarted,
            ctx: RetryContext { last_err: None },
            strategy,
        }
    }
}

impl<Retryable, FutOk, FutErr, Fut> Future for Retry<Retryable, FutErr, Fut>
where
    Retryable: RetryableFuture<Ok = FutOk, Err = FutErr, Future = Fut>,
    Fut: Future<Output = Result<FutOk, FutErr>> + Send + Unpin,
    FutErr: Clone,
{
    type Output = Fut::Output;

    #[project]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        loop {
            #[project]
            let next_state = match this.state.as_mut().project() {
                RetryState::NotStarted => {
                    let fut = this.f.future(this.ctx);
                    RetryState::Pending(fut)
                }
                RetryState::Pending(fut) => match ready!(fut.poll(cx)) {
                    Ok(r) => {
                        return Poll::Ready(Ok(r));
                    }
                    Err(e) => {
                        if this.f.retry(this.ctx) {
                            RetryState::Retrying(e)
                        } else {
                            return Poll::Ready(Err(e));
                        }
                    }
                },
                RetryState::Retrying(e) => match this.strategy.next() {
                    Some(duration) => {
                        this.ctx.last_err = Some(e.clone());

                        match duration {
                            Some(duration) => RetryState::Sleeping(time::delay_for(duration)),
                            None => RetryState::NotStarted,
                        }
                    }
                    None => {
                        return Poll::Ready(Err(e.clone()));
                    }
                },
                RetryState::Sleeping(timer) => {
                    ready!(timer.poll_unpin(cx));
                    RetryState::NotStarted
                }
            };

            this.state.set(next_state);
        }
    }
}

pub trait FutureFactory<'f, Ok, Err>: Unpin {
    type Future: Future<Output = Result<Ok, Err>> + Send + Unpin + 'f;

    fn future(&'f mut self, ctx: &mut RetryContext<Err>) -> Self::Future;
}

pub trait RetryableFuture:
    for<'f> FutureFactory<'f, <Self as RetryableFuture>::Ok, <Self as RetryableFuture>::Err> + Unpin
{
    type Ok;
    type Err: Clone;

    fn retry(&mut self, ctx: &mut RetryContext<Self::Err>) -> bool;
}

impl<'f, F, Ok, Err> FutureFactory<'f, Ok, Err> for F
where
    F: Future<Output = Result<Ok, Err>> + Send + Unpin + Clone + 'f,
{
    type Future = Self;

    fn future(&'f mut self, _ctx: &mut RetryContext<Err>) -> Self::Future {
        self.clone()
    }
}
