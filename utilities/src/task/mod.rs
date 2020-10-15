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

#[cfg(test)]
mod tests;

use crate::future::open_ended::OpenEndedFutures;
use futures::stream::FusedStream;
use futures::task::{Context, Poll};
use futures::{ready, Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use tokio::task::JoinHandle;

/// Trait for task spawners that will concurrently execute futures passed to them.
pub trait Spawner<F: Future>: FusedStream<Item = F::Output> {
    /// Try to add a new future. This should only fail if the spawner is no longer able to
    /// accept tasks (for example, when closing).
    fn try_add(&self, fut: F) -> Result<(), F>;

    fn add(&self, fut: F) {
        if self.try_add(fut).is_err() {
            panic!("Future added after closed.");
        }
    }

    /// Determine if the spawner is running any tasks.
    fn is_empty(&self) -> bool;

    /// Instruct the spawner to stop accepting new tasks and eventually terminated.
    fn stop(&mut self);
}

#[derive(Debug)]
pub struct TokioSpawner<Fut: Future>(OpenEndedFutures<JoinHandle<Fut::Output>>);

impl<Fut: Future> Default for TokioSpawner<Fut> {
    fn default() -> Self {
        TokioSpawner(Default::default())
    }
}

impl<Fut: Future> TokioSpawner<Fut> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn stop(&mut self) {
        self.0.stop()
    }
}

impl<F> Spawner<F> for OpenEndedFutures<F>
where
    F: Future + Send + 'static,
{
    fn try_add(&self, fut: F) -> Result<(), F> {
        self.try_push(fut)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn stop(&mut self) {
        OpenEndedFutures::stop(self)
    }
}

impl<Fut> Stream for TokioSpawner<Fut>
where
    Fut: Future + Send + 'static,
{
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let poll_result = ready!(self.as_mut().get_mut().0.poll_next_unpin(cx));
            match poll_result {
                Some(Ok(out)) => {
                    break Poll::Ready(Some(out));
                }
                Some(Err(e)) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    }
                }
                _ => {
                    break Poll::Ready(None);
                }
            }
        }
    }
}

impl<Fut> FusedStream for TokioSpawner<Fut>
where
    Fut: Future + Send + 'static,
{
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }
}

impl<Fut> Spawner<Fut> for TokioSpawner<Fut>
where
    Fut: Future + Send + Sync + 'static,
    Fut::Output: Send,
{
    fn try_add(&self, fut: Fut) -> Result<(), Fut> {
        let TokioSpawner(inner) = self;
        if inner.is_stopped() {
            Err(fut)
        } else {
            if inner.try_push(tokio::task::spawn(fut)).is_err() {
                panic!("Future rejected when not stopped.");
            }
            Ok(())
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn stop(&mut self) {
        self.0.stop()
    }
}
