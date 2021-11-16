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

use futures::stream::{FusedStream, FuturesUnordered};
use futures::task::{AtomicWaker, Context, Poll};
use futures::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;

#[cfg(test)]
mod tests;

/// A wrapper around [`FuturesUnordered`] that makes it easier to use with the `select!` and
/// `select_biased!` macros. In contrast to [`FuturesUnordered`], this will only be marked as
/// terminated when it is explicitly closed rather than each time it becomes empty.
#[derive(Debug)]
pub struct OpenEndedFutures<Fut> {
    inner: FuturesUnordered<Fut>,
    waker: AtomicWaker,
    stopped: bool,
}

impl<Fut> Default for OpenEndedFutures<Fut> {
    fn default() -> Self {
        OpenEndedFutures {
            inner: FuturesUnordered::default(),
            waker: AtomicWaker::default(),
            stopped: false,
        }
    }
}

impl<Fut> OpenEndedFutures<Fut> {
    pub fn new() -> Self {
        OpenEndedFutures::default()
    }

    /// After stopping `terminated` will return true and no more futures will be accepted.
    pub fn stop(&mut self) {
        self.stopped = true;
        self.waker.wake();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// Pushing will succeed if `stop` has not been called, otherwise the future will be returned
    /// to the caller.
    pub fn try_push(&self, fut: Fut) -> Result<(), Fut> {
        if self.stopped {
            Err(fut)
        } else {
            let was_empty = self.inner.is_empty();
            self.inner.push(fut);
            if was_empty {
                self.waker.wake();
            }
            Ok(())
        }
    }

    pub fn push(&self, fut: Fut) {
        if self.try_push(fut).is_err() {
            panic!("Future pushed after closed.")
        }
    }
}

impl<Fut: Future> Stream for OpenEndedFutures<Fut> {
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.inner.is_empty() && !self.stopped {
            self.waker.register(cx.waker());
            Poll::Pending
        } else {
            self.inner.poll_next_unpin(cx)
        }
    }
}

impl<Fut: Future> FusedStream for OpenEndedFutures<Fut> {
    fn is_terminated(&self) -> bool {
        self.stopped && self.inner.is_terminated()
    }
}
