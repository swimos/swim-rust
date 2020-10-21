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

use futures::future::FusedFuture;
use futures::stream::FuturesUnordered;
use futures::{ready, FutureExt, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// Selects between a dynamic collection of Tokio MPSC senders, pseudo-randomly choosing one that
/// is ready to take accept a value. Failed senders will remain in the selector and are counted
/// as ready and will be eligible for selection.
///
/// Senders in the selector have associated labels so the caller to select can distinguish them.
pub struct Selector<T, L>(FuturesUnordered<ReadyFuture<T, L>>);

impl<T, L> Default for Selector<T, L>
where
    T: Send + Unpin,
    L: Send + Unpin,
{
    fn default() -> Self {
        let mut inner = FuturesUnordered::default();
        let _ = inner.next().now_or_never();
        Selector(inner)
    }
}

type SelectResult<T, L> = Option<(L, Result<mpsc::Sender<T>, ()>)>;

impl<T, L> Selector<T, L>
where
    T: Send + Unpin,
    L: Send + Unpin,
{
    /// True when there are no pending senders in the selector. Note that this will still be true
    /// when the selector contains only failed senders.
    pub fn is_empty(&self) -> bool {
        let Selector(inner) = self;
        inner.is_empty()
    }

    /// Add a new sender to the selector.
    pub fn add(&mut self, label: L, sender: mpsc::Sender<T>) {
        let Selector(inner) = self;
        inner.push(ReadyFuture::new(label, sender));
    }

    /// Select a ready sender. This will return `None` immediately if the sender is empty and will
    /// block until a sender becomes available if they are all blocked.
    pub fn select<'a>(&'a mut self) -> impl FusedFuture<Output = SelectResult<T, L>> + Send + 'a {
        let Selector(inner) = self;
        inner.next()
    }
}

struct ReadyFutureInner<T, L> {
    sender: mpsc::Sender<T>,
    label: L,
}

struct ReadyFuture<T, L> {
    inner: Option<ReadyFutureInner<T, L>>,
}

impl<T, L> ReadyFuture<T, L> {
    fn new(label: L, sender: mpsc::Sender<T>) -> Self {
        ReadyFuture {
            inner: Some(ReadyFutureInner { label, sender }),
        }
    }
}

impl<T: Unpin, L: Unpin> Future for ReadyFuture<T, L> {
    type Output = (L, Result<mpsc::Sender<T>, ()>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ReadyFutureInner { sender, .. } = self
            .as_mut()
            .get_mut()
            .inner
            .as_mut()
            .expect("Ready future polled twice.");
        let result = ready!(sender.poll_ready(cx));
        let ReadyFutureInner { sender, label } = match self.get_mut().inner.take() {
            Some(inner) => inner,
            _ => unreachable!(),
        };
        Poll::Ready((label, result.map(|_| sender).map_err(|_| ())))
    }
}
