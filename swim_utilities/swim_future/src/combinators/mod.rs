// Copyright 2015-2023 Swim Inc.
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

mod immediate_or;
mod race;
#[cfg(test)]
mod tests;

use futures::task::{Context, Poll};
use futures::{pin_mut, ready, Future, Stream, StreamExt, TryStream};
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Notify;

pub use immediate_or::{
    immediate_or_join, immediate_or_start, ImmediateOrJoin, ImmediateOrStart, SecondaryResult,
};

pub use race::{race, race3, Either3, Race2, Race3};

/// A stream that runs another stream of [`Result`]s until it produces an error and then
/// terminates.
#[pin_project]
#[derive(Debug)]
pub struct StopAfterError<Str> {
    #[pin]
    stream: Str,
    terminated: bool,
}

impl<Str> StopAfterError<Str> {
    pub fn new(stream: Str) -> Self {
        StopAfterError {
            stream,
            terminated: false,
        }
    }
}

impl<Str: TryStream> Stream for StopAfterError<Str> {
    type Item = Result<Str::Ok, Str::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        if *projected.terminated {
            Poll::Ready(None)
        } else {
            let result = ready!(projected.stream.try_poll_next(cx));
            if matches!(result, Some(Err(_))) {
                *projected.terminated = true;
            }
            Poll::Ready(result)
        }
    }
}

#[pin_project]
pub struct NotifyOnBlocked<F> {
    #[pin]
    inner: F,
    notify: Arc<Notify>,
}

impl<F> NotifyOnBlocked<F> {
    pub fn new(inner: F, notify: Arc<Notify>) -> NotifyOnBlocked<F> {
        NotifyOnBlocked { inner, notify }
    }
}

impl<F: Future> Future for NotifyOnBlocked<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        let result = projected.inner.poll(cx);
        if result.is_pending() {
            projected.notify.notify_one();
        }
        result
    }
}

impl<S: Stream> Stream for NotifyOnBlocked<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        let result = projected.inner.poll_next(cx);
        if result.is_pending() {
            projected.notify.notify_one();
        }
        result
    }
}

/// Get the last value from a stream of results, terminating early if any member of the stream
/// is an error.
pub async fn try_last<S, T, E>(stream: S) -> Result<Option<T>, E>
where
    S: Stream<Item = Result<T, E>>,
{
    pin_mut!(stream);
    let mut last = None;
    while let Some(result) = stream.next().await {
        last = Some(result?);
    }
    Ok(last)
}
