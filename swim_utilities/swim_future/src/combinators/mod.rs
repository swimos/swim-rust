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

mod immediate_or;
#[cfg(test)]
mod tests;

use futures::task::{Context, Poll};
use futures::{ready, Future, Sink, Stream, TryStream};
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Notify;

pub use immediate_or::{
    immediate_or_join, immediate_or_start, ImmediateOrJoin, ImmediateOrStart, SecondaryResult,
};
/// A trait that is essentially equivalent to [`FnOnce`] with a single variable. However, it is
/// possible to implement this directly for a named type.
pub trait TransformOnce<In> {
    type Out;

    /// Trans from the input, potentially using the contents of this transformer.
    fn transform(self, input: In) -> Self::Out;
}

/// A trait that is essentially equivalent to [`Fn`] with a single variable. However, it is
/// possible to implement this directly for a named type.
pub trait Transform<In> {
    type Out;

    /// Transform the input.
    fn transform(&self, input: In) -> Self::Out;
}

/// A trait that is essentially equivalent to [`FnMut`] with a single variable. However, it is
/// possible to implement this directly for a named type.
pub trait TransformMut<In> {
    type Out;

    /// Transform the input.
    fn transform(&mut self, input: In) -> Self::Out;
}

impl<In, F> TransformMut<In> for F
where
    F: Transform<In>,
{
    type Out = F::Out;

    fn transform(&mut self, input: In) -> Self::Out {
        Transform::transform(self, input)
    }
}

impl<In, F> TransformOnce<In> for F
where
    F: TransformMut<In>,
{
    type Out = F::Out;

    fn transform(mut self, input: In) -> Self::Out {
        TransformMut::transform(&mut self, input)
    }
}

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
#[derive(Debug)]
pub struct TransformedSink<S, Trans> {
    #[pin]
    inner: S,
    transformer: Trans,
}

impl<S, Trans> TransformedSink<S, Trans> {
    pub fn new(sink: S, transformer: Trans) -> TransformedSink<S, Trans> {
        TransformedSink {
            inner: sink,
            transformer,
        }
    }
}

impl<S, Trans, Item> Sink<Item> for TransformedSink<S, Trans>
where
    Trans: TransformMut<Item>,
    S: Sink<Trans::Out>,
{
    type Error = S::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let this = self.project();
        let transformed = this.transformer.transform(item);

        this.inner.start_send(transformed)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

pub trait SwimSinkExt<Item>: Sink<Item> {
    /// Applys a transformation to each element that is sent to the sink.
    fn transform<Trans, I>(self, transformer: Trans) -> TransformedSink<Self, Trans>
    where
        Self: Sized,
        Trans: TransformMut<I>,
    {
        TransformedSink::new(self, transformer)
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
