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

pub mod retryable;

#[cfg(test)]
mod tests;

use futures::never::Never;
use futures::task::{Context, Poll};
use futures::{Future, Sink, Stream, TryFuture};
use pin_project::pin_project;
use std::marker::PhantomData;
use std::pin::Pin;

/// A future that transforms another future using [`Into`].
#[pin_project]
#[derive(Debug)]
pub struct FutureInto<F, T> {
    #[pin]
    future: F,
    _target: PhantomData<T>,
}

/// A future that transforms another future, that produces a [`Result`], using [`Into`].
#[pin_project]
#[derive(Debug)]
pub struct OkInto<F, T> {
    #[pin]
    future: F,
    _target: PhantomData<T>,
}

/// A future that transforms another future using a [`Transform`].
#[pin_project]
#[derive(Debug)]
pub struct TransformedFuture<Fut, Trans> {
    #[pin]
    future: Fut,
    transform: Option<Trans>,
}

/// Trans forms a stream of [`T`] into a stream of [`Result<T, Never>`].
#[pin_project]
#[derive(Debug)]
pub struct NeverErrorStream<Str>(#[pin] Str);

impl<F, T> FutureInto<F, T>
where
    F: Future,
    F::Output: Into<T>,
{
    pub fn new(future: F) -> Self {
        FutureInto {
            future,
            _target: PhantomData,
        }
    }
}

impl<F, T> OkInto<F, T>
where
    F: TryFuture,
    F::Ok: Into<T>,
{
    pub fn new(future: F) -> Self {
        OkInto {
            future,
            _target: PhantomData,
        }
    }
}

impl<Fut, Trans> TransformedFuture<Fut, Trans>
where
    Fut: Future,
    Trans: TransformOnce<Fut::Output>,
{
    pub fn new(future: Fut, transform: Trans) -> Self {
        TransformedFuture {
            future,
            transform: Some(transform),
        }
    }
}

impl<F, T> Future for FutureInto<F, T>
where
    F: Future,
    F::Output: Into<T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().future.poll(cx).map(Into::into)
    }
}

impl<F, T> From<F> for FutureInto<F, T>
where
    F: Future,
    F::Output: Into<T>,
{
    fn from(future: F) -> Self {
        FutureInto::new(future)
    }
}

impl<F, T1, T2, E> Future for OkInto<F, T2>
where
    F: Future<Output = Result<T1, E>>,
    T1: Into<T2>,
{
    type Output = Result<T2, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().future.poll(cx).map(|r| r.map(Into::into))
    }
}

impl<Fut, Trans> Future for TransformedFuture<Fut, Trans>
where
    Fut: Future,
    Trans: TransformOnce<Fut::Output>,
{
    type Output = Trans::Out;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        let fut = projected.future;
        let maybe_trans = projected.transform;
        fut.poll(cx).map(|input| match maybe_trans.take() {
            Some(trans) => trans.transform(input),
            _ => panic!("Transformed future used more than once."),
        })
    }
}

impl<T, Str> Stream for NeverErrorStream<Str>
where
    Str: Stream<Item = T>,
{
    type Item = Result<T, Never>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        projected.0.poll_next(cx).map(|t| t.map(Ok))
    }
}

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

pub trait SwimFutureExt: Future {
    /// Transform the output of a future using [`Into`].
    ///
    ///  # Examples
    /// ```
    /// use futures::executor::block_on;
    /// use futures::future::ready;
    /// use utilities::future::*;
    ///
    /// let n: i64 = block_on(ready(7).output_into());
    /// assert_eq!(n, 7);
    ///
    /// ```
    ///
    fn output_into<T>(self) -> FutureInto<Self, T>
    where
        Self: Sized,
        Self::Output: Into<T>,
    {
        FutureInto::new(self)
    }

    /// Apply a transformation to the output of a future.
    ///
    ///  # Examples
    /// ```
    /// use futures::executor::block_on;
    /// use futures::future::ready;
    /// use utilities::future::*;
    /// use std::ops::Add;
    /// use utilities::future::SwimFutureExt;
    /// struct Plus(i32);
    ///
    /// impl TransformOnce<i32> for Plus {
    ///     type Out = i32;
    ///
    ///     fn transform(self, input: i32) -> Self::Out {
    ///         input + self.0
    ///     }
    /// }
    ///
    /// let n: i32 = block_on(ready(2).transform(Plus(3)));
    /// assert_eq!(n, 5);
    ///
    /// ```
    fn transform<Trans>(self, transform: Trans) -> TransformedFuture<Self, Trans>
    where
        Self: Sized,
        Trans: TransformOnce<Self::Output>,
    {
        TransformedFuture::new(self, transform)
    }
}

impl<F: Future> SwimFutureExt for F {}

pub trait SwimTryFutureExt: TryFuture {
    /// Transform the [`Ok`] case of a fallible future using [`Into`].
    ///
    ///  # Examples
    /// ```
    /// use futures::executor::block_on;
    /// use futures::future::ready;
    /// use utilities::future::*;
    ///
    /// let n: Result<i64, String> = block_on(ready(Ok(7)).output_into());
    /// assert_eq!(n, Ok(7));
    ///
    /// ```
    ///
    fn ok_into<T>(self) -> OkInto<Self, T>
    where
        Self: Sized,
        Self::Ok: Into<T>,
    {
        OkInto::new(self)
    }
}

impl<F: TryFuture> SwimTryFutureExt for F {}

/// A stream that transforms another stream using a [`Transform`].
#[pin_project]
#[derive(Debug)]
pub struct TransformedStream<Str, Trans> {
    #[pin]
    stream: Str,
    transform: Trans,
}

impl<Str, Trans> TransformedStream<Str, Trans>
where
    Str: Stream,
    Trans: TransformMut<Str::Item>,
{
    pub fn new(stream: Str, transform: Trans) -> Self {
        TransformedStream { stream, transform }
    }
}

impl<Str, Trans> Stream for TransformedStream<Str, Trans>
where
    Str: Stream,
    Trans: TransformMut<Str::Item>,
{
    type Item = Trans::Out;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        let stream = projected.stream;
        let trans = projected.transform;
        stream
            .poll_next(cx)
            .map(|r| r.map(|item| trans.transform(item)))
    }
}

/// A stream that runs another stream of [`Result`]s until an error is produces, yielding the
/// OK values.
#[pin_project]
#[derive(Debug)]
pub struct UntilFailure<Str, Trans> {
    #[pin]
    stream: Str,
    transform: Trans,
}

/// Stream for the `take_until_completes` combinator.
#[pin_project]
#[derive(Debug)]
pub struct TakeUntil<S, F> {
    #[pin]
    stream: S,
    #[pin]
    fut: F,
}

impl<S, F> TakeUntil<S, F> {
    pub fn new(stream: S, limit: F) -> Self {
        TakeUntil { stream, fut: limit }
    }
}

impl<Str, Trans> UntilFailure<Str, Trans>
where
    Str: Stream,
    Trans: TransformMut<Str::Item>,
{
    pub fn new(stream: Str, transform: Trans) -> Self {
        UntilFailure { stream, transform }
    }
}

impl<Str, Trans, T, E> Stream for UntilFailure<Str, Trans>
where
    Str: Stream,
    Trans: TransformMut<Str::Item, Out = Result<T, E>>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        let stream = projected.stream;
        let trans = projected.transform;
        stream
            .poll_next(cx)
            .map(|r| r.and_then(|item| trans.transform(item).ok()))
    }
}

impl<S: Stream, F: Future> Stream for TakeUntil<S, F> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();

        if let Poll::Ready(_) = projected.fut.poll(cx) {
            Poll::Ready(None)
        } else {
            projected.stream.poll_next(cx)
        }
    }
}

pub trait SwimStreamExt: Stream {
    /// Apply a transformation to the items of a stream.
    ///
    ///  # Examples
    /// ```
    /// use futures::executor::block_on;
    /// use futures::StreamExt;
    /// use futures::stream::iter;
    /// use utilities::future::*;
    /// use std::ops::Add;
    /// use utilities::future::{SwimFutureExt, SwimStreamExt};
    /// struct Plus(i32);
    ///
    /// impl TransformMut<i32> for Plus {
    ///     type Out = i32;
    ///
    ///     fn transform(&mut self, input: i32) -> Self::Out {
    ///         input + self.0
    ///     }
    /// }
    ///
    /// let inputs = iter((0..5).into_iter());
    ///
    /// let outputs: Vec<i32> = block_on(inputs.transform(Plus(3)).collect::<Vec<i32>>());
    /// assert_eq!(outputs, vec![3, 4, 5, 6, 7]);
    /// ```
    fn transform<Trans>(self, transform: Trans) -> TransformedStream<Self, Trans>
    where
        Self: Sized,
        Trans: TransformMut<Self::Item>,
    {
        TransformedStream::new(self, transform)
    }

    /// Transform the items of a stream until an error is encountered, then terminate.
    ///  # Examples
    /// ```
    /// use futures::executor::block_on;
    /// use futures::StreamExt;
    /// use futures::stream::iter;
    /// use utilities::future::*;
    /// use std::ops::Add;
    /// use utilities::future::{SwimFutureExt, SwimStreamExt};
    /// struct PlusIfNonNeg(i32);
    ///
    /// impl TransformMut<i32> for PlusIfNonNeg {
    ///     type Out = Result<i32, ()>;
    ///
    ///     fn transform(&mut self, input: i32) -> Self::Out {
    ///         if input >= 0 {
    ///             Ok(input + self.0)
    ///         } else {
    ///             Err(())
    ///         }
    ///     }
    /// }
    ///
    /// let inputs = iter(vec![0, 1, 2, -3, 4].into_iter());
    /// let outputs: Vec<i32> = block_on(inputs.until_failure(PlusIfNonNeg(3)).collect::<Vec<i32>>());
    /// assert_eq!(outputs, vec![3, 4, 5]);
    /// ```
    fn until_failure<Trans, T, E>(self, transform: Trans) -> UntilFailure<Self, Trans>
    where
        Self: Sized,
        Trans: TransformMut<Self::Item, Out = Result<T, E>>,
    {
        UntilFailure::new(self, transform)
    }

    /// Transform this stream into an infallible [`TryStream`].
    ///
    fn never_error(self) -> NeverErrorStream<Self>
    where
        Self: Sized,
    {
        NeverErrorStream(self)
    }

    /// Creates a new stream that will produce the values that this stream would produce until
    /// a future completes.
    ///
    /// #Examples
    /// ```
    /// use futures::executor::block_on;
    /// use futures::stream::unfold;
    /// use futures::future::ready;
    /// use futures::StreamExt;
    /// use utilities::sync::trigger::trigger;
    /// use utilities::future::SwimStreamExt;
    ///
    /// let (stop, stop_sig) = trigger();
    /// let mut maybe_stop = Some(stop);
    /// let stream = unfold(0i32, |i| ready(Some((i, i + 1))))
    ///     .then(|i| {
    ///         if i == 5 {
    ///             if let Some(stop) = maybe_stop.take() {
    ///                 stop.trigger();
    ///             }
    ///         }
    ///         ready(i)
    ///    }).take_until_completes(stop_sig);
    ///
    /// assert_eq!(block_on(stream.collect::<Vec<_>>()), vec![0, 1, 2, 3, 4, 5]);
    ///
    /// ```
    fn take_until_completes<Fut>(self, limit: Fut) -> TakeUntil<Self, Fut>
    where
        Self: Sized,
        Fut: Future,
    {
        TakeUntil::new(self, limit)
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

impl<S> SwimStreamExt for S where S: Stream {}
