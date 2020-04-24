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

use futures::task::{Context, Poll};
use futures::{Future, Stream, TryFuture};
use pin_project::pin_project;
use std::marker::PhantomData;
use std::pin::Pin;

#[pin_project]
#[derive(Debug)]
pub struct FutureInto<F, T> {
    #[pin]
    future: F,
    _target: PhantomData<T>,
}

#[pin_project]
#[derive(Debug)]
pub struct OkInto<F, T> {
    #[pin]
    future: F,
    _target: PhantomData<T>,
}

#[pin_project]
#[derive(Debug)]
pub struct TransformedFuture<Fut, Trans> {
    #[pin]
    future: Fut,
    transform: Option<Trans>,
}

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

    /// Trans from the input, potentially using the contents of this transformer.
    fn transform(&self, input: In) -> Self::Out;
}

/// A trait that is essentially equivalent to [`FnMut`] with a single variable. However, it is
/// possible to implement this directly for a named type.
pub trait TransformMut<In> {
    type Out;

    /// Trans from the input, potentially using the contents of this transformer.
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

#[pin_project]
#[derive(Debug)]
pub struct UntilFailure<Str, Trans> {
    #[pin]
    stream: Str,
    transform: Trans,
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
}

impl<S> SwimStreamExt for S where S: Stream {}
