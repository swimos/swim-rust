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
use futures::{Future, TryFuture};
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
    Trans: Transformation<Fut::Output>,
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
    Trans: Transformation<Fut::Output>,
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
pub trait Transformation<In> {
    type Out;

    /// Trans from the input, potentially using the contents of this transformer.
    fn transform(self, input: In) -> Self::Out;
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
    /// impl Transformation<i32> for Plus {
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
        Trans: Transformation<Self::Output>,
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
