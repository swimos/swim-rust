// Copyright 2015-2021 SWIM.AI inc.
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

use crate::routing::ws::protocol::CloseReason;
use futures::future::ErrInto;
use futures::task::{Context, Poll};
use futures::{ready, Sink, Stream, TryFutureExt};
use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

pub mod selector;

/// Trait for two way channels where the send and receive halves cannot be separated.
///
/// #Type Parameters
///
/// * `T` - The type of the data both send and received over the channel.
/// * `E` - Error type for both sending and receiving.
pub trait JoinedStreamSink<T, E>: Stream<Item = Result<T, E>> + Sink<T, Error = E> {
    type CloseFut: Future<Output = Result<(), E>> + Send + 'static;

    fn close(self, reason: Option<CloseReason>) -> Self::CloseFut;

    /// Transform both the data and error types using [`Into`].
    fn transform_data<T2, E2>(self) -> TransformedStreamSink<Self, T, T2, E, E2>
    where
        Self: Sized,
        T2: From<T>,
        T: From<T2>,
        E2: From<E>,
    {
        TransformedStreamSink {
            str_sink: self,
            _bijection: PhantomData,
            _errors: PhantomData,
        }
    }
}

type Bijection<T1, T2> = (fn(T1) -> T2, fn(T2) -> T1);

/// A [`JoinedStreamSink`] where the data and error type have been transformed by [`Into`].
#[pin_project]
pub struct TransformedStreamSink<S, T1, T2, E1, E2> {
    #[pin]
    str_sink: S,
    _bijection: PhantomData<Bijection<T1, T2>>,
    _errors: PhantomData<fn(E1) -> E2>,
}

impl<S, T1, T2, E1, E2> TransformedStreamSink<S, T1, T2, E1, E2> {
    pub fn new(str_sink: S) -> Self {
        TransformedStreamSink {
            str_sink,
            _bijection: Default::default(),
            _errors: Default::default(),
        }
    }
}

impl<T1, T2, E1, E2, S> Stream for TransformedStreamSink<S, T1, T2, E1, E2>
where
    S: Stream<Item = Result<T1, E1>>,
    T2: From<T1>,
    E2: From<E1>,
{
    type Item = Result<T2, E2>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result: Option<Result<T1, E1>> = ready!(self.project().str_sink.poll_next(cx));
        Poll::Ready(result.map(|r| r.map(From::from).map_err(From::from)))
    }
}

impl<T1, T2, S, E1, E2> Sink<T2> for TransformedStreamSink<S, T1, T2, E1, E2>
where
    S: Sink<T1, Error = E1>,
    T1: From<T2>,
    E2: From<E1>,
{
    type Error = E2;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .str_sink
            .poll_ready(cx)
            .map(|r| r.map_err(From::from))
    }

    fn start_send(self: Pin<&mut Self>, item: T2) -> Result<(), Self::Error> {
        self.project()
            .str_sink
            .start_send(item.into())
            .map_err(From::from)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .str_sink
            .poll_flush(cx)
            .map(|r| r.map_err(From::from))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .str_sink
            .poll_close(cx)
            .map(|r| r.map_err(From::from))
    }
}

impl<T1, T2, E1, E2, S> JoinedStreamSink<T2, E2> for TransformedStreamSink<S, T1, T2, E1, E2>
where
    S: JoinedStreamSink<T1, E1>,
    T2: From<T1>,
    T1: From<T2>,
    E2: From<E1> + Send + Sync + 'static,
{
    type CloseFut = ErrInto<S::CloseFut, E2>;

    fn close(self, reason: Option<CloseReason>) -> Self::CloseFut {
        self.str_sink.close(reason).err_into()
    }
}
