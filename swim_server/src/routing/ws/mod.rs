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

use crate::routing::error::ConnectionError;
use futures::future::ErrInto;
use futures::stream::FusedStream;
use futures::task::{Context, Poll};
use futures::{ready, Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use swim_common::ws::WsMessage;

#[cfg(test)]
mod tests;

#[derive(Debug)]
enum State {
    Active,
    ClosePending,
    Terminating,
    Terminated,
}

/// The item type for [`WsStreamSelector`].
#[derive(Debug, PartialEq, Eq)]
pub enum SelectorResult<T> {
    /// A value was read from the stream.
    Read(T),
    /// A value was written to the sink.
    Written,
}

/// An alternative to using a lock for splitting the read and write halves of an IO channel
/// that cannot be split into two independently owned halves. Values are written into the
/// sink from a provided stream and the selector itself is a stream of values read from
/// the underlying stream and notifications of successful writes. The selector waits on both
/// the read and write halves of the underlying channel becoming available. In the situation where
/// both are available it will alternate between the two to prevent starvation.
#[pin_project]
pub struct WsStreamSelector<S, M, T> {
    ws: S,
    #[pin]
    messages: Option<M>,
    pending: Option<T>,
    bias: bool,
    state: State,
}

impl<S, M, T> WsStreamSelector<S, M, T>
where
    M: Stream<Item = T>,
    S: Sink<T>,
    S: Stream<Item = Result<T, SinkError<S, T>>> + Unpin,
{
    pub fn new(inner: S, message: M) -> Self {
        WsStreamSelector {
            ws: inner,
            messages: Some(message),
            pending: None,
            bias: false,
            state: State::Active,
        }
    }
}

pub type SinkError<S, T> = <S as Sink<T>>::Error;

impl<S, M, T> Stream for WsStreamSelector<S, M, T>
where
    M: Stream<Item = T>,
    S: Sink<T>,
    S: Stream<Item = Result<T, SinkError<S, T>>> + Unpin,
{
    type Item = Result<SelectorResult<T>, <S as Sink<T>>::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projected = self.project();
        match projected.state {
            State::Terminated => Poll::Ready(None),
            State::Terminating => {
                *projected.state = State::Terminated;
                Poll::Ready(None)
            }
            State::ClosePending => {
                let result = ready!(projected.ws.poll_close_unpin(cx));
                *projected.state = State::Terminated;
                match result {
                    Err(e) => Poll::Ready(Some(Err(e))),
                    _ => Poll::Ready(None),
                }
            }
            _ => {
                if projected.pending.is_some() {
                    if *projected.bias {
                        let send_result = try_send(projected.ws, cx, projected.pending);
                        if send_result.is_pending() {
                            try_read(projected.ws, cx, projected.state)
                        } else {
                            *projected.bias = false;
                            send_result
                        }
                    } else {
                        let read_result = try_read(projected.ws, cx, projected.state);
                        if read_result.is_pending() {
                            try_send(projected.ws, cx, projected.pending)
                        } else {
                            *projected.bias = true;
                            read_result
                        }
                    }
                } else {
                    match projected.messages.as_mut().as_pin_mut() {
                        Some(messages) => {
                            if *projected.bias {
                                let next = messages.poll_next(cx);
                                match next {
                                    Poll::Ready(Some(v)) => {
                                        *projected.pending = Some(v);
                                        let send_result =
                                            try_send(projected.ws, cx, projected.pending);
                                        if send_result.is_pending() {
                                            try_read(projected.ws, cx, projected.state)
                                        } else {
                                            *projected.bias = false;
                                            send_result
                                        }
                                    }
                                    Poll::Ready(None) => {
                                        projected.messages.set(None);
                                        try_read(projected.ws, cx, projected.state)
                                    }
                                    Poll::Pending => try_read(projected.ws, cx, projected.state),
                                }
                            } else {
                                let read_result = try_read(projected.ws, cx, projected.state);
                                if read_result.is_pending() {
                                    if let Some(next) = ready!(messages.poll_next(cx)) {
                                        *projected.pending = Some(next);
                                        try_send(projected.ws, cx, projected.pending)
                                    } else {
                                        projected.messages.set(None);
                                        read_result
                                    }
                                } else {
                                    *projected.bias = true;
                                    read_result
                                }
                            }
                        }
                        _ => try_read(projected.ws, cx, projected.state),
                    }
                }
            }
        }
    }
}

type SelectResult<S, T> = Result<SelectorResult<T>, SinkError<S, T>>;

fn try_send<S, T>(
    ws: &mut S,
    cx: &mut Context<'_>,
    pending: &mut Option<T>,
) -> Poll<Option<SelectResult<S, T>>>
where
    S: Sink<T>,
    S: Stream<Item = Result<T, <S as Sink<T>>::Error>> + Unpin,
{
    let result = ready!(ws.poll_ready_unpin(cx));
    if let Err(e) = result {
        Poll::Ready(Some(Err(e)))
    } else {
        let message = pending.take().unwrap();
        if let Err(e) = ws.start_send_unpin(message) {
            Poll::Ready(Some(Err(e)))
        } else {
            Poll::Ready(Some(Ok(SelectorResult::Written)))
        }
    }
}

impl<S, M, T> FusedStream for WsStreamSelector<S, M, T>
where
    M: Stream<Item = T>,
    S: Sink<T>,
    S: Stream<Item = Result<T, SinkError<S, T>>> + Unpin,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, State::Terminated)
    }
}

fn try_read<S, T>(
    ws: &mut S,
    cx: &mut Context<'_>,
    state: &mut State,
) -> Poll<Option<SelectResult<S, T>>>
where
    S: Sink<T>,
    S: Stream<Item = Result<T, <S as Sink<T>>::Error>> + Unpin,
{
    match ws.poll_next_unpin(cx) {
        Poll::Ready(Some(r)) => Poll::Ready(Some(r.map(SelectorResult::Read))),
        Poll::Ready(_) => match ws.poll_close_unpin(cx) {
            Poll::Ready(Err(e)) => {
                *state = State::Terminating;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Pending => {
                *state = State::ClosePending;
                Poll::Pending
            }
            _ => {
                *state = State::Terminated;
                Poll::Ready(None)
            }
        },
        _ => Poll::Pending,
    }
}

pub enum CloseReason {
    GoingAway,
    ProtocolError(String),
    //TODO Fill in others.
}

pub trait JoinedStreamSink<T, E>: Stream<Item = Result<T, E>> + Sink<T, Error = E> {
    type CloseFut: Future<Output = Result<(), E>> + Send + 'static;

    fn close(&mut self, reason: Option<CloseReason>) -> Self::CloseFut;

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

#[pin_project]
pub struct TransformedStreamSink<S, T1, T2, E1, E2> {
    #[pin]
    str_sink: S,
    _bijection: PhantomData<Bijection<T1, T2>>,
    _errors: PhantomData<fn(E1) -> E2>,
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

    fn close(&mut self, reason: Option<CloseReason>) -> Self::CloseFut {
        self.str_sink.close(reason).err_into()
    }
}

pub trait WsConnections<Sock: Send + Sync + Unpin> {
    type StreamSink: JoinedStreamSink<WsMessage, ConnectionError> + Send + Unpin + 'static;
    type Fut: Future<Output = Result<Self::StreamSink, ConnectionError>> + Send + 'static;

    fn open_connection(&self, socket: Sock) -> Self::Fut;
    fn accept_connection(&self, socket: Sock) -> Self::Fut;
}
