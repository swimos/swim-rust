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

#[cfg(test)]
mod tests;

use futures::future::FusedFuture;
use futures::task::{Context, Poll};
use futures::{ready, Sink, SinkExt, Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;

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
pub struct WsStreamSelector<S, M, T> {
    ws: S,
    messages: Option<M>,
    pending: Option<T>,
    bias: bool,
    state: State,
}

pub struct SelectRw<'a, S, M, T>(&'a mut WsStreamSelector<S, M, T>);
pub struct SelectW<'a, S, M, T>(&'a mut WsStreamSelector<S, M, T>);


impl<'a, S, M, T> Future for SelectRw<'a, S, M, T>
where
    M: Stream<Item = T> + Unpin,
    S: Sink<T>,
    S: Stream<Item = Result<T, SinkError<S, T>>> + Unpin,
{
    type Output = Option<Result<SelectorResult<T>, <S as Sink<T>>::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let WsStreamSelector {
            ws,
            messages,
            pending,
            bias,
            state,
        } = self.get_mut().0;
        match state {
            State::Terminated => Poll::Ready(None),
            State::Terminating => {
                *state = State::Terminated;
                Poll::Ready(None)
            }
            State::ClosePending => {
                let result = ready!(ws.poll_close_unpin(cx));
                *state = State::Terminated;
                match result {
                    Err(e) => Poll::Ready(Some(Err(e))),
                    _ => Poll::Ready(None),
                }
            }
            _ => {
                if pending.is_some() {
                    if *bias {
                        let send_result = try_send(ws, cx, pending);
                        if send_result.is_pending() {
                            try_read(ws, cx, state)
                        } else {
                            *bias = false;
                            send_result
                        }
                    } else {
                        let read_result = try_read(ws, cx, state);
                        if read_result.is_pending() {
                            try_send(ws, cx, pending)
                        } else {
                            *bias = true;
                            read_result
                        }
                    }
                } else {
                    match messages {
                        Some(message_str) => {
                            if *bias {
                                let next = message_str.poll_next_unpin(cx);
                                match next {
                                    Poll::Ready(Some(v)) => {
                                        *pending = Some(v);
                                        let send_result = try_send(ws, cx, pending);
                                        if send_result.is_pending() {
                                            try_read(ws, cx, state)
                                        } else {
                                            *bias = false;
                                            send_result
                                        }
                                    }
                                    Poll::Ready(None) => {
                                        *messages = None;
                                        try_read(ws, cx, state)
                                    }
                                    Poll::Pending => try_read(ws, cx, state),
                                }
                            } else {
                                let read_result = try_read(ws, cx, state);
                                if read_result.is_pending() {
                                    if let Some(next) = ready!(message_str.poll_next_unpin(cx)) {
                                        *pending = Some(next);
                                        try_send(ws, cx, pending)
                                    } else {
                                        *messages = None;
                                        read_result
                                    }
                                } else {
                                    *bias = true;
                                    read_result
                                }
                            }
                        }
                        _ => try_read(ws, cx, state),
                    }
                }
            }
        }
    }
}

impl<'a, S, M, T> Future for SelectW<'a, S, M, T>
    where
        M: Stream<Item = T> + Unpin,
        S: Sink<T>,
        S: Stream<Item = Result<T, SinkError<S, T>>> + Unpin,
{
    type Output = Option<Result<bool, <S as Sink<T>>::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let WsStreamSelector {
            ws,
            messages,
            pending,
            bias,
            state,
        } = self.get_mut().0;
        match state {
            State::Terminated => Poll::Ready(None),
            State::Terminating => {
                *state = State::Terminated;
                Poll::Ready(None)
            }
            State::ClosePending => {
                let result = ready!(ws.poll_close_unpin(cx));
                *state = State::Terminated;
                match result {
                    Err(e) => Poll::Ready(Some(Err(e))),
                    _ => Poll::Ready(None),
                }
            }
            _ => {
                if pending.is_some() {
                    let result = ready!(try_send(ws, cx, pending));
                    *bias = false;
                    Poll::Ready(result.map(|r| r.map(|_| true)))
                } else {
                    match messages {
                        Some(message_str) => {
                            let next = ready!(message_str.poll_next_unpin(cx));
                            match next {
                                Some(v) => {
                                    *pending = Some(v);
                                    let result = ready!(try_send(ws, cx, pending));
                                    *bias = false;
                                    Poll::Ready(result.map(|r| r.map(|_| true)))
                                }
                                _ => {
                                    *messages = None;
                                    *bias = false;
                                    Poll::Ready(Some(Ok(false)))
                                }
                            }
                        }
                        _ => {
                            *bias = false;
                            Poll::Ready(Some(Ok(false)))
                        },
                    }
                }
            }
        }
    }
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

    pub fn select_rw(&mut self) -> SelectRw<S, M, T> {
        SelectRw(self)
    }

    pub fn select_w(&mut self) -> SelectW<S, M, T> {
        SelectW(self)
    }

    pub fn is_terminated(&self) -> bool {
        matches!(&self.state, State::Terminated)
    }
}

pub type SinkError<S, T> = <S as Sink<T>>::Error;

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

impl<'a, S, M, T> FusedFuture for SelectRw<'a, S, M, T>
where
    M: Stream<Item = T> + Unpin,
    S: Sink<T>,
    S: Stream<Item = Result<T, SinkError<S, T>>> + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }
}

impl<'a, S, M, T> FusedFuture for SelectW<'a, S, M, T>
    where
        M: Stream<Item = T> + Unpin,
        S: Sink<T>,
        S: Stream<Item = Result<T, SinkError<S, T>>> + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
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
