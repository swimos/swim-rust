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
use tokio::time::Instant;

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
    pending: Option<(T, Instant)>,
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
                if *bias {
                    let write_result = match next_to_send(cx, pending, messages) {
                        Poll::Ready(Some((message, ts))) => {
                            try_write(ws, cx, pending, message, ts)
                        }
                        _ => Poll::Pending
                    };
                    if write_result.is_pending() {
                        try_read(ws, cx, state)
                    } else {
                        *bias = false;
                        write_result.map(Some)
                    }
                } else {
                    let read_result = try_read(ws, cx, state);
                    if read_result.is_pending() {
                        match next_to_send(cx, pending, messages) {
                            Poll::Ready(Some((message, ts))) => {
                                try_write(ws, cx, pending, message, ts).map(Some)
                            }
                            _ => Poll::Pending
                        }
                    } else {
                        *bias = true;
                        read_result
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
    type Output = Option<Result<(), <S as Sink<T>>::Error>>;

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
                let write_result = match next_to_send(cx, pending, messages) {
                    Poll::Ready(Some((message, ts))) => {
                        let result = ready!(try_write(ws, cx, pending, message, ts));
                        Poll::Ready(Some(result.map(|_| ())))
                    }
                    Poll::Ready(None) => Poll::Ready(None),
                    _ => Poll::Pending
                };
                if !write_result.is_pending() {
                    *bias = false
                }
                write_result
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

    /// Either read from the connection or write to it, depending on availability.
    pub fn select_rw(&mut self) -> SelectRw<S, M, T> {
        SelectRw(self)
    }

    /// Write the the channel, waiting for outgoing data to become available.
    pub fn select_w(&mut self) -> SelectW<S, M, T> {
        SelectW(self)
    }

    /// True when the connection is closed.
    pub fn is_terminated(&self) -> bool {
        matches!(&self.state, State::Terminated)
    }
}

pub type SinkError<S, T> = <S as Sink<T>>::Error;

type SelectResult<S, T> = Result<SelectorResult<T>, SinkError<S, T>>;

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

fn next_to_send<T, M>(cx: &mut Context<'_>,
                       pending: &mut Option<(T, Instant)>,
                      messages: &mut Option<M>) -> Poll<Option<(T, Option<Instant>)>>
where
    M: Stream<Item = T> + Unpin,
{
    match pending.take() {
        Some((message, t)) => Poll::Ready(Some((message, Some(t)))),
        _ => {
            if let Some(message_str) = messages {
                if let Some(message) = ready!(message_str.poll_next_unpin(cx)) {
                    Poll::Ready(Some((message, None)))

                } else {
                    *messages = None;
                    Poll::Ready(None)
                }
            } else {
                Poll::Ready(None)
            }
        }
    }
}

fn try_write<S, T>(ws: &mut S,
                   cx: &mut Context<'_>,
                   pending: &mut Option<(T, Instant)>,
                   message: T,
                   ts: Option<Instant>) -> Poll<SelectResult<S, T>>
where
    S: Sink<T> + Unpin,
{
    match ws.poll_ready_unpin(cx) {
        Poll::Ready(Ok(_)) => {
            Poll::Ready(ws.start_send_unpin(message).map(|_| SelectorResult::Written))
        },
        Poll::Ready(Err(e)) => {
            *pending = Some((message, ts.unwrap_or_else(|| Instant::now())));
            Poll::Ready(Err(e))
        }
        _ => {
            *pending = Some((message, ts.unwrap_or_else(|| Instant::now())));
            Poll::Pending
        }
    }
}
