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

use futures::stream::FusedStream;
use futures::task::{Context, Poll};
use futures::{ready, Sink, SinkExt, Stream, StreamExt};
use pin_project::pin_project;
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
