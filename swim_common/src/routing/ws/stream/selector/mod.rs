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
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::{Instant, Sleep};

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
pub struct WsStreamSelector<S, M, T, E> {
    ws: S,
    messages: Option<M>,
    pending: Option<(T, Instant)>,
    bias: bool,
    state: State,
    write_timeout: Duration,
    on_write_timeout: Box<dyn Fn(&Duration) -> E + Send>,
}

#[pin_project]
pub struct SelectRw<'a, S, M, T, E> {
    selector: &'a mut WsStreamSelector<S, M, T, E>,
    #[pin]
    timeout_sleep: Option<Sleep>,
}

#[pin_project]
pub struct SelectW<'a, S, M, T, E> {
    selector: &'a mut WsStreamSelector<S, M, T, E>,
    #[pin]
    timeout_sleep: Option<Sleep>,
}

impl<'a, S, M, T, E> Future for SelectRw<'a, S, M, T, E>
where
    M: Stream<Item = T> + Unpin,
    S: Sink<T, Error = E>,
    S: Stream<Item = Result<T, E>> + Unpin,
{
    type Output = Option<Result<SelectorResult<T>, E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let selector: &mut WsStreamSelector<S, M, T, E> = this.selector;
        let mut sleep: Pin<&mut Option<Sleep>> = this.timeout_sleep;

        match &selector.state {
            State::Terminated => Poll::Ready(None),
            State::Terminating => {
                selector.state = State::Terminated;
                Poll::Ready(None)
            }
            State::ClosePending => {
                let result = ready!(selector.ws.poll_close_unpin(cx));
                selector.state = State::Terminated;
                match result {
                    Err(e) => Poll::Ready(Some(Err(e))),
                    _ => Poll::Ready(None),
                }
            }
            _ => {
                if selector.bias {
                    let write_result = match selector.next_to_send(cx) {
                        Poll::Ready(Some((message, ts))) => {
                            selector.try_write(cx, message, ts, &mut sleep)
                        }
                        _ => Poll::Pending,
                    };
                    if write_result.is_pending() {
                        selector.try_read(cx)
                    } else {
                        selector.bias = false;
                        write_result.map(Some)
                    }
                } else {
                    let read_result = selector.try_read(cx);
                    if read_result.is_pending() {
                        match selector.next_to_send(cx) {
                            Poll::Ready(Some((message, ts))) => {
                                selector.try_write(cx, message, ts, &mut sleep).map(Some)
                            }
                            _ => Poll::Pending,
                        }
                    } else {
                        selector.bias = true;
                        read_result
                    }
                }
            }
        }
    }
}

impl<'a, S, M, T, E> Future for SelectW<'a, S, M, T, E>
where
    M: Stream<Item = T> + Unpin,
    S: Sink<T, Error = E>,
    S: Stream<Item = Result<T, E>> + Unpin,
{
    type Output = Option<Result<(), E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let selector: &mut WsStreamSelector<S, M, T, E> = this.selector;
        let mut sleep: Pin<&mut Option<Sleep>> = this.timeout_sleep;

        match &selector.state {
            State::Terminated => Poll::Ready(None),
            State::Terminating => {
                selector.state = State::Terminated;
                Poll::Ready(None)
            }
            State::ClosePending => {
                let result = ready!(selector.ws.poll_close_unpin(cx));
                selector.state = State::Terminated;
                match result {
                    Err(e) => Poll::Ready(Some(Err(e))),
                    _ => Poll::Ready(None),
                }
            }
            _ => {
                let write_result = match selector.next_to_send(cx) {
                    Poll::Ready(Some((message, ts))) => {
                        let result = ready!(selector.try_write(cx, message, ts, &mut sleep));
                        Poll::Ready(Some(result.map(|_| ())))
                    }
                    Poll::Ready(None) => Poll::Ready(None),
                    _ => Poll::Pending,
                };
                if !write_result.is_pending() {
                    selector.bias = false
                }
                write_result
            }
        }
    }
}

impl<S, M, T, E> WsStreamSelector<S, M, T, E>
where
    M: Stream<Item = T> + Unpin,
    S: Sink<T, Error = E>,
    S: Stream<Item = Result<T, E>> + Unpin,
{
    pub fn new<F>(inner: S, message: M, write_timeout: Duration, on_write_timeout: F) -> Self
    where
        F: Fn(&Duration) -> E + Send + 'static,
    {
        WsStreamSelector {
            ws: inner,
            messages: Some(message),
            pending: None,
            bias: false,
            state: State::Active,
            write_timeout,
            on_write_timeout: Box::new(on_write_timeout),
        }
    }

    /// Either read from the connection or write to it, depending on availability.
    pub fn select_rw(&mut self) -> SelectRw<S, M, T, E> {
        let sleep = self
            .pending
            .as_ref()
            .map(|(_, t)| tokio::time::sleep_until(*t));
        SelectRw {
            selector: self,
            timeout_sleep: sleep,
        }
    }

    /// Write the the channel, waiting for outgoing data to become available.
    pub fn select_w(&mut self) -> SelectW<S, M, T, E> {
        let sleep = self
            .pending
            .as_ref()
            .map(|(_, t)| tokio::time::sleep_until(*t));
        SelectW {
            selector: self,
            timeout_sleep: sleep,
        }
    }

    /// True when the connection is closed.
    pub fn is_terminated(&self) -> bool {
        matches!(&self.state, State::Terminated)
    }

    fn try_read(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<SelectorResult<T>, E>>> {
        match self.ws.poll_next_unpin(cx) {
            Poll::Ready(Some(r)) => Poll::Ready(Some(r.map(SelectorResult::Read))),
            Poll::Ready(_) => match self.ws.poll_close_unpin(cx) {
                Poll::Ready(Err(e)) => {
                    self.state = State::Terminating;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => {
                    self.state = State::ClosePending;
                    Poll::Pending
                }
                _ => {
                    self.state = State::Terminated;
                    Poll::Ready(None)
                }
            },
            _ => Poll::Pending,
        }
    }

    fn next_to_send(&mut self, cx: &mut Context<'_>) -> Poll<Option<(T, Option<Instant>)>> {
        match self.pending.take() {
            Some((message, t)) => Poll::Ready(Some((message, Some(t)))),
            _ => {
                if let Some(message_str) = &mut self.messages {
                    if let Some(message) = ready!(message_str.poll_next_unpin(cx)) {
                        Poll::Ready(Some((message, None)))
                    } else {
                        self.messages = None;
                        Poll::Ready(None)
                    }
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }

    fn try_write(
        &mut self,
        cx: &mut Context<'_>,
        message: T,
        ts: Option<Instant>,
        sleep: &mut Pin<&mut Option<Sleep>>,
    ) -> Poll<Result<SelectorResult<T>, E>> {
        match self.ws.poll_ready_unpin(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(
                self.ws
                    .start_send_unpin(message)
                    .map(|_| SelectorResult::Written),
            ),
            Poll::Ready(Err(e)) => {
                if let Some(ts) = ts {
                    self.pending = Some((message, ts));
                } else {
                    let ts = Instant::now()
                        .checked_add(self.write_timeout)
                        .expect("Duration overflow.");
                    self.pending = Some((message, ts));
                    register_sleep(sleep, ts);
                }
                Poll::Ready(Err(e))
            }
            _ => {
                if let Some(ts) = ts {
                    self.pending = Some((message, ts));
                } else {
                    let ts = Instant::now()
                        .checked_add(self.write_timeout)
                        .expect("Duration overflow.");
                    self.pending = Some((message, ts));
                    register_sleep(sleep, ts);
                }
                if let Some(s) = sleep.as_mut().as_pin_mut() {
                    ready!(s.poll(cx));
                    Poll::Ready(Err((self.on_write_timeout)(&self.write_timeout)))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

impl<'a, S, M, T, E> FusedFuture for SelectRw<'a, S, M, T, E>
where
    M: Stream<Item = T> + Unpin,
    S: Sink<T, Error = E>,
    S: Stream<Item = Result<T, E>> + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.selector.is_terminated()
    }
}

impl<'a, S, M, T, E> FusedFuture for SelectW<'a, S, M, T, E>
where
    M: Stream<Item = T> + Unpin,
    S: Sink<T, Error = E>,
    S: Stream<Item = Result<T, E>> + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.selector.is_terminated()
    }
}

fn register_sleep(sleep: &mut Pin<&mut Option<Sleep>>, at: Instant) {
    let delay = tokio::time::sleep_until(at);
    sleep.set(Some(delay));
}
