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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::{maybe_done, MaybeDone};
use futures::{Sink, SinkExt};

use pin_project::pin_project;

#[cfg(test)]
mod tests;

enum State {
    Init,
    BothPending,
    FutPending,
    FlushPending,
    Done,
}

/// Future type for the [`or_flush`] function.
#[pin_project]
pub struct OrFlush<'a, Item, F: Future, Snk: Sink<Item>> {
    #[pin]
    future: MaybeDone<F>,
    sink: &'a mut Snk,
    state: State,
    flush_result: Option<Result<(), Snk::Error>>,
}

/// Create a future that will poll another future and, if it does not complete immediately, will
/// attempt to flush the provided sink. If flushing of the sink starts, the future will not complete
/// until both subsidiary actions have completed. If the future completes immediate, the sink will not
/// be affected.
///
/// The intended use of this is for use in a loop where a future is expected to return work that will
/// result in output being written to the sink. If there is no more work immediately available, it is
/// then desirable for the sink to be flushed.
///
/// # Arguments
/// * `f` - The future to poll.
/// * `sink` - The sink to be (potentially) flushed.
pub fn or_flush<'a, Item, F, Snk>(f: F, sink: &'a mut Snk) -> OrFlush<'a, Item, F, Snk>
where
    F: Future + 'a,
    Snk: Sink<Item> + Unpin,
{
    OrFlush::new(f, sink)
}

impl<'a, Item, F: Future, Snk: Sink<Item>> OrFlush<'a, Item, F, Snk> {
    pub fn new(f: F, sink: &'a mut Snk) -> Self {
        OrFlush {
            future: maybe_done(f),
            sink,
            state: State::Init,
            flush_result: None,
        }
    }
}

impl<'a, Item, F, Snk> Future for OrFlush<'a, Item, F, Snk>
where
    F: Future + 'a,
    Snk: Sink<Item> + Unpin,
{
    type Output = (F::Output, Option<Result<(), Snk::Error>>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.state {
            State::Init => {
                if this.future.as_mut().poll(cx).is_ready() {
                    let result = this.future.take_output().unwrap();
                    *this.state = State::Done;
                    Poll::Ready((result, None))
                } else {
                    if let Poll::Ready(fr) = this.sink.poll_flush_unpin(cx) {
                        *this.state = State::FutPending;
                        *this.flush_result = Some(fr);
                    } else {
                        *this.state = State::BothPending;
                    }
                    Poll::Pending
                }
            }
            State::BothPending => {
                let fut_poll = this.future.as_mut().poll(cx);
                let flush_poll = this.sink.poll_flush_unpin(cx);
                match (fut_poll, flush_poll) {
                    (Poll::Ready(_), Poll::Ready(fr)) => {
                        let result = this.future.take_output().unwrap();
                        *this.state = State::Done;
                        Poll::Ready((result, Some(fr)))
                    }
                    (Poll::Ready(_), _) => {
                        *this.state = State::FlushPending;
                        Poll::Pending
                    }
                    (_, Poll::Ready(fr)) => {
                        *this.state = State::FutPending;
                        *this.flush_result = Some(fr);
                        Poll::Pending
                    }
                    _ => Poll::Pending,
                }
            }
            State::FutPending => {
                if this.future.as_mut().poll(cx).is_ready() {
                    let result = this.future.take_output().unwrap();
                    *this.state = State::Done;
                    Poll::Ready((result, this.flush_result.take()))
                } else {
                    Poll::Pending
                }
            }
            State::FlushPending => {
                if let Poll::Ready(fr) = this.sink.poll_flush_unpin(cx) {
                    let result = this.future.take_output().unwrap();
                    *this.state = State::Done;
                    Poll::Ready((result, Some(fr)))
                } else {
                    Poll::Pending
                }
            }
            State::Done => {
                panic!("OrFlush polled after complete.")
            }
        }
    }
}
