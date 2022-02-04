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

use pin_project::pin_project;

#[cfg(test)]
mod tests;

enum State {
    Init,
    BothPending,
    FirstPending,
    SecondPending,
    Done,
}

/// Future type for the [`immediate_or_join`] function.
#[pin_project]
pub struct ImmediateOrJoin<F: Future, G: Future> {
    #[pin]
    first: MaybeDone<F>,
    #[pin]
    second: MaybeDone<G>,
    state: State,
}

/// Create a future that will poll a future and, if it does not complete immediately, will
/// attempt to poll a second future. If the second future starts, the future will not complete
/// until both subsidiary actions have completed. If the first future completes immediately,
/// the secon future will never be polled.
///
/// The intended use of this is for use in a loop where a future is expected to return work, the outcome
/// of which may need comitting. If no more work is available, it is necessary to process the commit
/// action (the second future). For example, the second future could be to flush a sink.
///
/// # Arguments
/// * `f` - The first future to poll.
/// * `g` - The second future to poll.
pub fn immediate_or_join<F, G>(f: F, g: G) -> ImmediateOrJoin<F, G>
where
    F: Future,
    G: Future,
{
    ImmediateOrJoin::new(f, g)
}

impl<F: Future, G: Future> ImmediateOrJoin<F, G> {
    pub fn new(f: F, g: G) -> Self {
        ImmediateOrJoin {
            first: maybe_done(f),
            second: maybe_done(g),
            state: State::Init,
        }
    }
}

impl<F, G> Future for ImmediateOrJoin<F, G>
where
    F: Future,
    G: Future,
{
    type Output = (F::Output, Option<G::Output>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.state {
            State::Init => {
                if this.first.as_mut().poll(cx).is_ready() {
                    let result = this.first.take_output().unwrap();
                    *this.state = State::Done;
                    Poll::Ready((result, None))
                } else {
                    if this.second.as_mut().poll(cx).is_ready() {
                        *this.state = State::FirstPending;
                    } else {
                        *this.state = State::BothPending;
                    }
                    Poll::Pending
                }
            }
            State::BothPending => {
                let first_poll = this.first.as_mut().poll(cx);
                let second_poll = this.second.as_mut().poll(cx);
                match (first_poll, second_poll) {
                    (Poll::Ready(_), Poll::Ready(_)) => {
                        let first_result = this.first.take_output().unwrap();
                        let second_result = this.second.take_output();
                        *this.state = State::Done;
                        Poll::Ready((first_result, second_result))
                    }
                    (Poll::Ready(_), _) => {
                        *this.state = State::SecondPending;
                        Poll::Pending
                    }
                    (_, Poll::Ready(_)) => {
                        *this.state = State::FirstPending;
                        Poll::Pending
                    }
                    _ => Poll::Pending,
                }
            }
            State::FirstPending => {
                if this.first.as_mut().poll(cx).is_ready() {
                    let first_result = this.first.take_output().unwrap();
                    let second_result = this.second.take_output();
                    *this.state = State::Done;
                    Poll::Ready((first_result, second_result))
                } else {
                    Poll::Pending
                }
            }
            State::SecondPending => {
                if this.second.as_mut().poll(cx).is_ready() {
                    let first_result = this.first.take_output().unwrap();
                    let second_result = this.second.take_output();
                    *this.state = State::Done;
                    Poll::Ready((first_result, second_result))
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
