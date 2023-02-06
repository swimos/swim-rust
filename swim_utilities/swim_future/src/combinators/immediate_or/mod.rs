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
use futures::FutureExt;

use pin_project::pin_project;

#[cfg(test)]
mod tests;

enum JoinState {
    Init,
    BothPending,
    FirstPending,
    SecondPending,
    Done,
}

enum StartState {
    Init,
    Pending,
    Done,
}

/// Future type for the [`immediate_or_join`] function.
#[pin_project]
pub struct ImmediateOrJoin<F: Future, G: Future> {
    #[pin]
    first: MaybeDone<F>,
    #[pin]
    second: MaybeDone<G>,
    state: JoinState,
}

/// Future type for the [`immediate_or_start`] function.
#[pin_project]
pub struct ImmediateOrStart<F: Future, G: Future> {
    #[pin]
    first: F,
    second: Option<G>,
    second_output: Option<G::Output>,
    state: StartState,
}

/// Create a future that will poll a future and, if it does not complete immediately, will
/// attempt to poll a second future. If the second future starts, the future will not complete
/// until both subsidiary actions have completed. If the first future completes immediately,
/// the second future will never be polled.
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

/// Create a future that will poll a future and, if it does not complete immediately, will
/// star polling a second future.  If the first future completes immediately,
/// the second future will never be polled. If the second future completes before the first,
/// both results will be returned. If the first future complets before the second, the result
/// of the first future and the second future itself will be returned. As is must be possible
/// to move the second future, it must be `Unpin`.
///
/// # Arguments
/// * `f` - The first future to poll.
/// * `g` - The second future to poll.
pub fn immediate_or_start<F, G>(f: F, g: G) -> ImmediateOrStart<F, G>
where
    F: Future,
    G: Future,
{
    ImmediateOrStart::new(f, g)
}

impl<F: Future, G: Future> ImmediateOrJoin<F, G> {
    pub fn new(f: F, g: G) -> Self {
        ImmediateOrJoin {
            first: maybe_done(f),
            second: maybe_done(g),
            state: JoinState::Init,
        }
    }
}

impl<F: Future, G: Future> ImmediateOrStart<F, G> {
    pub fn new(f: F, g: G) -> Self {
        ImmediateOrStart {
            first: f,
            second: Some(g),
            second_output: None,
            state: StartState::Init,
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
            JoinState::Init => {
                if this.first.as_mut().poll(cx).is_ready() {
                    let result = this.first.take_output().unwrap();
                    *this.state = JoinState::Done;
                    Poll::Ready((result, None))
                } else {
                    if this.second.as_mut().poll(cx).is_ready() {
                        *this.state = JoinState::FirstPending;
                    } else {
                        *this.state = JoinState::BothPending;
                    }
                    Poll::Pending
                }
            }
            JoinState::BothPending => {
                let first_poll = this.first.as_mut().poll(cx);
                let second_poll = this.second.as_mut().poll(cx);
                match (first_poll, second_poll) {
                    (Poll::Ready(_), Poll::Ready(_)) => {
                        let first_result = this.first.take_output().unwrap();
                        let second_result = this.second.take_output();
                        *this.state = JoinState::Done;
                        Poll::Ready((first_result, second_result))
                    }
                    (Poll::Ready(_), _) => {
                        *this.state = JoinState::SecondPending;
                        Poll::Pending
                    }
                    (_, Poll::Ready(_)) => {
                        *this.state = JoinState::FirstPending;
                        Poll::Pending
                    }
                    _ => Poll::Pending,
                }
            }
            JoinState::FirstPending => {
                if this.first.as_mut().poll(cx).is_ready() {
                    let first_result = this.first.take_output().unwrap();
                    let second_result = this.second.take_output();
                    *this.state = JoinState::Done;
                    Poll::Ready((first_result, second_result))
                } else {
                    Poll::Pending
                }
            }
            JoinState::SecondPending => {
                if this.second.as_mut().poll(cx).is_ready() {
                    let first_result = this.first.take_output().unwrap();
                    let second_result = this.second.take_output();
                    *this.state = JoinState::Done;
                    Poll::Ready((first_result, second_result))
                } else {
                    Poll::Pending
                }
            }
            JoinState::Done => {
                panic!("ImmediateOrJoin polled after complete.")
            }
        }
    }
}

/// Type for the result of the second future passed to `immediate_or_start`.
#[derive(Debug)]
pub enum SecondaryResult<F: Future> {
    /// The second future was never started.
    NotStarted(F),
    /// The second future was started but did not complete.
    Pending(F),
    /// The second future was completed.
    Completed(F::Output),
}

impl<F: Future> SecondaryResult<F> {
    pub fn completed(self) -> Option<F::Output> {
        match self {
            SecondaryResult::Completed(output) => Some(output),
            _ => None,
        }
    }

    pub fn reclaim(self) -> Option<F> {
        match self {
            SecondaryResult::Pending(f) => Some(f),
            _ => None,
        }
    }
}

impl<F, G> Future for ImmediateOrStart<F, G>
where
    F: Future,
    G: Future + Unpin,
{
    type Output = (F::Output, SecondaryResult<G>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        match this.state {
            StartState::Init => {
                if let Poll::Ready(result) = this.first.as_mut().poll(cx) {
                    *this.state = StartState::Done;
                    let second_fut = this.second.take().unwrap();
                    Poll::Ready((result, SecondaryResult::NotStarted(second_fut)))
                } else {
                    if let Poll::Ready(result) = this.second.as_mut().unwrap().poll_unpin(cx) {
                        *this.second = None;
                        *this.second_output = Some(result);
                    }
                    *this.state = StartState::Pending;
                    Poll::Pending
                }
            }
            StartState::Pending => {
                let first_poll = this.first.as_mut().poll(cx);
                let second_poll = this.second.as_mut().map(|second| second.poll_unpin(cx));
                match (first_poll, second_poll) {
                    (Poll::Ready(first_result), Some(Poll::Ready(second_result))) => {
                        *this.state = StartState::Done;
                        Poll::Ready((first_result, SecondaryResult::Completed(second_result)))
                    }
                    (Poll::Ready(first_result), Some(Poll::Pending)) => {
                        let second_fut = this.second.take().unwrap();
                        Poll::Ready((first_result, SecondaryResult::Pending(second_fut)))
                    }
                    (Poll::Ready(first_result), None) => {
                        let second_result = this.second_output.take().unwrap();
                        Poll::Ready((first_result, SecondaryResult::Completed(second_result)))
                    }
                    (_, Some(Poll::Ready(second_result))) => {
                        *this.second_output = Some(second_result);
                        *this.second = None;
                        Poll::Pending
                    }
                    _ => Poll::Pending,
                }
            }
            StartState::Done => {
                panic!("ImmediateOrStart polled after complete.")
            }
        }
    }
}
