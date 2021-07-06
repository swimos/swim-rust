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

use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(test)]
mod tests;

/// A wrapper that makes futures cancellable.
///
/// The future `F` will run until completion or until the cancel future `C` has completed.
#[pin_project]
pub struct Cancellable<F, C> {
    #[pin]
    future: F,
    #[pin]
    cancel: C,
}

impl<F: Future, C: Future> Cancellable<F, C> {
    pub fn new(future: F, cancel: C) -> Cancellable<F, C> {
        Cancellable { future, cancel }
    }
}

impl<F: Future, C: Future> Future for Cancellable<F, C> {
    type Output = CancellableResult<F::Output, C::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.poll(cx) {
            Poll::Ready(result) => Poll::Ready(CancellableResult::Completed(result)),
            Poll::Pending => match this.cancel.poll(cx) {
                Poll::Ready(result) => Poll::Ready(CancellableResult::Cancelled(result)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// Result returned by a [`Cancellable`] future.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CancellableResult<T, S> {
    /// Variant returned if the future `F` of [`Cancellable`] has completed.
    Completed(T),
    /// Variant returned if the cancel future `C` of [`Cancellable`] has completed.
    Cancelled(S),
}
