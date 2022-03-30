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

use std::{sync::{atomic::{AtomicUsize, Ordering}, Arc}, pin::Pin, task::{Context, Poll}};
use std::future::Future;

use futures::task::AtomicWaker;

#[cfg(test)]
mod tests;

/// Create a countdown latch. The [`OnDone`] future will complete when all of
/// the [`Countdown`]s have been dropped.
pub fn make_latch<const N: usize>() -> ([Countdown; N], OnDone) {
    let inner = Arc::new(LatchInner {
        count: AtomicUsize::new(N),
        waker: Default::default(),
    });
    let counter_vec = (0..N).map(|_| Countdown { inner: inner.clone() }).collect::<Vec<_>>();
    let counters = counter_vec.try_into()
        .expect("Vector had incorrect number of entries.");
    let on_done = OnDone { inner };
    (counters, on_done)
}

#[derive(Debug)]
struct LatchInner {
    count: AtomicUsize,
    waker: AtomicWaker,
}

/// Drop instances of this type to decrement the latch.
#[derive(Debug)]
pub struct Countdown {
    inner: Arc<LatchInner>,
}

impl Drop for Countdown {
    fn drop(&mut self) {
        let LatchInner { count, waker} = &*self.inner;
        if count.fetch_sub(1, Ordering::Release) == 1 {
            waker.wake();
        }
    }
}

/// A future that will complete when the count of the latch reaches 0.
#[derive(Debug)]
pub struct OnDone {
    inner: Arc<LatchInner>,
}

impl Future for OnDone {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let LatchInner { count, waker} = &*self.get_mut().inner;
        if count.load(Ordering::Acquire) == 0 {
            Poll::Ready(())
        } else {
            waker.register(cx.waker());
            if count.load(Ordering::Acquire) == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
}

