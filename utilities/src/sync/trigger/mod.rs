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

use futures_util::task::Context;
use parking_lot::Mutex;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};
use std::task::Waker;
use tokio::macros::support::{Pin, Poll};

#[cfg(test)]
mod tests;

#[derive(Debug)]
struct TriggerInner {
    flag: AtomicU8,
    waiters: Mutex<Vec<Waker>>,
}

#[derive(Debug)]
pub struct Sender {
    inner: Option<Weak<TriggerInner>>,
}

impl Error for TriggerError {}

#[derive(Clone, Debug)]
pub struct Receiver {
    inner: Arc<TriggerInner>,
}

/// Create a simple one to many asynchronous trigger. Every copy of the receiver will complete
/// successfully (when the sender is triggered) or with an error (if the sender is dropped).
pub fn trigger() -> (Sender, Receiver) {
    let inner = Arc::new(TriggerInner {
        flag: AtomicU8::new(0),
        waiters: Mutex::new(vec![]),
    });
    (
        Sender {
            inner: Some(Arc::downgrade(&inner)),
        },
        Receiver { inner },
    )
}

impl Sender {
    /// Trigger the sender causing all receivers to complete successfully.
    pub fn trigger(mut self) -> bool {
        if let Some(inner) = self.inner.take().and_then(|weak| weak.upgrade()) {
            trigger_with(&inner, 1);
            true
        } else {
            false
        }
    }
}

fn trigger_with(inner: &Arc<TriggerInner>, flag: u8) {
    inner.flag.store(flag, Ordering::Release);
    let mut lock = inner.waiters.lock();
    for waker in std::mem::take::<Vec<Waker>>(lock.as_mut()).into_iter() {
        waker.wake();
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take().and_then(|weak| weak.upgrade()) {
            if inner.flag.load(Ordering::Acquire) == 0 {
                trigger_with(&inner, 2);
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TriggerError;

impl Display for TriggerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Trigger sender was dropped.")
    }
}

impl Future for Receiver {
    type Output = Result<(), TriggerError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let flag = self.inner.flag.load(Ordering::Acquire);
        match flag {
            0 => {
                let mut lock = self.inner.waiters.lock();
                match self.inner.flag.load(Ordering::Acquire) {
                    0 => {
                        lock.push(cx.waker().clone());
                        Poll::Pending
                    }
                    1 => Poll::Ready(Ok(())),
                    _ => Poll::Ready(Err(TriggerError)),
                }
            }
            1 => Poll::Ready(Ok(())),
            _ => Poll::Ready(Err(TriggerError)),
        }
    }
}
