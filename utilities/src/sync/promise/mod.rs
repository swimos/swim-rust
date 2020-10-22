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

use crate::sync::trigger;
use futures::ready;
use futures::task::{Context, Poll};
use futures::FutureExt;
use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(Debug)]
struct PromiseInner<T>(Arc<UnsafeCell<Option<Arc<T>>>>);

unsafe impl<T: Send> Send for PromiseInner<T> {}
unsafe impl<T: Sync> Sync for PromiseInner<T> {}

/// Send half of a promise that can provide the value.
#[derive(Debug)]
pub struct Sender<T> {
    trigger: trigger::Sender,
    data: PromiseInner<T>,
}

/// Receive half of the promise that can await the completion promise and provide access to the value.
#[derive(Debug)]
pub struct Receiver<T> {
    trigger: trigger::Receiver,
    data: PromiseInner<T>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver {
            trigger: self.trigger.clone(),
            data: PromiseInner(self.data.0.clone()),
        }
    }
}

/// A promise allows a value to be provided, exactly once, at some time in the future.
pub fn promise<T: Send + Sync>() -> (Sender<T>, Receiver<T>) {
    let data = Arc::new(UnsafeCell::new(None));
    let (tx, rx) = trigger::trigger();
    (
        Sender {
            trigger: tx,
            data: PromiseInner(data.clone()),
        },
        Receiver {
            trigger: rx,
            data: PromiseInner(data),
        },
    )
}

impl<T: Send + Sync> Sender<T> {
    /// Provide the value for the promise. This consumes the sender, which cannot be cloned, so the
    /// value can be provided exactly once.
    pub fn provide(self, value: T) -> Result<(), T> {
        let Sender {
            trigger,
            data: PromiseInner(data),
        } = self;
        unsafe {
            *data.get() = Some(Arc::new(value));
            if trigger.trigger() {
                Ok(())
            } else if let Ok(value) = Arc::try_unwrap((&mut *data.get()).take().unwrap()) {
                Err(value)
            } else {
                unreachable!()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromiseError;

impl Display for PromiseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Promise was dropped before it was completed.")
    }
}

impl<T> Receiver<T> {
    pub fn same_promise(left: &Self, right: &Self) -> bool {
        trigger::Receiver::same_receiver(&left.trigger, &right.trigger)
    }
}

impl Error for PromiseError {}

impl<T: Send + Sync> Future for Receiver<T> {
    type Output = Result<Arc<T>, PromiseError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if ready!(self.trigger.poll_unpin(cx)).is_ok() {
            let value = (unsafe { &*self.data.0.get() })
                .as_ref()
                .expect("Promise did not provide a value on completion.")
                .clone();
            Poll::Ready(Ok(value))
        } else {
            Poll::Ready(Err(PromiseError))
        }
    }
}
