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

use std::{
    cell::Cell,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures::{task::AtomicWaker, Future};
use static_assertions::{assert_impl_all, assert_not_impl_any};

#[cfg(test)]
mod tests;

const INIT: u8 = 0b00;
const FIRST: u8 = 0b01;
const SECOND: u8 = 0b10;
const UNANIMITY: u8 = 0b11;

#[derive(Debug)]
struct Inner {
    flags: AtomicU8,
    waker: AtomicWaker,
}

/// Allows for a party to the coordination to vote for the process to stop or to attempt
/// to rescind a previous vote.
#[derive(Debug)]
pub struct Voter {
    flag: u8,
    inverse: u8,
    voted: Cell<bool>,
    inner: Arc<Inner>,
}

assert_impl_all!(Voter: Send);
assert_not_impl_any!(Voter: Clone);

/// A future that completes when the parties reach unanimity.
#[derive(Debug)]
pub struct Receiver {
    inner: Arc<Inner>,
}

assert_impl_all!(Receiver: Send, Sync);
assert_not_impl_any!(Receiver: Clone);

/// Allows the read and write parts of the agent runtime to vote on when the runtime should stop.
/// The [`Receiver`] future will only complete when both [`Sender`]s have voted to stop. If
/// only one sender has voted to stop, it may rescind its vote. Rescinding a vote will only be
/// respected if unanimity was not reached.
pub fn timeout_coordinator() -> (Voter, Voter, Receiver) {
    let inner = Arc::new(Inner {
        flags: AtomicU8::new(INIT),
        waker: Default::default(),
    });
    let sender1 = Voter {
        flag: FIRST,
        inverse: SECOND,
        voted: Cell::new(false),
        inner: inner.clone(),
    };
    let sender2 = Voter {
        flag: SECOND,
        inverse: FIRST,
        voted: Cell::new(false),
        inner: inner.clone(),
    };
    let receiver = Receiver { inner };
    (sender1, sender2, receiver)
}

impl Voter {
    /// Vote for the process to stop. Returns true if unanimity has been reached. This can
    /// be called any number of times.
    pub fn vote(&self) -> bool {
        let Voter {
            flag,
            inverse,
            voted,
            inner,
        } = self;
        let Inner { flags, waker } = &**inner;
        let before = flags.fetch_or(*flag, Ordering::Release);
        voted.set(true);
        if before == *inverse {
            waker.wake();
            true
        } else {
            false
        }
    }

    /// Rescid a vote for termination. Returns true if unanimity has already been reached (and
    /// this has had no effect). If this sender has not previously voted to stop, this does
    /// nothing. Sequences of vote and rescind can be called any number of times in any order.
    /// Once unanimity has been reached, no calls will have any effect.
    pub fn rescind(&self) -> bool {
        let Voter {
            flag, voted, inner, ..
        } = self;
        let Inner { flags, .. } = &**inner;
        voted.get()
            && flags
                .compare_exchange(*flag, INIT, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
    }
}

impl Drop for Voter {
    fn drop(&mut self) {
        //If a sender is dropped before voting, vote to ensure the receiver can't deadlock.
        if !self.voted.get() {
            self.vote();
        }
    }
}

impl Future for Receiver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Inner { flags, waker } = &*self.get_mut().inner;
        if flags.load(Ordering::Relaxed) == UNANIMITY {
            Poll::Ready(())
        } else {
            waker.register(cx.waker());
            if flags.load(Ordering::Acquire) == UNANIMITY {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
}
