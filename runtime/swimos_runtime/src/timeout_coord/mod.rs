// Copyright 2015-2023 Swim Inc.
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

const INIT: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteResult {
    Unanimous,
    UnanimityPending,
}

#[derive(Debug)]
struct Inner {
    flags: AtomicU8,
    waker: AtomicWaker,
    unanimity: u8,
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

/// Allows the read, http and write parts of the agent runtime to vote on when the runtime should stop.
/// The [`Receiver`] future will only complete when all three [`Sender`]s have voted to stop. If
/// only one or two senders have voted to stop, they may rescind their votes. Rescinding a vote will
/// only be respected if unanimity was not reached.
pub fn agent_timeout_coordinator() -> (Voter, Voter, Voter, Receiver) {
    let ([sender1, sender2, sender3], receiver) = multi_party_coordinator::<3>();
    (sender1, sender2, sender3, receiver)
}

/// Allows the read and write parts of the downlink runtime to vote on when the runtime should stop.
/// The [`Receiver`] future will only complete when both [`Sender`]s have voted to stop. If only one
/// sender has voted to stop, it may rescind its vote. Rescinding a vote will only be respected if unanimity
/// was not reached.
pub fn downlink_timeout_coordinator() -> (Voter, Voter, Receiver) {
    let ([sender1, sender2], receiver) = multi_party_coordinator::<2>();
    (sender1, sender2, receiver)
}

pub trait NumParties {
    fn all() -> u8;
}

impl NumParties for [Voter; 2] {
    fn all() -> u8 {
        (1 << 2) - 1
    }
}
impl NumParties for [Voter; 3] {
    fn all() -> u8 {
        (1 << 3) - 1
    }
}
impl NumParties for [Voter; 4] {
    fn all() -> u8 {
        (1 << 4) - 1
    }
}
impl NumParties for [Voter; 5] {
    fn all() -> u8 {
        (1 << 5) - 1
    }
}
impl NumParties for [Voter; 6] {
    fn all() -> u8 {
        (1 << 6) - 1
    }
}
impl NumParties for [Voter; 7] {
    fn all() -> u8 {
        (1 << 7) - 1
    }
}
impl NumParties for [Voter; 8] {
    fn all() -> u8 {
        u8::MAX
    }
}

pub(crate) fn multi_party_coordinator<const N: usize>() -> ([Voter; N], Receiver)
where
    [Voter; N]: NumParties,
{
    let all: u8 = <[Voter; N] as NumParties>::all();
    let inner = Arc::new(Inner {
        flags: AtomicU8::new(INIT),
        waker: Default::default(),
        unanimity: all,
    });
    let senders = std::array::from_fn::<Voter, N, _>(|i| {
        let flag: u8 = 1 << i;
        let inverse: u8 = all ^ flag;
        Voter {
            flag,
            inverse,
            voted: Cell::new(false),
            inner: inner.clone(),
        }
    });
    let receiver = Receiver { inner };
    (senders, receiver)
}

const TWO_VOTERS_LIM: u8 = 3;

impl Voter {
    /// Vote for the process to stop. Returns whether unanimity has been reached. This can
    /// be called any number of times.
    pub fn vote(&self) -> VoteResult {
        let Voter {
            flag,
            inverse,
            voted,
            inner,
        } = self;
        let Inner { flags, waker, .. } = &**inner;
        let before = flags.fetch_or(*flag, Ordering::Release);
        voted.set(true);
        if before == *inverse {
            waker.wake();
            VoteResult::Unanimous
        } else {
            VoteResult::UnanimityPending
        }
    }

    /// Rescind a vote for termination. This has no effect if unanimity has already been reached or if this
    /// sender had not previously voted. Sequences of vote and rescind operations can be called any number of
    /// times in any order. Once unanimity has been reached, no calls will have any effect.
    ///
    /// Returns whether unanimity had already been reached before this operation was attempted.
    pub fn rescind(&self) -> VoteResult {
        let Voter {
            flag,
            inverse,
            voted,
            inner,
            ..
        } = self;
        let Inner { flags, .. } = &**inner;
        if voted.get() {
            if *inverse < TWO_VOTERS_LIM {
                if flags
                    .compare_exchange(*flag, INIT, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                {
                    VoteResult::Unanimous
                } else {
                    VoteResult::UnanimityPending
                }
            } else {
                loop {
                    let current = flags.load(Ordering::Relaxed);
                    if current == inverse | flag {
                        break VoteResult::Unanimous;
                    } else if flags
                        .compare_exchange(
                            current,
                            current & !flag,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break VoteResult::UnanimityPending;
                    }
                }
            }
        } else {
            VoteResult::UnanimityPending
        }
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
        let Inner {
            flags,
            waker,
            unanimity,
        } = &*self.get_mut().inner;
        if flags.load(Ordering::Relaxed) == *unanimity {
            Poll::Ready(())
        } else {
            waker.register(cx.waker());
            if flags.load(Ordering::Acquire) == *unanimity {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
}
