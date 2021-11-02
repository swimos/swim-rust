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

pub mod wait;

use crate::wait::WaitStrategy;
use core::fmt;
use crossbeam_utils::CachePadded;
use parking_lot_core::{ParkResult, SpinWait};
use parking_lot_core::{ParkToken, UnparkToken};
use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

const LOCKED_BIT: u8 = 0b01;
const PARKED_BIT: u8 = 0b10;
const UNLOCK_TOKEN: UnparkToken = UnparkToken(1);
const HANDOFF_TOKEN: UnparkToken = UnparkToken(0);

pub type BiLock<T> = RawBiLock<T, SpinWait>;

/// Creates a new BiLock that provides exclusive access to the value. For situations where there
/// will only ever be two owners this is much cheaper than a mutex.
///
/// The returned `BiLock` will be backed by a `SpinWait` strategy which handles contention between
/// the two halves.
pub fn bilock<T>(val: T) -> (BiLock<T>, BiLock<T>) {
    raw_bilock::<T, SpinWait>(val)
}

/// Creates a new BiLock that provides exclusive access to the value. For situations where there
/// will only ever be two owners this is much cheaper than a mutex.
pub fn raw_bilock<T, W>(val: T) -> (RawBiLock<T, W>, RawBiLock<T, W>)
where
    W: WaitStrategy,
{
    let inner = Arc::new(Inner {
        state: CachePadded::new(AtomicU8::new(0)),
        value: UnsafeCell::new(val),
        backoff: PhantomData::default(),
    });
    (
        RawBiLock {
            inner: inner.clone(),
        },
        RawBiLock { inner },
    )
}

/// A lock primitive for protecting shared data between two owned halves. This lock will use `W`
/// (the wait strategy) to handle contention between the two owners.
///
/// A BiLock is fair and will occasionally (roughly every 0.5ms) hand the lock to a parked thread if
/// one is waiting.
pub struct RawBiLock<T, W> {
    inner: Arc<Inner<T, W>>,
}

impl<T, W> Debug for RawBiLock<T, W>
where
    T: Debug,
    W: WaitStrategy,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("BiLock");
        match self.try_lock() {
            Some(guard) => debug_struct.field("data", &*guard).finish(),
            None => debug_struct.field("data", &"locked").finish(),
        }
    }
}

impl<T, W> RawBiLock<T, W>
where
    W: WaitStrategy,
{
    /// Returns true if the two BiLocks point to the same allocation
    pub fn same_bilock(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Acquire the lock and return a guard which will release it when it is dropped.
    ///
    /// If the wait strategy is one which will stop executing until reset, then this will block the
    /// current thread until it is able to acquire the mutex.
    #[inline]
    pub fn lock(&self) -> BiLockGuard<'_, T, W> {
        match self.inner.state.compare_exchange_weak(
            0,
            LOCKED_BIT,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => BiLockGuard { bilock: self },
            Err(_) => self.lock_park(),
        }
    }

    /// Try and acquire the lock.
    #[inline]
    pub fn try_lock(&self) -> Option<BiLockGuard<'_, T, W>> {
        match self.inner.state.compare_exchange_weak(
            0,
            LOCKED_BIT,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => Some(BiLockGuard { bilock: self }),
            Err(_) => None,
        }
    }

    #[cold]
    fn lock_park(&self) -> BiLockGuard<'_, T, W> {
        let mut spinwait = W::new();

        loop {
            let mut state = self.inner.state.load(Ordering::Relaxed);
            if state & LOCKED_BIT == 0 {
                // Try and acquire the lock
                match self.inner.state.compare_exchange_weak(
                    state,
                    state | LOCKED_BIT,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return BiLockGuard { bilock: self },
                    Err(x) => {
                        state = x;
                    }
                }
            }

            // Try and perform a spin before attempting to park
            if spinwait.spin() {
                // No need to park yet as the spinwait still has attempts remaining in its quota
                continue;
            }

            // Try and set the parked bit
            if self
                .inner
                .state
                .compare_exchange_weak(
                    state,
                    state | PARKED_BIT,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                // The state has changed or it could not be set. Loop back round to see if we can
                // acquire the lock
                continue;
            }

            let park_result = unsafe {
                parking_lot_core::park(
                    self.inner.as_ref() as *const _ as usize,
                    || self.inner.state.load(Ordering::Relaxed) == LOCKED_BIT | PARKED_BIT,
                    || {},
                    |_, _| {},
                    ParkToken(0),
                    None,
                )
            };

            match park_result {
                ParkResult::Unparked(HANDOFF_TOKEN) => {
                    // Another thread handed us an unpark token. That thread keeps the state marked
                    // as locked and has cleared the parked flag so there is nothing for us to
                    // change here
                    return BiLockGuard { bilock: self };
                }
                ParkResult::Unparked(_) | ParkResult::Invalid | ParkResult::TimedOut => {
                    // We have unparked so reset the strategy to use CPU spinlock hints and thread
                    // yielding before attempting to acquire the lock again
                    spinwait.reset();
                    continue;
                }
            }
        }
    }

    #[inline]
    fn unlock(&self) {
        if self
            .inner
            .state
            .compare_exchange(LOCKED_BIT, 0, Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            self.unpark()
        }
    }

    #[cold]
    fn unpark(&self) {
        let addr = self.inner.as_ref() as *const _ as usize;
        unsafe {
            parking_lot_core::unpark_one(addr, |result| {
                // If there is another thread that is parked
                if result.unparked_threads != 0 && result.be_fair {
                    // Set the state as still locked but no longer parked
                    self.inner.state.store(LOCKED_BIT, Ordering::Relaxed);
                    // Unpark the other thread with the token
                    HANDOFF_TOKEN
                } else {
                    // There are no other threads parked or we are not being fair. Reset the state
                    self.inner.state.store(0, Ordering::Relaxed);
                    UNLOCK_TOKEN
                }
            });
        }
    }

    /// Reunites two `BiLock`s that form a pair or returns an error if they do not guard the same
    /// value.
    pub fn reunite(self, other: RawBiLock<T, W>) -> Result<T, ReuniteError<T, W>>
    where
        T: Unpin,
    {
        if Arc::ptr_eq(&self.inner, &other.inner) {
            drop(other);

            let inner = Arc::try_unwrap(self.inner)
                .ok()
                .expect("Failed to unwrap Arc");
            Ok(inner.value.into_inner())
        } else {
            Err(ReuniteError(self, other))
        }
    }
}

struct Inner<T, W> {
    state: CachePadded<AtomicU8>,
    value: UnsafeCell<T>,
    backoff: PhantomData<W>,
}

unsafe impl<T: Send, W> Send for Inner<T, W> {}
unsafe impl<T: Send, W> Sync for Inner<T, W> {}

/// A RAII guard which will unlock the bilock when it is dropped.
#[must_use = "if a BiLock guard is not used it will be immediately unlocked"]
pub struct BiLockGuard<'l, T, W>
where
    W: WaitStrategy,
{
    bilock: &'l RawBiLock<T, W>,
}

impl<'l, T, W> Drop for BiLockGuard<'l, T, W>
where
    W: WaitStrategy,
{
    #[inline]
    fn drop(&mut self) {
        self.bilock.unlock();
    }
}

impl<T, W> Deref for BiLockGuard<'_, T, W>
where
    W: WaitStrategy,
{
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.bilock.inner.value.get() }
    }
}

impl<T: Unpin, W> DerefMut for BiLockGuard<'_, T, W>
where
    W: WaitStrategy,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.bilock.inner.value.get() }
    }
}

pub struct ReuniteError<T, W>(pub RawBiLock<T, W>, pub RawBiLock<T, W>);

impl<T, W> Debug for ReuniteError<T, W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReuniteError").finish()
    }
}

impl<T, W> Display for ReuniteError<T, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Attempted to reunite two BiLocks that don't form a pair")
    }
}

impl<T, W> Error for ReuniteError<T, W> {}

#[cfg(feature = "io")]
mod io {
    use crate::wait::WaitStrategy;
    use crate::RawBiLock;
    use std::ops::DerefMut;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    impl<T, W> AsyncRead for RawBiLock<T, W>
    where
        T: AsyncRead + Unpin,
        W: WaitStrategy,
    {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let mut guard = self.lock();
            Pin::new(guard.deref_mut()).poll_read(cx, buf)
        }
    }

    impl<T, W> AsyncWrite for RawBiLock<T, W>
    where
        T: AsyncWrite + Unpin,
        W: WaitStrategy,
    {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            let mut guard = self.lock();
            Pin::new(guard.deref_mut()).poll_write(cx, buf)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            let mut guard = self.lock();
            Pin::new(guard.deref_mut()).poll_flush(cx)
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            let mut guard = self.lock();
            Pin::new(guard.deref_mut()).poll_flush(cx)
        }
    }
}
