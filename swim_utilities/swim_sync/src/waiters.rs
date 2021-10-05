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

use slab::Slab;
use std::task::Waker;

/// Maintains a bag of readers waiting to obtain the lock.
#[derive(Debug, Default)]
pub struct ReadWaiters {
    waiters: Slab<Waker>,
    epoch: u64,
}

impl ReadWaiters {
    /// Insert a new waker return the assigned slot and the current epoch.
    pub fn insert(&mut self, waker: Waker) -> (usize, u64) {
        let ReadWaiters { waiters, epoch } = self;
        (waiters.insert(waker), *epoch)
    }

    /// If the epoch has changed, does the same as `insert`, otherwise updates the waker in
    /// the specified slot.
    pub fn update(&mut self, waker: &Waker, slot: usize, expected_epoch: u64) -> (usize, u64) {
        let ReadWaiters { waiters, epoch } = self;
        if *epoch == expected_epoch {
            if let Some(existing) = waiters.get_mut(slot) {
                if !waker.will_wake(existing) {
                    *existing = waker.clone();
                }
                (slot, expected_epoch)
            } else {
                (waiters.insert(waker.clone()), *epoch)
            }
        } else {
            (waiters.insert(waker.clone()), *epoch)
        }
    }

    /// Remove the waker at the provided slot if the epoch has not changed.
    pub fn remove(&mut self, slot: usize, expected_epoch: u64) {
        let ReadWaiters { waiters, epoch } = self;
        if *epoch == expected_epoch {
            waiters.remove(slot);
        }
    }

    /// Wake all wakers and advance the epoch.
    pub fn wake_all(&mut self) {
        let ReadWaiters { waiters, epoch } = self;
        *epoch = epoch.wrapping_add(1);
        waiters.drain().for_each(|w| w.wake());
    }

    pub fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }
}
