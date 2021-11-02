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

use parking_lot_core::SpinWait;

/// A strategy for handling contention when the lock cannot be acquired.
pub trait WaitStrategy {
    /// Construct an instance of the strategy.
    fn new() -> Self;

    /// Execute a spin operation. This function should return whether the operation was successful.
    /// If it is not, then the thread will park until it is woken up or unparked.
    fn spin(&mut self) -> bool;

    /// Reset the spin strategy back to its initial state.
    fn reset(&mut self);
}

/// An implementation of `WaitStrategy` for Parking Lot's `SpinWait`.
///
/// This strategy will perform exponential backoff through a CPU-bound loop and then thread yields
/// until its quota has been reached.
impl WaitStrategy for SpinWait {
    fn new() -> Self {
        SpinWait::new()
    }

    fn spin(&mut self) -> bool {
        SpinWait::spin(self)
    }

    fn reset(&mut self) {
        SpinWait::reset(self)
    }
}

/// A wait strategy that will perform no backoff and effectively serves as a busy wait.
pub struct BusyWait;
impl WaitStrategy for BusyWait {
    fn new() -> Self {
        BusyWait
    }

    fn spin(&mut self) -> bool {
        true
    }

    fn reset(&mut self) {}
}

/// A wait strategy that will perform no backoff and the `BiLock` will park as soon as it can when
/// it cannot acquire the lock.
pub struct Park;
impl WaitStrategy for Park {
    fn new() -> Self {
        Park
    }

    fn spin(&mut self) -> bool {
        false
    }

    fn reset(&mut self) {}
}

/// A wait strategy that will always execute a CPU-bound loop and then yield to the OS when it
/// cannot acquire the lock.
///
/// This is a thin wrapper around a `SpinWait` that always resets its internal state when the quota
/// has been reached; resulting in no park operations.
pub struct NoPark(SpinWait);
impl WaitStrategy for NoPark {
    fn new() -> Self {
        NoPark(SpinWait::new())
    }

    fn spin(&mut self) -> bool {
        if !self.0.spin() {
            self.0.reset()
        }

        true
    }

    fn reset(&mut self) {
        self.0.reset();
    }
}
