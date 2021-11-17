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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// An Instant type which can be safely shared between threads.
///
/// The offset is stored in milliseconds.
#[derive(Debug)]
pub struct AtomicInstant {
    base: Instant,
    offset: AtomicU64,
}

impl AtomicInstant {
    pub fn new(base: Instant) -> AtomicInstant {
        AtomicInstant {
            base,
            offset: AtomicU64::new(0),
        }
    }

    /// Loads a value from the atomic Instant.
    ///
    /// `load` takes an [`Ordering`] argument which describes the memory ordering
    /// of this operation. Possible values are [`Ordering::SeqCst`], [`Ordering::Acquire`]
    /// and [`Ordering::Relaxed`].
    ///
    /// # Panics
    ///
    /// Panics if `order` is [`Ordering::Release`] or [`Ordering::AcqRel`].
    pub fn load(&self, order: Ordering) -> Instant {
        let offset_millis = self.offset.load(order);
        let offset = Duration::from_millis(offset_millis);
        self.base + offset
    }

    /// Stores a value into the atomic Instant.
    ///
    /// `store` takes an [`Ordering`] argument which describes the memory ordering of this operation.
    ///  Possible values are [`Ordering::SeqCst`], [`Ordering::Release`] and [`Ordering::Relaxed`].
    ///
    /// # Panics
    ///
    /// Panics if `order` is [`Ordering::Acquire`] or [`Ordering::AcqRel`].
    pub fn store(&self, val: Instant, order: Ordering) {
        let offset = val - self.base;
        self.offset.store(offset.as_millis() as u64, order);
    }
}
