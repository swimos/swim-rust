use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

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
