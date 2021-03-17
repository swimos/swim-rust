use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

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

    pub fn load(&self, order: Ordering) -> Instant {
        let offset_millis = self.offset.load(order);
        let offset = Duration::from_millis(offset_millis);
        self.base + offset
    }

    pub fn store(&self, val: Instant, order: Ordering) {
        let offset = val - self.base;
        self.offset.store(offset.as_millis() as u64, order);
    }
}
