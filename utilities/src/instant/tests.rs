use crate::instant::AtomicInstant;
use std::sync::atomic::Ordering::Relaxed;
use std::{thread, time};
use tokio::time::Instant;

#[test]
fn atomic_instant_create() {
    let instant = Instant::now();
    let atomic_instant = AtomicInstant::new(instant.clone());

    assert_eq!(atomic_instant.offset.load(Relaxed), 0);
    assert_eq!(atomic_instant.base, instant);
}

#[test]
fn atomic_instant_store() {
    let first_instant = Instant::now();
    let atomic_instant = AtomicInstant::new(first_instant.clone());

    thread::sleep(time::Duration::from_secs(1));

    let second_instant = Instant::now();
    atomic_instant.store(second_instant, Relaxed);

    assert_eq!(atomic_instant.base, first_instant);
    assert_eq!(
        atomic_instant.offset.load(Relaxed),
        (second_instant - first_instant).as_millis() as u64
    );
}

#[test]
fn atomic_instant_load() {
    let first_instant = Instant::now();
    let atomic_instant = AtomicInstant::new(first_instant.clone());

    thread::sleep(time::Duration::from_secs(1));

    let second_instant = Instant::now();
    atomic_instant.store(second_instant, Relaxed);

    let expected = (second_instant - first_instant).as_millis();
    let actual = (atomic_instant.load(Relaxed) - first_instant).as_millis();

    assert_eq!(expected, actual);
}
