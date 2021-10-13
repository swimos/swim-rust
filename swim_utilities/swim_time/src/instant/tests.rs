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

use crate::instant::AtomicInstant;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;
use std::{thread, time};

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
