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

use crate::wait::Park;
use crate::{bilock, raw_bilock, RawBiLock, LOCKED_BIT, PARKED_BIT};
use futures::future::join;
use parking_lot_core::SpinWait;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn bounds() {
    fn f<T: Send + Sync>() {}
    f::<RawBiLock<(), SpinWait>>();
}

#[tokio::test]
async fn simple_lock() {
    let value = 13;
    let (left, right) = bilock(value);
    let guard = left.lock();
    assert_eq!(*guard.deref(), 13);
    drop(guard);

    let guard = right.lock();
    assert_eq!(*guard.deref(), 13);
}

#[tokio::test]
async fn guards() {
    let value = 13;
    let (left, right) = bilock(value);

    let guard_opt = right.try_lock();
    assert!(guard_opt.is_some());
    drop(guard_opt);

    let mut guard = left.lock();
    *guard.deref_mut() = 15;

    let guard_opt = right.try_lock();
    assert!(guard_opt.is_none());
    let guard_opt = right.try_lock();
    assert!(guard_opt.is_none());

    drop(guard);

    let guard_opt = right.try_lock();
    assert!(guard_opt.is_some());
}

#[tokio::test]
async fn two_tasks() {
    let value = 13;
    let (left, right) = bilock(value);

    let left_task = tokio::spawn(async move {
        let mut guard = left.lock();
        *guard.deref_mut() += 1;
        drop(guard);
        left
    });

    let right_task = tokio::spawn(async move {
        let mut guard = right.lock();
        *guard.deref_mut() += 100;
    });

    let (left_result, right_result) = join(left_task, right_task).await;
    assert!(left_result.is_ok());
    let left = left_result.unwrap();
    assert!(right_result.is_ok());

    let guard = left.lock();
    assert_eq!(*guard.deref(), 114);
}

#[test]
fn reunite_ok() {
    let (left, right) = bilock(13);
    let reunite_result = left.reunite(right);
    assert!(reunite_result.is_ok());
    assert_eq!(reunite_result.unwrap(), 13);
}

#[test]
fn reunite_err() {
    let (left, _right) = bilock(13);
    let (_left, right) = bilock(13);
    let reunite_result = left.reunite(right);

    assert!(reunite_result.is_err());
    assert_eq!(
        reunite_result.unwrap_err().to_string(),
        "Attempted to reunite two BiLocks that don't form a pair".to_string()
    );
}

#[test]
fn try_lock() {
    let initial = 13;
    let (_left, right) = bilock(initial);
    let guard = right.try_lock().expect("Failed to try and lock");
    assert_eq!(initial, *guard);
}

#[test]
fn locked_state() {
    let (_left, right) = bilock(13);
    let guard = right.lock();

    let state = right.inner.state.load(Ordering::Relaxed);
    assert_eq!(state, LOCKED_BIT);

    drop(guard);

    let state = right.inner.state.load(Ordering::Relaxed);
    assert_eq!(state, 0);
}

#[test]
fn parked_state() {
    let (left, right) = raw_bilock::<i32, Park>(13);
    let guard = right.lock();

    let inner = right.inner.clone();
    let state = inner.state.load(Ordering::Relaxed);
    assert_eq!(state, LOCKED_BIT);

    let barrier = Arc::new(Barrier::new(2));
    let thread_barrier = barrier.clone();

    let handle = thread::spawn(move || {
        thread_barrier.wait();
        let mut guard = left.lock();
        *guard = 14;
    });

    barrier.wait();

    let mut backoff_count = 0;

    loop {
        let state = inner.state.load(Ordering::Relaxed);
        if state == LOCKED_BIT | PARKED_BIT {
            break;
        } else {
            backoff_count += 1;

            if backoff_count > 10 {
                panic!("Parked state never set");
            } else {
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    drop(guard);
    handle.join().unwrap();

    let guard = right.lock();
    assert_eq!(*guard, 14);
}

#[test]
fn debug_unlocked() {
    let (_left, right) = bilock(13);
    let output = format!("{:?}", right);
    assert_eq!(&output, "BiLock { data: 13 }");
}

#[test]
fn debug_locked() {
    let (_left, right) = bilock(13);
    let _guard = right.lock();
    let output = format!("{:?}", right);
    assert_eq!(&output, "BiLock { data: \"locked\" }");
}
