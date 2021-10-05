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

use crate::split::bilock::{bilock, BiLock};
use futures::future::join;
use futures::task::waker;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::task::Context;
use swim_utilities::test_util::TestWaker;

#[test]
fn bounds() {
    fn f<T: Send + Sync>() {}
    f::<BiLock<()>>();
}

#[tokio::test]
async fn simple_lock() {
    let value = 13;
    let (left, right) = bilock(value);
    let guard = left.lock().await;
    assert_eq!(*guard.deref(), 13);
    drop(guard);

    let guard = right.lock().await;
    assert_eq!(*guard.deref(), 13);
}

#[tokio::test]
async fn guards() {
    let value = 13;
    let (left, right) = bilock(value);

    let test_waker = Arc::new(TestWaker::default());
    let waker = waker(test_waker.clone());
    let mut ctx = Context::from_waker(&waker);

    let poll = right.poll_lock(&mut ctx);
    assert!(poll.is_ready());
    drop(poll);

    let mut guard = left.lock().await;
    *guard.deref_mut() = 15;

    let poll = right.poll_lock(&mut ctx);
    assert!(poll.is_pending());
    let poll = right.poll_lock(&mut ctx);
    assert!(poll.is_pending());

    drop(guard);

    let poll = right.poll_lock(&mut ctx);
    assert!(poll.is_ready());
    assert!(test_waker.woken());
}

#[tokio::test]
async fn two_tasks() {
    let value = 13;
    let (left, right) = bilock(value);

    let left_task = tokio::spawn(async move {
        let mut guard = left.lock().await;
        *guard.deref_mut() += 1;
        drop(guard);
        left
    });

    let right_task = tokio::spawn(async move {
        let mut guard = right.lock().await;
        *guard.deref_mut() += 100;
    });

    let (left_result, right_result) = join(left_task, right_task).await;
    assert!(left_result.is_ok());
    let left = left_result.unwrap();
    assert!(right_result.is_ok());

    let guard = left.lock().await;
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
