// Copyright 2015-2020 SWIM.AI inc.
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

use crate::sync::trigger::TriggerError;
use futures::task::{waker_ref, ArcWake};
use pin_utils::pin_mut;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

#[tokio::test]
async fn trigger_before_await() {
    let (tx, rx) = super::trigger();
    assert!(tx.trigger());
    assert_eq!(rx.await, Ok(()));
}

#[tokio::test]
async fn drop_before_await() {
    let (tx, rx) = super::trigger();
    drop(tx);
    assert_eq!(rx.await, Err(TriggerError));
}

#[tokio::test]
async fn wait_twice() {
    let (tx, rx) = super::trigger();
    let rx2 = rx.clone();
    assert!(tx.trigger());
    assert_eq!(rx.await, Ok(()));
    assert_eq!(rx2.await, Ok(()));
}

#[tokio::test]
async fn trigger_no_receivers() {
    let (tx, rx) = super::trigger();
    drop(rx);
    assert!(!tx.trigger());
}

struct TestWaker(AtomicBool);

impl TestWaker {
    fn new() -> Arc<Self> {
        Arc::new(TestWaker(AtomicBool::new(false)))
    }

    fn is_woken(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    fn reset(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl ArcWake for TestWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn trigger_after_pending() {
    let arc_waker = TestWaker::new();
    let waker = waker_ref(&arc_waker);
    let mut context = Context::from_waker(&*waker);
    let (tx, rx) = super::trigger();
    pin_mut!(rx);

    assert_eq!(rx.as_mut().poll(&mut context), Poll::Pending);

    assert!(!arc_waker.is_woken());

    tx.trigger();

    assert!(arc_waker.is_woken());

    arc_waker.reset();

    assert_eq!(rx.as_mut().poll(&mut context), Poll::Ready(Ok(())));

    assert!(!arc_waker.is_woken());
}

#[test]
fn drop_after_pending() {
    let arc_waker = TestWaker::new();
    let waker = waker_ref(&arc_waker);
    let mut context = Context::from_waker(&*waker);
    let (tx, rx) = super::trigger();
    pin_mut!(rx);

    assert_eq!(rx.as_mut().poll(&mut context), Poll::Pending);

    assert!(!arc_waker.is_woken());

    drop(tx);

    assert!(arc_waker.is_woken());

    arc_waker.reset();

    assert_eq!(
        rx.as_mut().poll(&mut context),
        Poll::Ready(Err(TriggerError))
    );

    assert!(!arc_waker.is_woken());
}

#[tokio::test(threaded_scheduler)]
async fn trigger_threaded() {
    let (tx, rx) = super::trigger();

    let rx_task = swim_runtime::task::spawn(async move { rx.await });

    let tx_task = swim_runtime::task::spawn(async move { tx.trigger() });

    let tx_result = tx_task.await;
    let rx_result = rx_task.await;

    assert!(matches!(tx_result, Ok(true)));
    assert!(matches!(rx_result, Ok(Ok(()))));
}
