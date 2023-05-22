// Copyright 2015-2023 Swim Inc.
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

use futures::task::{waker, ArcWake};
use std::{
    cell::Cell,
    num::NonZeroUsize,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll},
};
use std::{pin::pin, sync::atomic::Ordering};
use swim_num::non_zero_usize;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{byte_channel, RunWithBudget};

struct TestWaker(AtomicBool);

impl TestWaker {
    fn triggered(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for TestWaker {
    fn default() -> Self {
        Self(AtomicBool::new(false))
    }
}

impl ArcWake for TestWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::Relaxed);
    }
}

#[test]
fn consume_unit_of_budget() {
    let w = Arc::new(TestWaker::default());
    let waker = waker(w.clone());
    let mut cx = Context::from_waker(&waker);

    super::set_budget(10);
    assert!(super::consume_budget(&mut cx).is_ready());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(9));
    assert!(!w.triggered());
}

#[test]
fn exhaust_budget() {
    let w = Arc::new(TestWaker::default());
    let waker = waker(w.clone());
    let mut cx = Context::from_waker(&waker);

    super::set_budget(1);
    assert!(super::consume_budget(&mut cx).is_pending());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), None);
    assert!(w.triggered());
}

#[test]
fn return_budget_when_pending() {
    super::set_budget(2);
    assert!(super::track_progress(Poll::<()>::Pending).is_pending());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(3));
}

#[test]
fn budget_stays_consumed_when_ready() {
    super::set_budget(2);
    assert!(super::track_progress(Poll::Ready(())).is_ready());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(2));
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);

#[test]
fn writer_consumes_budget() {
    let w = Arc::new(TestWaker::default());
    let waker = waker(w.clone());
    let mut cx = Context::from_waker(&waker);
    let (tx, _rx) = byte_channel(BUFFER_SIZE);

    super::set_budget(3);

    let mut tx = pin!(tx);

    let bytes = vec![0, 1, 2, 3];

    let poll = tx.as_mut().poll_write(&mut cx, &bytes);
    assert!(!w.triggered());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(2));
    assert!(matches!(poll, Poll::Ready(Ok(4))));

    let poll = tx.as_mut().poll_write(&mut cx, &bytes);
    assert!(!w.triggered());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(1));
    assert!(matches!(poll, Poll::Ready(Ok(4))));

    let poll = tx.poll_write(&mut cx, &bytes);
    assert!(w.triggered());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), None);
    assert!(matches!(poll, Poll::Pending));
}

#[test]
fn reader_consumes_budget() {
    let w = Arc::new(TestWaker::default());
    let waker = waker(w.clone());
    let mut cx = Context::from_waker(&waker);
    let (tx, rx) = byte_channel(BUFFER_SIZE);

    let tx = pin!(tx);
    let mut rx = pin!(rx);

    let bytes: Vec<u8> = (0..16).collect();

    super::set_budget(4);
    let poll = tx.poll_write(&mut cx, &bytes);
    assert!(!w.triggered());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(3));
    assert!(matches!(poll, Poll::Ready(Ok(16))));

    let mut buf: [u8; 4] = [0; 4];

    let mut read_buf = ReadBuf::new(&mut buf);
    let poll = rx.as_mut().poll_read(&mut cx, &mut read_buf);
    assert!(!w.triggered());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(2));
    assert!(matches!(poll, Poll::Ready(Ok(_))));

    let mut read_buf = ReadBuf::new(&mut buf);
    let poll = rx.as_mut().poll_read(&mut cx, &mut read_buf);
    assert!(!w.triggered());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(1));
    assert!(matches!(poll, Poll::Ready(Ok(_))));

    let mut read_buf = ReadBuf::new(&mut buf);
    let poll = rx.as_mut().poll_read(&mut cx, &mut read_buf);
    assert!(w.triggered());
    assert_eq!(super::TASK_BUDGET.with(Cell::get), None);
    assert!(matches!(poll, Poll::Pending));
}

const BUDGET: NonZeroUsize = non_zero_usize!(13);

#[tokio::test]
async fn with_budget_sets_budget() {
    super::set_budget(1);

    let fut = RunWithBudget::with_budget(BUDGET, async {
        assert_eq!(super::TASK_BUDGET.with(Cell::get), Some(BUDGET.get()));
    });

    fut.await;
}
