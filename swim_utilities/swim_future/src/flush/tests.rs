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

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::or_flush;
use futures::future::ready;
use futures::task::{waker, ArcWake};
use futures::{Future, Sink};

use parking_lot::Mutex;
use pin_utils::pin_mut;

#[derive(Clone, Copy)]
enum State {
    Pending,
    Flushed,
    Failed,
}

struct FakeSinkInner {
    state: State,
    waker: Option<Waker>,
}

#[derive(Clone)]
struct FakeSink(Arc<Mutex<FakeSinkInner>>);

impl FakeSink {
    fn new(state: State) -> Self {
        FakeSink(Arc::new(Mutex::new(FakeSinkInner { state, waker: None })))
    }

    fn set_state(&self, state: State) {
        let mut this = self.0.lock();
        this.state = state;
        match state {
            State::Flushed | State::Failed => {
                if let Some(waker) = this.waker.take() {
                    waker.wake();
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Failed;

impl Sink<()> for FakeSink {
    type Error = Failed;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: ()) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut guard = self.get_mut().0.lock();
        let FakeSinkInner { state, waker } = &mut *guard;
        match *state {
            State::Pending => {
                *waker = Some(cx.waker().clone());
                Poll::Pending
            }
            State::Flushed => Poll::Ready(Ok(())),
            State::Failed => Poll::Ready(Err(Failed)),
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

struct FakeWaker {
    woken: AtomicBool,
}

impl FakeWaker {
    fn was_woken(&self) -> bool {
        self.woken.swap(false, Ordering::SeqCst)
    }
}

impl Default for FakeWaker {
    fn default() -> Self {
        Self {
            woken: AtomicBool::new(false),
        }
    }
}

impl ArcWake for FakeWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.woken.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn future_completes_immediately() {
    let mut sink = FakeSink::new(State::Pending);
    let fut = or_flush(ready(()), &mut sink);
    let (_, flush_result) = fut.await;
    assert!(flush_result.is_none());
}

#[test]
fn future_completes_later_flush_immediate_ok() {
    let mut sink = FakeSink::new(State::Flushed);

    let arc_wake = Arc::new(FakeWaker::default());
    let waker = waker(arc_wake.clone());
    let mut context = Context::from_waker(&waker);

    let (tx, rx) = swim_trigger::trigger();

    let fut = or_flush(rx, &mut sink);
    pin_mut!(fut);

    assert!(fut.as_mut().poll(&mut context).is_pending());

    tx.trigger();
    assert!(arc_wake.was_woken());

    if let Poll::Ready((trigger_result, flush_result)) = fut.poll(&mut context) {
        assert!(trigger_result.is_ok());
        assert!(matches!(flush_result, Some(Ok(_))));
    } else {
        panic!("Unexpected pending.");
    }
}

#[test]
fn future_completes_later_flush_immediate_ok_2() {
    let mut sink = FakeSink::new(State::Flushed);

    let arc_wake = Arc::new(FakeWaker::default());
    let waker = waker(arc_wake.clone());
    let mut context = Context::from_waker(&waker);

    let (tx, rx) = swim_trigger::trigger();

    let fut = or_flush(rx, &mut sink);
    pin_mut!(fut);

    assert!(fut.as_mut().poll(&mut context).is_pending());
    assert!(fut.as_mut().poll(&mut context).is_pending());

    tx.trigger();
    assert!(arc_wake.was_woken());

    if let Poll::Ready((trigger_result, flush_result)) = fut.poll(&mut context) {
        assert!(trigger_result.is_ok());
        assert!(matches!(flush_result, Some(Ok(_))));
    } else {
        panic!("Unexpected pending.");
    }
}

#[test]
fn future_completes_later_flush_immediate_err() {
    let mut sink = FakeSink::new(State::Failed);

    let arc_wake = Arc::new(FakeWaker::default());
    let waker = waker(arc_wake.clone());
    let mut context = Context::from_waker(&waker);

    let (tx, rx) = swim_trigger::trigger();

    let fut = or_flush(rx, &mut sink);
    pin_mut!(fut);

    assert!(fut.as_mut().poll(&mut context).is_pending());

    tx.trigger();
    assert!(arc_wake.was_woken());

    if let Poll::Ready((trigger_result, flush_result)) = fut.poll(&mut context) {
        assert!(trigger_result.is_ok());
        assert!(matches!(flush_result, Some(Err(_))));
    } else {
        panic!("Unexpected pending.");
    }
}

#[test]
fn future_completes_later_before_flush() {
    let mut sink = FakeSink::new(State::Pending);
    let sink_ref = sink.clone();

    let arc_wake = Arc::new(FakeWaker::default());
    let waker = waker(arc_wake.clone());
    let mut context = Context::from_waker(&waker);

    let (tx, rx) = swim_trigger::trigger();

    let fut = or_flush(rx, &mut sink);
    pin_mut!(fut);

    assert!(fut.as_mut().poll(&mut context).is_pending());

    tx.trigger();
    assert!(arc_wake.was_woken());

    assert!(fut.as_mut().poll(&mut context).is_pending());

    sink_ref.set_state(State::Flushed);
    assert!(arc_wake.was_woken());

    if let Poll::Ready((trigger_result, flush_result)) = fut.poll(&mut context) {
        assert!(trigger_result.is_ok());
        assert!(matches!(flush_result, Some(Ok(_))));
    } else {
        panic!("Unexpected pending.");
    }
}

#[test]
fn future_completes_later_after_flush() {
    let mut sink = FakeSink::new(State::Pending);
    let sink_ref = sink.clone();

    let arc_wake = Arc::new(FakeWaker::default());
    let waker = waker(arc_wake.clone());
    let mut context = Context::from_waker(&waker);

    let (tx, rx) = swim_trigger::trigger();

    let fut = or_flush(rx, &mut sink);
    pin_mut!(fut);

    assert!(fut.as_mut().poll(&mut context).is_pending());

    sink_ref.set_state(State::Flushed);
    assert!(arc_wake.was_woken());

    assert!(fut.as_mut().poll(&mut context).is_pending());

    tx.trigger();
    assert!(arc_wake.was_woken());

    if let Poll::Ready((trigger_result, flush_result)) = fut.poll(&mut context) {
        assert!(trigger_result.is_ok());
        assert!(matches!(flush_result, Some(Ok(_))));
    } else {
        panic!("Unexpected pending.");
    }
}

#[test]
fn future_completes_later_with_flush() {
    let mut sink = FakeSink::new(State::Pending);
    let sink_ref = sink.clone();

    let arc_wake = Arc::new(FakeWaker::default());
    let waker = waker(arc_wake.clone());
    let mut context = Context::from_waker(&waker);

    let (tx, rx) = swim_trigger::trigger();

    let fut = or_flush(rx, &mut sink);
    pin_mut!(fut);

    assert!(fut.as_mut().poll(&mut context).is_pending());

    sink_ref.set_state(State::Flushed);
    tx.trigger();
    assert!(arc_wake.was_woken());

    if let Poll::Ready((trigger_result, flush_result)) = fut.poll(&mut context) {
        assert!(trigger_result.is_ok());
        assert!(matches!(flush_result, Some(Ok(_))));
    } else {
        panic!("Unexpected pending.");
    }
}

#[test]
fn future_completes_later_with_flush_2() {
    let mut sink = FakeSink::new(State::Pending);
    let sink_ref = sink.clone();

    let arc_wake = Arc::new(FakeWaker::default());
    let waker = waker(arc_wake.clone());
    let mut context = Context::from_waker(&waker);

    let (tx, rx) = swim_trigger::trigger();

    let fut = or_flush(rx, &mut sink);
    pin_mut!(fut);

    assert!(fut.as_mut().poll(&mut context).is_pending());
    assert!(fut.as_mut().poll(&mut context).is_pending());

    sink_ref.set_state(State::Flushed);
    tx.trigger();
    assert!(arc_wake.was_woken());

    if let Poll::Ready((trigger_result, flush_result)) = fut.poll(&mut context) {
        assert!(trigger_result.is_ok());
        assert!(matches!(flush_result, Some(Ok(_))));
    } else {
        panic!("Unexpected pending.");
    }
}
