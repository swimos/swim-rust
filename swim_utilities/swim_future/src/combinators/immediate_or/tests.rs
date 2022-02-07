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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::{immediate_or_join, immediate_or_start, SecondaryResult};
use futures::task::ArcWake;

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

mod or_join {

    use std::sync::Arc;
    use std::task::{Context, Poll};

    use super::immediate_or_join;
    use futures::future::ready;
    use futures::task::waker;
    use futures::Future;

    use super::FakeWaker;
    use pin_utils::pin_mut;

    #[tokio::test]
    async fn future_completes_immediately() {
        let (_tx, rx) = swim_trigger::trigger();
        let fut = immediate_or_join(ready(()), rx);
        let (_, second_result) = fut.await;
        assert!(second_result.is_none());
    }

    #[test]
    fn first_completes_later_second_immediate() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();
        tx2.trigger();

        let fut = immediate_or_join(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, Some(Ok(_))));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn future_completes_later_second_immediate_2() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();
        tx2.trigger();

        let fut = immediate_or_join(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());
        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, Some(Ok(_))));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn future_completes_later_before_second() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_join(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        assert!(arc_wake.was_woken());

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx2.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, Some(Ok(_))));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn first_completes_later_after_second() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_join(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx2.trigger();
        assert!(arc_wake.was_woken());

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, Some(Ok(_))));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn first_completes_later_with_second() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_join(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        tx2.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, Some(Ok(_))));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn future_completes_later_with_second_2() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_join(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());
        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        tx2.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, Some(Ok(_))));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    #[should_panic]
    fn panic_when_polled_after_done() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_join(rx1, rx2);
        pin_mut!(fut);

        tx1.trigger();
        tx2.trigger();
        assert!(arc_wake.was_woken());

        assert!(fut.as_mut().poll(&mut context).is_ready());
        fut.poll(&mut context).is_ready();
    }
}

mod or_start {

    use std::sync::Arc;
    use std::task::{Context, Poll};

    use super::{immediate_or_start, SecondaryResult};
    use futures::future::ready;
    use futures::task::waker;
    use futures::Future;

    use super::FakeWaker;
    use pin_utils::pin_mut;

    #[tokio::test]
    async fn second_never_starts() {
        let (_tx, rx) = swim_trigger::trigger();
        let fut = immediate_or_start(ready(()), rx);
        let (_, second_result) = fut.await;
        assert!(matches!(second_result, SecondaryResult::NotStarted));
    }

    #[test]
    fn first_completes_later_second_immediate() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();
        tx2.trigger();

        let fut = immediate_or_start(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, SecondaryResult::Completed(Ok(_))));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn first_completes_later_second_immediate_2() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();
        tx2.trigger();

        let fut = immediate_or_start(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());
        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, SecondaryResult::Completed(Ok(_))));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn future_completes_later_before_second() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (_tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_start(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, SecondaryResult::Pending(_)));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn first_completes_later_after_second() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_start(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx2.trigger();
        assert!(arc_wake.was_woken());

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, SecondaryResult::Completed(_)));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn first_completes_later_with_second() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_start(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        tx2.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, SecondaryResult::Completed(_)));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    fn first_completes_later_with_second_2() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_start(rx1, rx2);
        pin_mut!(fut);

        assert!(fut.as_mut().poll(&mut context).is_pending());
        assert!(fut.as_mut().poll(&mut context).is_pending());

        tx1.trigger();
        tx2.trigger();
        assert!(arc_wake.was_woken());

        if let Poll::Ready((first_result, second_result)) = fut.poll(&mut context) {
            assert!(first_result.is_ok());
            assert!(matches!(second_result, SecondaryResult::Completed(_)));
        } else {
            panic!("Unexpected pending.");
        }
    }

    #[test]
    #[should_panic]
    fn panic_when_polled_after_done() {
        let arc_wake = Arc::new(FakeWaker::default());
        let waker = waker(arc_wake.clone());
        let mut context = Context::from_waker(&waker);

        let (tx1, rx1) = swim_trigger::trigger();
        let (tx2, rx2) = swim_trigger::trigger();

        let fut = immediate_or_start(rx1, rx2);
        pin_mut!(fut);

        tx1.trigger();
        tx2.trigger();
        assert!(arc_wake.was_woken());

        assert!(fut.as_mut().poll(&mut context).is_ready());
        fut.poll(&mut context).is_ready();
    }
}
