use futures::task;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Waker;

pub fn make_waker() -> (Arc<TestWaker>, Waker) {
    let test_waker = Arc::new(TestWaker::default());
    let waker = task::waker(test_waker.clone());
    (test_waker, waker)
}

#[derive(Default, Debug)]
pub struct TestWaker(AtomicBool);

impl TestWaker {
    pub fn woken(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

impl task::ArcWake for TestWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::SeqCst);
    }
}
