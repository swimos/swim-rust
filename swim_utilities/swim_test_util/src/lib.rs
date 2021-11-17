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
