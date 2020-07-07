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

use futures::future::{ready, BoxFuture};
use std::collections::VecDeque;
use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::time::Duration;
use utilities::clock::Clock;

/// A test clock where the passage of time can be manipulated monotonically.
#[derive(Debug, Clone, Default)]
pub struct TestClock {
    epoch: Arc<AtomicUsize>,
    inner: Arc<Mutex<TestClockInner>>,
}

#[derive(Debug, Default)]
struct TestClockInner {
    wakers: VecDeque<(usize, oneshot::Sender<()>)>,
}

impl TestClock {
    /// Advance the clock by the specified duration.
    pub async fn advance(&self, duration: Duration) {
        let offset: usize = duration.as_millis().try_into().expect("Timer overflow.");
        let new_epoch = self
            .epoch
            .fetch_add(offset, Ordering::SeqCst)
            .checked_add(offset)
            .expect("Timer overflow.");
        let mut inner = self.inner.lock().await;
        loop {
            match inner.wakers.pop_front() {
                Some((t, tx)) if t <= new_epoch => {
                    let _ = tx.send(());
                }
                Some(ow) => {
                    inner.wakers.push_front(ow);
                    break;
                }
                _ => {
                    break;
                }
            }
        }
    }
}

impl Clock for TestClock {
    type DelayFuture = BoxFuture<'static, ()>;

    fn delay(&self, duration: Duration) -> Self::DelayFuture {
        if duration == Duration::from_nanos(0) {
            Box::pin(ready(()))
        } else {
            let epoch = self.epoch.clone();
            let offset: usize = duration.as_millis().try_into().expect("Timer overflow.");
            let now = epoch.load(Ordering::SeqCst);
            let time = now.checked_add(offset).expect("Timer overflow.");
            let contents = self.inner.clone();
            Box::pin(async move {
                let mut inner = contents.lock().await;
                let after = epoch.load(Ordering::SeqCst);
                if after < time {
                    let (tx, rx) = oneshot::channel();
                    inner.wakers.push_back((time, tx));

                    let _ = rx.await;
                }
            })
        }
    }
}
