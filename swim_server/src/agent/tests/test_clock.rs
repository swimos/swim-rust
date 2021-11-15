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

use futures::future::{join, ready, BoxFuture};
use std::collections::VecDeque;
use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use swim_async_runtime::task;
use swim_async_runtime::time::clock::Clock;
use swim_utilities::trigger;
use tokio::sync::{oneshot, Mutex};
use tokio::time::{timeout, Duration};

/// A test clock where the passage of time can be manipulated monotonically.
#[derive(Debug, Clone, Default)]
pub struct TestClock {
    epoch: Arc<AtomicUsize>,
    inner: Arc<Mutex<TestClockInner>>,
}

#[derive(Debug, Default)]
struct TestClockInner {
    wakers: VecDeque<(usize, oneshot::Sender<()>)>,
    waiting: Vec<trigger::Sender>,
}

impl TestClock {
    /// Wait until a task is block waiting on the clock.
    pub async fn wait_until_blocked(&self) {
        let mut inner = self.inner.lock().await;
        if inner.wakers.is_empty() {
            let (sig, wait) = trigger::trigger();
            inner.waiting.push(sig);
            drop(inner);
            let _ = wait.await;
        }
    }

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

    /// Wait until at least one task is blocked on the task, then advance the clock.
    pub async fn advance_when_blocked(&self, duration: Duration) {
        self.wait_until_blocked().await;
        self.advance(duration).await
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
            if offset == 0 {
                panic!("The test clock only has millisecond precision.");
            }
            let now = epoch.load(Ordering::SeqCst);
            let time = now.checked_add(offset).expect("Timer overflow.");
            let contents = self.inner.clone();
            Box::pin(async move {
                let mut inner = contents.lock().await;
                let after = epoch.load(Ordering::SeqCst);
                if after < time {
                    let (tx, rx) = oneshot::channel();
                    inner.wakers.push_back((time, tx));
                    for trigger in std::mem::take(&mut inner.waiting).into_iter() {
                        trigger.trigger();
                    }
                    drop(inner);
                    let _ = rx.await;
                }
            })
        }
    }
}

#[tokio::test]
async fn check_advance() {
    let clock = TestClock::default();

    let wait = clock.delay(Duration::from_secs(1));

    //Note this is a real timeout and independent of the fake clock.
    let with_timeout = timeout(Duration::from_secs(1), wait);

    let step = clock.advance(Duration::from_secs(2));

    let (result, _) = join(with_timeout, step).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn check_under_advance() {
    let clock = TestClock::default();

    let wait = clock.delay(Duration::from_secs(1));

    //Note this is a real timeout and independent of the fake clock.
    let with_timeout = timeout(Duration::from_millis(100), wait);

    let step = clock.advance(Duration::from_millis(500));

    let (result, _) = join(with_timeout, step).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn wait_for_block() {
    let clock = TestClock::default();

    //Note this is a real timeout and independent of the fake clock.
    let wait_task = task::spawn(timeout(
        Duration::from_secs(1),
        clock.delay(Duration::from_secs(1)),
    ));

    clock.wait_until_blocked().await;
    clock.advance(Duration::from_secs(2)).await;

    let result = wait_task.await;
    assert!(matches!(result, Ok(Ok(()))));
}
