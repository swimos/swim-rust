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

use std::sync::Arc;
use std::time::Duration;
use std::future::Future;
use futures::future::join;
use swim_utilities::future::SwimFutureExt;
use tokio::sync::Notify;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_millis(100);

async fn with_timeout<Fut>(test_case: Fut) -> Fut::Output
where
    Fut: Future,
{
    if let Ok(value) = timeout(TIMEOUT, test_case).await {
        value
    } else {
        panic!("Test case timed out.");
    }
}

#[tokio::test]
async fn complete_immediately() {

    with_timeout(async {
        let (tx1, tx2, rx) = super::timeout_coordinator();
        assert!(!tx1.vote());
        assert!(tx2.vote());

        rx.await;
    }).await;

}

#[tokio::test]
async fn complete_async() {
    let (tx1, tx2, rx) = super::timeout_coordinator();

    let notify = Arc::new(Notify::new());

    let wait_task = rx.notify_on_blocked(notify.clone());

    let vote_task = async move {
        notify.notified().await;
        assert!(!tx1.vote());
        assert!(tx2.vote());
    };

    with_timeout(join(wait_task, vote_task)).await;

}

#[tokio::test]
async fn complete_async2() {
    let (tx1, tx2, rx) = super::timeout_coordinator();

    let notify = Arc::new(Notify::new());

    let wait_task = rx.notify_on_blocked(notify.clone());

    let vote_task = async move {
        notify.notified().await;
        assert!(!tx2.vote());
        assert!(tx1.vote());
    };

    with_timeout(join(wait_task, vote_task)).await;

}

#[tokio::test]
async fn complete_async_wait_between_votes() {
    let (tx1, tx2, rx) = super::timeout_coordinator();

    let notify1 = Arc::new(Notify::new());
    let notify1_cpy = notify1.clone();
    let notify2 = Arc::new(Notify::new());
    let notify2_cpy = notify2.clone();

    let wait_task = async move {
        notify1.notified().await;
        rx.notify_on_blocked(notify2).await;
    };

    let vote_task = async move {
        assert!(!tx1.vote());
        notify1_cpy.notify_one();
        notify2_cpy.notified().await;
        assert!(tx2.vote());
    };

    with_timeout(join(wait_task, vote_task)).await;

}

#[tokio::test]
async fn rescind_vote() {
    let (tx1, tx2, rx) = super::timeout_coordinator();

    let notify1 = Arc::new(Notify::new());
    let notify1_cpy = notify1.clone();
    let notify2 = Arc::new(Notify::new());
    let notify2_cpy = notify2.clone();

    let wait_task = async move {
        notify1.notified().await;
        rx.notify_on_blocked(notify2).await;
    };

    let vote_task = async move {
        assert!(!tx1.vote());
        assert!(!tx1.rescind());
        assert!(!tx2.vote());
        notify1_cpy.notify_one();
        notify2_cpy.notified().await;
        assert!(tx1.vote());
    };

    with_timeout(join(wait_task, vote_task)).await;

}

#[tokio::test]
async fn cannot_rescind_after_unanimity() {

    with_timeout(async {
        let (tx1, tx2, rx) = super::timeout_coordinator();
        assert!(!tx1.vote());
        assert!(tx2.vote());
        assert!(tx1.rescind());

        rx.await;
    }).await;

}

