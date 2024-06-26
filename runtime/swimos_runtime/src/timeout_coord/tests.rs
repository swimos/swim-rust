// Copyright 2015-2024 Swim Inc.
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

use futures::future::join;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use swimos_utilities::future::NotifyOnBlocked;
use tokio::sync::Notify;
use tokio::time::timeout;

use crate::timeout_coord::VoteResult;

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
        let ([tx1, tx2], rx) = super::multi_party_coordinator::<2>();
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::Unanimous);

        rx.await;
    })
    .await;
}

#[tokio::test]
async fn complete_async() {
    let ([tx1, tx2], rx) = super::multi_party_coordinator::<2>();

    let notify = Arc::new(Notify::new());

    let wait_task = NotifyOnBlocked::new(rx, notify.clone());

    let vote_task = async move {
        notify.notified().await;
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn complete_async2() {
    let ([tx1, tx2], rx) = super::multi_party_coordinator::<2>();

    let notify = Arc::new(Notify::new());

    let wait_task = NotifyOnBlocked::new(rx, notify.clone());

    let vote_task = async move {
        notify.notified().await;
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx1.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn complete_async_wait_between_votes() {
    let ([tx1, tx2], rx) = super::multi_party_coordinator::<2>();

    let notify1 = Arc::new(Notify::new());
    let notify1_cpy = notify1.clone();
    let notify2 = Arc::new(Notify::new());
    let notify2_cpy = notify2.clone();

    let wait_task = async move {
        notify1.notified().await;
        NotifyOnBlocked::new(rx, notify2).await;
    };

    let vote_task = async move {
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        notify1_cpy.notify_one();
        notify2_cpy.notified().await;
        assert_eq!(tx2.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn rescind_vote() {
    let ([tx1, tx2], rx) = super::multi_party_coordinator::<2>();

    let notify1 = Arc::new(Notify::new());
    let notify1_cpy = notify1.clone();
    let notify2 = Arc::new(Notify::new());
    let notify2_cpy = notify2.clone();

    let wait_task = async move {
        notify1.notified().await;
        NotifyOnBlocked::new(rx, notify2).await;
    };

    let vote_task = async move {
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx1.rescind(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        notify1_cpy.notify_one();
        notify2_cpy.notified().await;
        assert_eq!(tx1.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn cannot_rescind_after_unanimity() {
    with_timeout(async {
        let ([tx1, tx2], rx) = super::multi_party_coordinator::<2>();
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::Unanimous);
        assert_eq!(tx1.rescind(), VoteResult::Unanimous);

        rx.await;
    })
    .await;
}

#[tokio::test]
async fn drop_causes_vote() {
    with_timeout(async {
        let ([tx1, tx2], rx) = super::multi_party_coordinator::<2>();
        drop(tx1);
        assert_eq!(tx2.vote(), VoteResult::Unanimous);

        rx.await;
    })
    .await;
}

#[tokio::test]
async fn dropping_both_is_unanimity() {
    with_timeout(async {
        let ([tx1, tx2], rx) = super::multi_party_coordinator::<2>();
        drop(tx1);
        drop(tx2);

        rx.await;
    })
    .await;
}

#[tokio::test]
async fn complete_immediately_three() {
    with_timeout(async {
        let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx3.vote(), VoteResult::Unanimous);

        rx.await;
    })
    .await;
}

#[tokio::test]
async fn complete_async_three() {
    let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();

    let notify = Arc::new(Notify::new());

    let wait_task = NotifyOnBlocked::new(rx, notify.clone());

    let vote_task = async move {
        notify.notified().await;
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx3.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn complete_async2_three() {
    let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();

    let notify = Arc::new(Notify::new());

    let wait_task = NotifyOnBlocked::new(rx, notify.clone());

    let vote_task = async move {
        notify.notified().await;
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx3.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn complete_async3_three() {
    let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();

    let notify = Arc::new(Notify::new());

    let wait_task = NotifyOnBlocked::new(rx, notify.clone());

    let vote_task = async move {
        notify.notified().await;
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx3.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx1.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn complete_async4_three() {
    let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();

    let notify = Arc::new(Notify::new());

    let wait_task = NotifyOnBlocked::new(rx, notify.clone());

    let vote_task = async move {
        notify.notified().await;
        assert_eq!(tx3.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx1.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn complete_async_wait_between_votes_three() {
    let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();

    let notify1 = Arc::new(Notify::new());
    let notify1_cpy = notify1.clone();
    let notify2 = Arc::new(Notify::new());
    let notify2_cpy = notify2.clone();

    let wait_task = async move {
        notify1.notified().await;
        NotifyOnBlocked::new(rx, notify2).await;
    };

    let vote_task = async move {
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        notify1_cpy.notify_one();
        notify2_cpy.notified().await;
        assert_eq!(tx3.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn rescind_vote_three() {
    let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();

    let notify1 = Arc::new(Notify::new());
    let notify1_cpy = notify1.clone();
    let notify2 = Arc::new(Notify::new());
    let notify2_cpy = notify2.clone();

    let wait_task = async move {
        notify1.notified().await;
        NotifyOnBlocked::new(rx, notify2).await;
    };

    let vote_task = async move {
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx3.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx1.rescind(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        notify1_cpy.notify_one();
        notify2_cpy.notified().await;
        assert_eq!(tx1.vote(), VoteResult::Unanimous);
    };

    with_timeout(join(wait_task, vote_task)).await;
}

#[tokio::test]
async fn cannot_rescind_after_unanimity_three() {
    with_timeout(async {
        let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();
        assert_eq!(tx1.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        assert_eq!(tx3.vote(), VoteResult::Unanimous);
        assert_eq!(tx1.rescind(), VoteResult::Unanimous);

        rx.await;
    })
    .await;
}

#[tokio::test]
async fn drop_causes_vote_three() {
    with_timeout(async {
        let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();
        assert_eq!(tx2.vote(), VoteResult::UnanimityPending);
        drop(tx1);
        assert_eq!(tx3.vote(), VoteResult::Unanimous);

        rx.await;
    })
    .await;
}

#[tokio::test]
async fn dropping_all_is_unanimity_three() {
    with_timeout(async {
        let ([tx1, tx2, tx3], rx) = super::multi_party_coordinator::<3>();
        drop(tx1);
        drop(tx2);
        drop(tx3);

        rx.await;
    })
    .await;
}
