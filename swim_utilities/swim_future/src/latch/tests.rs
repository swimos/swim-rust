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

use futures::future::join;
use tokio::time::timeout;
use tokio::sync::Notify;
use std::sync::Arc;
use std::time::Duration;
use crate::SwimFutureExt;

const TIMEOUT: Duration = Duration::from_millis(100);

#[tokio::test]
async fn wait_on_zero() {
    let (_counters, on_done) = super::make_latch::<0>();
    assert!(timeout(TIMEOUT, on_done).await.is_ok());
}

#[tokio::test]
async fn wait_on_one_immediate() {
    let ([counter], on_done) = super::make_latch::<1>();
    drop(counter);
    assert!(timeout(TIMEOUT, on_done).await.is_ok());
}

#[tokio::test]
async fn wait_on_one_delayed() {
    let ([counter], on_done) = super::make_latch::<1>();
    
    let notify = Arc::new(Notify::new());
    
    let done_fut = on_done.notify_on_blocked(notify.clone());

    let countdown_fut = async move {
        notify.notified().await;
        drop(counter);
    };

    assert!(timeout(TIMEOUT, join(done_fut, countdown_fut)).await.is_ok());

}

#[tokio::test]
async fn wait_on_two_immediate() {
    let (counters, on_done) = super::make_latch::<2>();
    drop(counters);
    assert!(timeout(TIMEOUT, on_done).await.is_ok());
}

#[tokio::test]
async fn wait_on_two_single_delayed() {
    let ([counter1, counter2], on_done) = super::make_latch::<2>();
    drop(counter1);

    let notify = Arc::new(Notify::new());
    
    let done_fut = on_done.notify_on_blocked(notify.clone());

    let countdown_fut = async move {
        notify.notified().await;
        drop(counter2);
    };

    assert!(timeout(TIMEOUT, join(done_fut, countdown_fut)).await.is_ok());
}

#[tokio::test]
async fn wait_on_two_both_delayed() {
    let ([counter1, counter2], on_done) = super::make_latch::<2>();
    
    let notify = Arc::new(Notify::new());
    
    let done_fut = on_done.notify_on_blocked(notify.clone());

    let countdown_fut = async move {
        notify.notified().await;
        drop(counter2);
        drop(counter1);
    };

    assert!(timeout(TIMEOUT, join(done_fut, countdown_fut)).await.is_ok());
}