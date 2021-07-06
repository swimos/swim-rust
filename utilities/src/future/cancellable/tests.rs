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

use crate::future::cancellable::{Cancellable, CancellableResult};
use crate::sync::trigger;
use std::time::Duration;
use tokio::time;

#[tokio::test]
async fn test_cancellable_completed() {
    let main_future = async {
        time::sleep(Duration::from_millis(10)).await;
        10
    };
    let (_cancel_tx, cancel_rx) = trigger::trigger();
    let cancellable = Cancellable::new(main_future, cancel_rx);

    let result = cancellable.await;

    assert_eq!(result, CancellableResult::Completed(10));
}

#[tokio::test]
async fn test_cancellable_cancelled() {
    let main_future = async {
        time::sleep(Duration::from_millis(10)).await;
        10
    };
    let (cancel_tx, cancel_rx) = trigger::trigger();
    let cancellable = Cancellable::new(main_future, cancel_rx);

    cancel_tx.trigger();
    let result = cancellable.await;

    assert_eq!(result, CancellableResult::Cancelled(Ok(())));
}

#[tokio::test]
async fn test_cancellable_instant_completion() {
    let main_future = async { 10 };
    let (cancel_tx, cancel_rx) = trigger::trigger();
    let cancellable = Cancellable::new(main_future, cancel_rx);

    cancel_tx.trigger();
    let result = cancellable.await;

    assert_eq!(result, CancellableResult::Completed(10));
}
