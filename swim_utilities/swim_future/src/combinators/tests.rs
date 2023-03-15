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

use crate::{try_last, NotifyOnBlocked, StopAfterError};

use futures::executor::block_on;
use futures::future::{join, select, Either};
use futures::stream::{self, iter};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use swim_trigger::trigger;
use tokio::sync::Notify;
use tokio::time::timeout;

#[test]
fn stop_after_error() {
    let inputs = iter(vec![Ok(0), Ok(1), Ok(2), Err("Boom!"), Ok(4), Err("Boom!")].into_iter());
    let outputs = block_on(StopAfterError::new(inputs).collect::<Vec<_>>());
    assert_eq!(outputs, vec![Ok(0), Ok(1), Ok(2), Err("Boom!")]);
}

#[tokio::test]
async fn stream_notify_on_blocked() {
    let (tx, rx) = trigger();
    let notify = Arc::new(Notify::new());
    let notify_cpy = notify.clone();

    let blocker = async move {
        let mut stream = NotifyOnBlocked::new(stream::pending::<()>(), notify_cpy);
        let result = select(stream.next(), rx).await;
        assert!(matches!(result, Either::Right((Ok(_), _))));
    };

    let unblocker = async move {
        notify.notified().await;
        tx.trigger();
    };

    let result = timeout(Duration::from_secs(5), join(blocker, unblocker)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn try_last_empty() {
    let stream = futures::stream::empty::<Result<(), ()>>();
    assert!(matches!(try_last(stream).await, Ok(None)));
}

#[tokio::test]
async fn try_last_all_good() {
    let values: Vec<Result<i32, ()>> = vec![Ok(1), Ok(2), Ok(3)];
    let stream = futures::stream::iter(values);
    assert_eq!(try_last(stream).await, Ok(Some(3)));
}

#[tokio::test]
async fn stop_on_error() {
    let values: Vec<Result<i32, ()>> = vec![Ok(1), Err(()), Ok(3)];
    let stream = futures::stream::iter(values);
    assert!(try_last(stream).await.is_err());
}
