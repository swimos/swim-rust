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

use super::*;
use crate::agent::lane::tests::ExactlyOnce;
use futures::{FutureExt, StreamExt};
use stm::transaction::atomically;
use tokio::sync::mpsc::error::TryRecvError;

#[tokio::test]
async fn value_lane_get() {
    let (lane, mut events) = make_lane_model(0, Queue::default());

    let result = atomically(&lane.get(), ExactlyOnce).await;

    assert!(matches!(result, Ok(v) if v == Arc::new(0)));

    let event = events.try_recv();
    assert!(matches!(event, Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn value_lane_set_queue() {
    let (lane, mut events) = make_lane_model(0, Queue::default());

    let result = atomically(&lane.set(1), ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.recv().await;
    assert!(matches!(event, Some(v) if *v == 1));
}

#[tokio::test]
async fn value_lane_set_dropping() {
    let (lane, mut events) = make_lane_model(3, Dropping);

    let init_event = events.recv().await;
    assert!(matches!(init_event, Some(v) if *v == 3));

    let result = atomically(&lane.set(7), ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.recv().await;
    assert!(matches!(event, Some(v) if *v == 7));
}

#[tokio::test]
async fn value_lane_set_buffered() {
    let (lane, mut events) = make_lane_model(0, Buffered::default());

    let result = atomically(&lane.set(1), ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(v) if *v == 1));
}

#[tokio::test]
async fn value_lane_compound_transaction_queue() {
    let (lane, mut events) = make_lane_model(5, Queue::default());

    let stm = lane
        .get()
        .and_then(|i| lane.set(-1).followed_by(lane.set(*i + 1)));

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.recv().await;
    assert!(matches!(event, Some(v) if *v == 6));

    let event2 = events.try_recv();
    assert!(matches!(event2, Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn value_lane_compound_transaction_dropping() {
    let (lane, mut events) = make_lane_model(5, Dropping);

    let init_event = events.recv().await;
    assert!(matches!(init_event, Some(v) if *v == 5));

    let stm = lane
        .get()
        .and_then(|i| lane.set(-1).followed_by(lane.set(*i + 1)));

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.recv().await;
    assert!(matches!(event, Some(v) if *v == 6));

    let event2 = events.next().now_or_never();
    assert!(event2.is_none());
}

#[tokio::test]
async fn value_lane_compound_transaction_buffered() {
    let (lane, mut events) = make_lane_model(5, Buffered::default());

    let stm = lane
        .get()
        .and_then(|i| lane.set(-1).followed_by(lane.set(*i + 1)));

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(result.is_ok());

    let event = events.next().await;
    assert!(matches!(event, Some(v) if *v == 6));

    let event2 = events.next().now_or_never();
    assert!(event2.is_none());
}
