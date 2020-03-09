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
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use tokio::sync::watch;
use futures::StreamExt;

#[tokio::test]
pub async fn receive_from_watch_topic() {
    let (tx, rx) = watch::channel(None);
    let (mut topic, mut rx1) = WatchTopic::new(rx);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let mut rx2 = maybe_rx.unwrap();

    let send_result = tx.broadcast(Some(5));
    assert_that!(&send_result, ok());

    let n1 = rx1.next().await;
    let n2 = rx2.next().await;

    assert_that!(n1, eq(Some(5)));
    assert_that!(n2, eq(Some(5)));
}

#[tokio::test]
pub async fn miss_record_from_watch_topic() {
    let (tx, rx) = watch::channel(None);
    let (mut topic, mut rx1) = WatchTopic::new(rx);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let mut rx2 = maybe_rx.unwrap();

    let send_result1 = tx.broadcast(Some(5));
    assert_that!(&send_result1, ok());

    let n1 = rx1.next().await;

    let send_result2 = tx.broadcast(Some(10));
    assert_that!(&send_result2, ok());

    let n2 = rx2.next().await;

    assert_that!(n1, eq(Some(5)));
    assert_that!(n2, eq(Some(10)));
}

#[tokio::test]
pub async fn single_receiver_dropped_for_watch_topic() {
    let (tx, rx) = watch::channel(None);
    let (mut topic, rx1) = WatchTopic::new(rx);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let mut rx2 = maybe_rx.unwrap();

    drop(rx1);

    let send_result = tx.broadcast(Some(5));
    assert_that!(&send_result, ok());

    let n2 = rx2.next().await;

    assert_that!(n2, eq(Some(5)));
}

#[tokio::test]
pub async fn all_receivers_dropped_for_watch_topic() {
    let (tx, rx) = watch::channel(None);
    let (mut topic, rx1) = WatchTopic::new(rx);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let rx2 = maybe_rx.unwrap();

    drop(rx1);
    drop(rx2);

    let bad_rx = topic.subscribe().await;
    assert_that!(bad_rx, err());

    let send_result = tx.broadcast(Some(5));
    assert_that!(send_result, err());

}

#[tokio::test]
pub async fn receive_from_broadcast_topic() {
    let (mut topic, tx, mut rx1) =
        BroadcastTopic::new(2);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let mut rx2 = maybe_rx.unwrap();

    let send_result = tx.send(5);
    assert_that!(&send_result, ok());

    let n1 = rx1.next().await;
    let n2 = rx2.next().await;

    assert_that!(n1, eq(Some(5)));
    assert_that!(n2, eq(Some(5)));
}

#[tokio::test]
pub async fn receive_multiple_broadcast_topic() {
    let (mut topic, tx, rx1) =
        BroadcastTopic::new(2);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let rx2 = maybe_rx.unwrap();

    let send_result = tx.send(5);
    assert_that!(&send_result, ok());
    let send_result = tx.send(10);
    assert_that!(&send_result, ok());

    let results1 = rx1.take(2).collect::<Vec<_>>().await;
    let results2 = rx2.take(2).collect::<Vec<_>>().await;

    assert_that!(results1, eq(vec![5, 10]));
    assert_that!(results2, eq(vec![5, 10]));
}

#[tokio::test]
pub async fn miss_record_from_broadcast_topic() {
    let (mut topic, tx, mut rx1) =
        BroadcastTopic::new(2);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let rx2 = maybe_rx.unwrap();

    let send_result = tx.send(5);
    assert_that!(&send_result, ok());

    let first = rx1.next().await;

    assert_that!(first, eq(Some(5)));

    let send_result = tx.send(10);
    assert_that!(&send_result, ok());
    let send_result = tx.send(15);
    assert_that!(&send_result, ok());

    let results1 = rx1.take(2).collect::<Vec<_>>().await;

    let results2 = rx2.take(2).collect::<Vec<_>>().await;

    assert_that!(results1, eq(vec![10, 15]));
    //The second receiver never observed 5.
    assert_that!(results2, eq(vec![10, 15]));
}

#[tokio::test]
pub async fn single_receiver_dropped_for_broadcast_topic() {
    let (mut topic, tx, rx1) =
        BroadcastTopic::new(2);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let mut rx2 = maybe_rx.unwrap();

    drop(rx1);

    let send_result = tx.send(5);
    assert_that!(&send_result, ok());

    let n2 = rx2.next().await;

    assert_that!(n2, eq(Some(5)));
}

#[tokio::test]
pub async fn all_receivers_dropped_for_broadcast_topic() {
    let (mut topic, tx, rx1) =
        BroadcastTopic::new(2);

    let  maybe_rx = topic.subscribe().await;
    assert_that!(&maybe_rx, ok());
    let rx2 = maybe_rx.unwrap();

    drop(rx1);
    drop(rx2);

    let bad_rx = topic.subscribe().await;
    assert_that!(bad_rx, err());

    let send_result = tx.send(5);
    assert_that!(send_result, err());
}

