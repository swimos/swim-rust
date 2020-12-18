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

use futures::StreamExt;
use std::sync::Arc;
use stm::var::observer::Observer;
use tokio::sync::{broadcast, mpsc, oneshot};

#[tokio::test]
async fn channel_observer_send_mpsc() {
    let (tx, mut rx) = mpsc::channel(5);
    let mut observer: Observer<i32> = tx.into();

    let value = Arc::new(4);

    observer.notify(value.clone()).await;

    let received = rx.recv().await;

    assert!(matches!(received, Some(v) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn channel_observer_send_after_drop_mpsc() {
    let (tx, rx) = mpsc::channel(5);
    let mut observer: Observer<i32> = tx.into();

    drop(rx);

    let value = Arc::new(4);

    //No assertion is needed as we merely need to ensure that this does not panic.
    observer.notify(value).await;
}

#[tokio::test]
async fn channel_observer_send_broadcast() {
    let (tx, mut rx) = broadcast::channel(5);
    let mut observer: Observer<i32> = tx.into();

    let value = Arc::new(4);

    observer.notify(value.clone()).await;

    let received = rx.recv().await;

    assert!(matches!(received, Ok(v) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn channel_observer_send_after_drop_broadcast() {
    let (tx, rx) = broadcast::channel(5);
    let mut observer: Observer<i32> = tx.into();

    drop(rx);

    let value = Arc::new(4);

    //No assertion is needed as we merely need to ensure that this does not panic.
    observer.notify(value).await;
}

#[tokio::test]
async fn deferred_mpsc_observer_nominal() {
    let (prim_tx, _prim_rx) = mpsc::channel(8);
    let (channel_tx, channel_rx) = oneshot::channel();

    let mut observer = Observer::new_with_deferred(prim_tx.into(), channel_rx);

    let v1 = Arc::new(7);

    observer.notify(v1).await;

    let (tx, mut rx) = mpsc::channel(8);
    assert!(channel_tx.send(tx.into()).is_ok());

    let v2 = Arc::new(13);
    observer.notify(v2.clone()).await;

    assert!(matches!(rx.try_recv(), Ok(v) if Arc::ptr_eq(&v, &v2)));
}

#[tokio::test]
async fn deferred_mpsc_dropped_sender() {
    let (prim_tx, prim_rx) = mpsc::channel(8);
    let (channel_tx, channel_rx) = oneshot::channel();

    let mut observer = Observer::new_with_deferred(prim_tx.into(), channel_rx);

    let v1 = Arc::new(7);

    observer.notify(v1.clone()).await;

    drop(channel_tx);

    let v2 = Arc::new(13);
    observer.notify(v2.clone()).await;

    drop(observer);
    let received: Vec<Arc<i32>> = prim_rx.collect().await;
    assert!(
        matches!(received.as_slice(), [r1, r2] if Arc::ptr_eq(r1, &v1) && Arc::ptr_eq(r2, &v2))
    );
}
