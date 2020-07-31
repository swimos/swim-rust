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

use crate::agent::lane::strategy::ChannelObserver;
use std::sync::Arc;
use stm::var::observer::Observer;
use tokio::sync::{broadcast, mpsc, watch};

#[tokio::test]
async fn channel_observer_send_mpsc() {
    let (tx, mut rx) = mpsc::channel(5);
    let mut observer = ChannelObserver::new(tx);

    let value = Arc::new(4);

    observer.notify(value.clone()).await;

    let received = rx.recv().await;

    assert!(matches!(received, Some(v) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn channel_observer_send_after_drop_mpsc() {
    let (tx, rx) = mpsc::channel(5);
    let mut observer = ChannelObserver::new(tx);

    drop(rx);

    let value = Arc::new(4);

    //No assertion is needed as we merely need to ensure that this does not panic.
    observer.notify(value).await;
}

#[tokio::test]
async fn channel_observer_send_watch() {
    let (tx, mut rx) = watch::channel(Arc::new(0));
    let mut observer = ChannelObserver::new(tx);

    let init = rx.recv().await;
    assert!(matches!(init, Some(v) if *v == 0));

    let value = Arc::new(4);

    observer.notify(value.clone()).await;

    let received = rx.recv().await;

    assert!(matches!(received, Some(v) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn channel_observer_send_after_drop_watch() {
    let (tx, mut rx) = watch::channel(Arc::new(0));
    let mut observer = ChannelObserver::new(tx);

    let init = rx.recv().await;
    assert!(matches!(init, Some(v) if *v == 0));

    drop(rx);

    let value = Arc::new(4);

    //No assertion is needed as we merely need to ensure that this does not panic.
    observer.notify(value).await;
}

#[tokio::test]
async fn channel_observer_send_broadcast() {
    let (tx, mut rx) = broadcast::channel(5);
    let mut observer = ChannelObserver::new(tx);

    let value = Arc::new(4);

    observer.notify(value.clone()).await;

    let received = rx.recv().await;

    assert!(matches!(received, Ok(v) if Arc::ptr_eq(&v, &value)));
}

#[tokio::test]
async fn channel_observer_send_after_drop_broadcast() {
    let (tx, rx) = broadcast::channel(5);
    let mut observer = ChannelObserver::new(tx);

    drop(rx);

    let value = Arc::new(4);

    //No assertion is needed as we merely need to ensure that this does not panic.
    observer.notify(value).await;
}
