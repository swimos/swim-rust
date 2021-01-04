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
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use swim_common::sink::item::for_mpsc_sender;
use swim_runtime::time::timeout::timeout;
use tokio::sync::{mpsc, Barrier};
use utilities::sync::circular_buffer;

const TIMEOUT: Duration = Duration::from_secs(30);

fn yield_after() -> NonZeroUsize {
    NonZeroUsize::new(256).unwrap()
}

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(2).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn single_pass_through() {
    let (mut buf_tx, buf_rx) = circular_buffer::channel(buffer_size());
    let (tx, mut rx) = mpsc::channel(5);

    let task = super::release_pressure(buf_rx, for_mpsc_sender(tx), yield_after());
    let handle = tokio::task::spawn(task);

    let receiver = tokio::task::spawn(async move { rx.recv().await.unwrap() });

    assert!(buf_tx.try_send(6).is_ok());

    let value = timeout(TIMEOUT, receiver).await.unwrap().unwrap();
    assert_eq!(value, 6);

    drop(buf_tx);
    assert!(matches!(handle.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn send_multiple() {
    let (mut buf_tx, buf_rx) = circular_buffer::channel(buffer_size());
    let (tx, rx) = mpsc::channel(5);

    let task = super::release_pressure(buf_rx, for_mpsc_sender(tx), yield_after());
    let handle = tokio::task::spawn(task);

    let receiver = tokio::task::spawn(rx.collect::<Vec<_>>());

    for n in 0..10 {
        assert!(buf_tx.try_send(n).is_ok());
    }
    drop(buf_tx);

    let result = timeout(TIMEOUT, receiver).await;
    assert!(matches!(result, Ok(Ok(_))));
    let received = result.unwrap().unwrap();
    assert!(received.len() <= 10);
    let mut prev = None;
    for i in received.into_iter() {
        if let Some(p) = prev {
            assert!(p < i);
        }
        prev = Some(i);
    }
    assert_eq!(prev, Some(9));

    assert!(matches!(handle.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn send_multiple_chunks() {
    let (mut buf_tx, buf_rx) = circular_buffer::channel(buffer_size());
    let (tx, mut rx) = mpsc::channel(5);

    let task = super::release_pressure(buf_rx, for_mpsc_sender(tx), yield_after());
    let handle = tokio::task::spawn(task);

    let barrier_tx = Arc::new(Barrier::new(2));
    let barrier_rx = barrier_tx.clone();

    let receiver = tokio::task::spawn(async move {
        let mut results = vec![];
        while let Some(value) = rx.next().await {
            results.push(value);
            if value == 9 {
                barrier_rx.wait().await;
            }
        }
        results
    });

    for n in 0..10 {
        assert!(buf_tx.try_send(n).is_ok());
    }
    barrier_tx.wait().await;
    for n in 10..20 {
        assert!(buf_tx.try_send(n).is_ok());
    }

    drop(buf_tx);

    let result = timeout(TIMEOUT, receiver).await;
    assert!(matches!(result, Ok(Ok(_))));
    let received = result.unwrap().unwrap();
    assert!(received.len() <= 20);
    let mut prev = None;
    let mut seen9 = false;
    for i in received.into_iter() {
        if i == 9 {
            seen9 = true;
        }
        if let Some(p) = prev {
            assert!(p < i);
        }
        prev = Some(i);
    }
    assert!(seen9);
    assert_eq!(prev, Some(19));

    assert!(matches!(handle.await, Ok(Ok(_))));
}
