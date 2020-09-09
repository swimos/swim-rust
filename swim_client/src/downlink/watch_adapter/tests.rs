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

#[tokio::test]
async fn send_and_receive() {
    let (mut tx, mut rx) = super::channel(0);
    let first = rx.recv().await;
    assert_eq!(first, Some(0));

    let handle = tokio::task::spawn(async move {
        let mut rx = rx;
        rx.recv().await
    });

    assert!(tx.broadcast(3).is_ok());

    let second = handle.await.unwrap();
    assert_eq!(second, Some(3));
}

#[tokio::test]
async fn receive_defined() {
    let (mut tx, rx) = super::channel::<Option<i32>>(None);

    let handle = tokio::task::spawn(async move {
        let mut rx = rx;
        rx.recv_defined().await
    });

    assert!(tx.broadcast(None).is_ok());
    assert!(tx.broadcast(Some(42)).is_ok());

    let first = handle.await.unwrap();
    assert_eq!(first, Some(42));
}

#[tokio::test]
async fn receive_stream() {
    let (tx, rx) = super::channel(-4);

    let handle = tokio::task::spawn(async move { rx.collect::<Vec<_>>().await });
    drop(tx);

    let output = handle.await.unwrap();
    assert_eq!(output, vec![-4]);
}

#[tokio::test]
async fn in_order_no_duplicates_recv() {
    let (mut tx, rx) = super::channel(0);

    let handle = tokio::task::spawn(async move {
        let mut rx = rx;
        let mut results = vec![];
        while let Some(n) = rx.recv().await {
            results.push(n);
        }
        results
    });
    for i in 1..1000 {
        assert!(tx.broadcast(i).is_ok());
    }
    drop(tx);

    let output = handle.await.unwrap();

    assert_ne!(output.len(), 0);

    let mut prev = None;
    for n in output.into_iter() {
        if let Some(p) = prev {
            assert!(p < n);
        }
        prev = Some(n);
    }
}

#[tokio::test]
async fn in_order_no_duplicates_stream() {
    let (mut tx, rx) = super::channel(0);

    let handle = tokio::task::spawn(async move { rx.collect::<Vec<_>>().await });
    for i in 1..1000 {
        assert!(tx.broadcast(i).is_ok());
    }
    drop(tx);

    let output = handle.await.unwrap();
    assert_ne!(output.len(), 0);

    let mut prev = None;
    for n in output.into_iter() {
        if let Some(p) = prev {
            assert!(p < n);
        }
        prev = Some(n);
    }
}
