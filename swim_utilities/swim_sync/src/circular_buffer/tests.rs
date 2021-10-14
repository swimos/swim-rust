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

use crate::circular_buffer::error::{RecvError, SendError};
use crate::circular_buffer::{InternalQueue, OneItemQueue, LARGE_BOUNDARY};
use futures::task::ArcWake;
use futures::StreamExt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::Barrier;

#[test]
fn one_item_queue() {
    let queue = OneItemQueue::new();
    assert!(queue.push_value(1).is_ok());
    assert_eq!(queue.push_value(2), Err(2));

    assert_eq!(queue.pop_value(), Some(1));
    assert!(queue.pop_value().is_none());
    assert!(queue.push_value(2).is_ok());
}

async fn send_and_receive(n: NonZeroUsize) {
    let (mut tx, mut rx) = super::channel(n);

    let send_task = async move {
        assert!(tx.try_send(4).is_ok());
    };

    let recv_task = async move {
        assert_eq!(rx.recv().await, Ok(4));
    };

    let send_handle = tokio::spawn(send_task);
    let recv_handle = tokio::spawn(recv_task);

    assert!(send_handle.await.is_ok());
    assert!(recv_handle.await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_send_and_receive() {
    send_and_receive(NonZeroUsize::new(1).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn small_send_and_receive() {
    send_and_receive(NonZeroUsize::new(5).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_send_and_receive() {
    send_and_receive(NonZeroUsize::new(LARGE_BOUNDARY + 1).unwrap()).await;
}

async fn receive_after_sender_dropped(n: NonZeroUsize) {
    let (mut tx, mut rx) = super::channel(n);
    let barrier_send = Arc::new(Barrier::new(2));
    let barrier_receive = barrier_send.clone();

    let send_task = async move {
        assert!(tx.try_send(4).is_ok());
        drop(tx);
        barrier_send.wait().await;
    };

    let recv_task = async move {
        barrier_receive.wait().await;
        assert_eq!(rx.recv().await, Ok(4));
        assert_eq!(rx.recv().await, Err(RecvError));
    };

    let send_handle = tokio::spawn(send_task);
    let recv_handle = tokio::spawn(recv_task);

    assert!(send_handle.await.is_ok());
    assert!(recv_handle.await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_receive_after_sender_dropped() {
    receive_after_sender_dropped(NonZeroUsize::new(1).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn small_receive_after_sender_dropped() {
    receive_after_sender_dropped(NonZeroUsize::new(5).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_receive_after_sender_dropped() {
    receive_after_sender_dropped(NonZeroUsize::new(LARGE_BOUNDARY + 1).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn miss_update() {
    let (mut tx, mut rx) = super::channel(NonZeroUsize::new(1).unwrap());
    let barrier_send = Arc::new(Barrier::new(2));
    let barrier_receive = barrier_send.clone();

    let send_task = async move {
        assert!(tx.try_send(4).is_ok());
        assert!(tx.try_send(5).is_ok());
        barrier_send.wait().await;
    };

    let recv_task = async move {
        barrier_receive.wait().await;
        assert_eq!(rx.recv().await, Ok(5));
    };

    let send_handle = tokio::spawn(send_task);
    let recv_handle = tokio::spawn(recv_task);

    assert!(send_handle.await.is_ok());
    assert!(recv_handle.await.is_ok());
}

async fn receive_several(n: usize) {
    let (mut tx, mut rx) = super::channel(NonZeroUsize::new(n).unwrap());

    let send_task = async move {
        for i in 0..n {
            assert!(tx.try_send(i).is_ok());
        }
    };

    let recv_task = async move {
        for i in 0..n {
            assert_eq!(rx.recv().await, Ok(i));
        }
    };

    let send_handle = tokio::spawn(send_task);
    let recv_handle = tokio::spawn(recv_task);

    assert!(send_handle.await.is_ok());
    assert!(recv_handle.await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn small_receive_several() {
    receive_several(5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_receive_several() {
    receive_several(LARGE_BOUNDARY + 1).await;
}

async fn receive_several_stream(n: usize) {
    let (mut tx, mut rx) = super::channel(NonZeroUsize::new(n).unwrap());

    let send_task = async move {
        for i in 0..n {
            assert!(tx.try_send(i).is_ok());
        }
    };

    let recv_task = async move {
        for i in 0..n {
            assert_eq!(rx.next().await, Some(i));
        }
    };

    let send_handle = tokio::spawn(send_task);
    let recv_handle = tokio::spawn(recv_task);

    assert!(send_handle.await.is_ok());
    assert!(recv_handle.await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn small_receive_several_stream() {
    receive_several_stream(5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_receive_several_stream() {
    receive_several_stream(LARGE_BOUNDARY + 1).await;
}

async fn receive_all_stream(n: usize) {
    let (mut tx, rx) = super::channel(NonZeroUsize::new(n).unwrap());

    let send_task = async move {
        for i in 0..n {
            assert!(tx.try_send(i).is_ok());
        }
        drop(tx);
    };

    let recv_task = async move {
        let values = rx.collect::<Vec<_>>().await;
        assert_eq!(values, (0..n).collect::<Vec<_>>());
    };

    let send_handle = tokio::spawn(send_task);
    let recv_handle = tokio::spawn(recv_task);

    assert!(send_handle.await.is_ok());
    assert!(recv_handle.await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn small_receive_all_stream() {
    receive_all_stream(5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_receive_all_stream() {
    receive_all_stream(LARGE_BOUNDARY + 1).await;
}

async fn send_after_receiver_dropped(n: NonZeroUsize) {
    let (mut tx, rx) = super::channel(n);
    let barrier_send = Arc::new(Barrier::new(2));
    let barrier_receive = barrier_send.clone();

    let send_task = async move {
        barrier_send.wait().await;
        assert_eq!(tx.try_send(4), Err(SendError(4)));
    };

    let recv_task = async move {
        drop(rx);
        barrier_receive.wait().await;
    };

    let send_handle = tokio::spawn(send_task);
    let recv_handle = tokio::spawn(recv_task);

    assert!(send_handle.await.is_ok());
    assert!(recv_handle.await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_send_after_receiver_dropped() {
    send_after_receiver_dropped(NonZeroUsize::new(1).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn small_send_after_receiver_dropped() {
    send_after_receiver_dropped(NonZeroUsize::new(5).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_send_after_receiver_dropped() {
    send_after_receiver_dropped(NonZeroUsize::new(LARGE_BOUNDARY + 1).unwrap()).await;
}

async fn send_and_receive_many(n: usize, r: usize) {
    let (mut tx, rx) = super::channel(NonZeroUsize::new(n).unwrap());

    let send_task = async move {
        for i in 0..r {
            assert!(tx.try_send(i).is_ok());
        }
        drop(tx);
    };

    let recv_task = async move {
        let results = rx.collect::<Vec<_>>().await;
        assert!(results.len() <= r);
        let mut prev = None;
        for i in results.iter() {
            if let Some(p) = prev {
                assert!(p < *i);
            }
            prev = Some(*i);
        }
        assert_eq!(prev, Some(r - 1));
    };

    let send_handle = tokio::spawn(send_task);
    let recv_handle = tokio::spawn(recv_task);

    assert!(send_handle.await.is_ok());
    assert!(recv_handle.await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn one_send_and_receive_many() {
    send_and_receive_many(1, 10000).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn small_send_and_receive_many() {
    send_and_receive_many(5, 10000).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_send_and_receive_many() {
    send_and_receive_many(LARGE_BOUNDARY + 1, 10000).await;
}

struct WakeObserver(AtomicBool);

impl WakeObserver {
    fn new() -> Self {
        WakeObserver(AtomicBool::new(false))
    }
}

impl ArcWake for WakeObserver {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let WakeObserver(flag) = &**arc_self;
        flag.store(true, Ordering::SeqCst);
    }
}

#[test]
fn receiver_wake_on_sender_dropped() {
    let (tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(5).unwrap());

    let obs = Arc::new(WakeObserver::new());

    let waker = futures::task::waker(obs.clone());
    let mut context = Context::from_waker(&waker);

    let mut recv_fut = rx.recv();

    let mut pinned = Pin::new(&mut recv_fut);

    assert!(pinned.as_mut().poll(&mut context).is_pending());

    drop(tx);

    let WakeObserver(flag) = &*obs;
    assert!(flag.load(Ordering::SeqCst));

    assert_eq!(pinned.poll(&mut context), Poll::Ready(Err(RecvError)));
}
