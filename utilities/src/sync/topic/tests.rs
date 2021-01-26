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

use super::{TryRecvError, TrySendError};
use crate::future::SwimFutureExt;
use crate::sync::topic::{Receiver, SendError, Sender};
use crate::sync::trigger;
use futures::future::{join, join3};
use futures::StreamExt;
use pin_utils::core_reexport::num::NonZeroUsize;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[test]
fn next_slots() {
    assert_eq!(super::next_slot(0, 2), 1);
    assert_eq!(super::next_slot(1, 2), 2);
    assert_eq!(super::next_slot(2, 2), 0);

    assert_eq!(super::next_slot(0, 3), 1);
    assert_eq!(super::next_slot(1, 3), 2);
    assert_eq!(super::next_slot(2, 3), 3);
    assert_eq!(super::next_slot(3, 3), 0);
}

#[test]
fn prev_slots() {
    assert_eq!(super::prev_slot(0, 2), 2);
    assert_eq!(super::prev_slot(1, 2), 0);
    assert_eq!(super::prev_slot(2, 2), 1);

    assert_eq!(super::prev_slot(0, 3), 3);
    assert_eq!(super::prev_slot(1, 3), 0);
    assert_eq!(super::prev_slot(2, 3), 1);
    assert_eq!(super::prev_slot(3, 3), 2);
}

fn send_until_full_for(n: usize) {
    let (mut tx, _rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)))
}

const SIZE_RANGE: Range<usize> = 1..5;

#[test]
fn send_until_full() {
    for n in SIZE_RANGE {
        send_until_full_for(n);
    }
}

async fn send_until_full_async_for(n: usize) {
    let (mut tx, _rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    for i in 0..n {
        assert!(tx.send(i).await.is_ok());
    }
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)))
}

#[tokio::test]
async fn send_until_full_async() {
    for n in SIZE_RANGE {
        send_until_full_async_for(n).await;
    }
}

fn send_and_receive_single_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    assert!(tx.try_send(0).is_ok());

    let guard = rx.try_recv();
    assert!(matches!(guard, Ok(g) if *g == 0));

    assert!(matches!(rx.try_recv(), Err(TryRecvError::NoValue)));
}

#[test]
fn send_and_receive_single() {
    for n in SIZE_RANGE {
        send_and_receive_single_for(n);
    }
}

async fn send_and_receive_single_async_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    assert!(tx.send(0).await.is_ok());

    let guard = rx.recv().await;
    assert!(matches!(guard, Some(g) if *g == 0));

    assert!(matches!(rx.try_recv(), Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn send_and_receive_single_async() {
    for n in SIZE_RANGE {
        send_and_receive_single_async_for(n).await;
    }
}

fn send_and_receive_single_two_consumers_for(n: usize) {
    let (mut tx, mut rx1) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let mut rx2 = rx1.clone();

    assert!(tx.try_send(0).is_ok());

    let guard = rx1.try_recv();
    assert!(matches!(guard, Ok(g) if *g == 0));
    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));

    let guard = rx2.try_recv();
    assert!(matches!(guard, Ok(g) if *g == 0));
    assert!(matches!(rx2.try_recv(), Err(TryRecvError::NoValue)));
}

#[test]
fn send_and_receive_single_two_consumers() {
    for n in SIZE_RANGE {
        send_and_receive_single_two_consumers_for(n);
    }
}

async fn send_and_receive_single_two_consumers_async_for(n: usize) {
    let (mut tx, mut rx1) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let mut rx2 = rx1.clone();

    assert!(tx.send(0).await.is_ok());

    let guard = rx1.recv().await;
    assert!(matches!(guard, Some(g) if *g == 0));
    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));

    let guard = rx2.recv().await;
    assert!(matches!(guard, Some(g) if *g == 0));
    assert!(matches!(rx2.try_recv(), Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn send_and_receive_single_two_consumers_async() {
    for n in SIZE_RANGE {
        send_and_receive_single_two_consumers_async_for(n).await;
    }
}

fn send_and_receive_multiple_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }

    for i in 0..n {
        let guard = rx.try_recv();
        assert!(matches!(guard, Ok(g) if *g == i));
    }

    assert!(matches!(rx.try_recv(), Err(TryRecvError::NoValue)));
}

#[test]
fn send_and_receive_multiple() {
    for n in SIZE_RANGE {
        send_and_receive_multiple_for(n);
    }
}

async fn send_and_receive_multiple_async_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.send(i).await.is_ok());
    }

    for i in 0..n {
        let guard = rx.recv().await;
        assert!(matches!(guard, Some(g) if *g == i));
    }

    assert!(matches!(rx.try_recv(), Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn send_and_receive_multiple_async() {
    for n in SIZE_RANGE {
        send_and_receive_multiple_async_for(n).await;
    }
}

fn send_and_receive_multiple_interleaved_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
        let guard = rx.try_recv();
        assert!(matches!(guard, Ok(g) if *g == i));
    }
}

#[test]
fn send_and_receive_multiple_interleaved() {
    for n in SIZE_RANGE {
        send_and_receive_multiple_interleaved_for(n);
    }
}

async fn send_and_receive_multiple_interleaved_async_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.send(i).await.is_ok());
        let guard = rx.recv().await;
        assert!(matches!(guard, Some(g) if *g == i));
    }
}

#[tokio::test]
async fn send_and_receive_multiple_interleaved_async() {
    for n in SIZE_RANGE {
        send_and_receive_multiple_interleaved_async_for(n).await;
    }
}

fn send_and_receive_multiple_two_consumers_for(n: usize) {
    let (mut tx, mut rx1) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let mut rx2 = rx1.clone();

    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }

    for i in 0..n {
        let guard = rx1.try_recv();
        assert!(matches!(guard, Ok(g) if *g == i));
    }

    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));

    for i in 0..n {
        let guard = rx2.try_recv();
        assert!(matches!(guard, Ok(g) if *g == i));
    }

    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));
    assert!(matches!(rx2.try_recv(), Err(TryRecvError::NoValue)));
}

#[test]
fn send_and_receive_multiple_two_consumers() {
    for n in SIZE_RANGE {
        send_and_receive_multiple_two_consumers_for(n);
    }
}

async fn send_and_receive_multiple_two_consumers_async_for(n: usize) {
    let (mut tx, mut rx1) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let mut rx2 = rx1.clone();

    for i in 0..n {
        assert!(tx.send(i).await.is_ok());
    }

    for i in 0..n {
        let guard = rx1.recv().await;
        assert!(matches!(guard, Some(g) if *g == i));
    }

    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));

    for i in 0..n {
        let guard = rx2.recv().await;
        assert!(matches!(guard, Some(g) if *g == i));
    }

    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));
    assert!(matches!(rx2.try_recv(), Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn send_and_receive_multiple_two_consumers_async() {
    for n in SIZE_RANGE {
        send_and_receive_multiple_two_consumers_async_for(n).await;
    }
}

fn unblock_with_read_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    assert!(matches!(rx.try_recv(), Ok(g) if *g == 0));

    assert!(tx.try_send(n).is_ok());
    assert_eq!(tx.try_send(n + 1), Err(TrySendError::NoCapacity(n + 1)));
}

#[test]
fn unblock_with_read() {
    for n in SIZE_RANGE {
        unblock_with_read_for(n);
    }
}

async fn unblock_with_read_async_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.send(i).await.is_ok());
    }
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    assert!(matches!(rx.recv().await, Some(g) if *g == 0));

    assert!(tx.send(n).await.is_ok());
    assert_eq!(tx.try_send(n + 1), Err(TrySendError::NoCapacity(n + 1)));
}

#[tokio::test]
async fn unblock_with_read_async() {
    for n in SIZE_RANGE {
        unblock_with_read_async_for(n).await;
    }
}

fn unblock_with_two_readers_for(n: usize) {
    let (mut tx, mut rx1) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let mut rx2 = rx1.clone();
    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    assert!(matches!(rx1.try_recv(), Ok(g) if *g == 0));
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    assert!(matches!(rx2.try_recv(), Ok(g) if *g == 0));
    assert!(tx.try_send(n).is_ok());
    assert_eq!(tx.try_send(n + 1), Err(TrySendError::NoCapacity(n + 1)));
}

#[test]
fn unblock_with_two_readers() {
    for n in SIZE_RANGE {
        unblock_with_two_readers_for(n);
    }
}

async fn unblock_with_two_readers_async_for(n: usize) {
    let (mut tx, mut rx1) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let mut rx2 = rx1.clone();
    for i in 0..n {
        assert!(tx.send(i).await.is_ok());
    }
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    assert!(matches!(rx1.recv().await, Some(g) if *g == 0));
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    assert!(matches!(rx2.recv().await, Some(g) if *g == 0));
    assert!(tx.send(n).await.is_ok());
    assert_eq!(tx.try_send(n + 1), Err(TrySendError::NoCapacity(n + 1)));
}

#[tokio::test]
async fn unblock_with_two_readers_async() {
    for n in SIZE_RANGE {
        unblock_with_two_readers_async_for(n).await;
    }
}

fn unblock_with_dropped_reader_for(n: usize) {
    let (mut tx, mut rx1) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let rx2 = rx1.clone();
    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    assert!(matches!(rx1.try_recv(), Ok(g) if *g == 0));
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    drop(rx2);
    assert!(tx.try_send(n).is_ok());
    assert_eq!(tx.try_send(n + 1), Err(TrySendError::NoCapacity(n + 1)));
}

#[test]
fn unblock_with_dropped_reader() {
    for n in SIZE_RANGE {
        unblock_with_dropped_reader_for(n);
    }
}

async fn unblock_with_dropped_reader_async_for(n: usize) {
    let (mut tx, mut rx1) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let rx2 = rx1.clone();
    for i in 0..n {
        assert!(tx.send(i).await.is_ok());
    }
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    assert!(matches!(rx1.recv().await, Some(g) if *g == 0));
    assert_eq!(tx.try_send(n), Err(TrySendError::NoCapacity(n)));

    drop(rx2);
    assert!(tx.send(n).await.is_ok());
    assert_eq!(tx.try_send(n + 1), Err(TrySendError::NoCapacity(n + 1)));
}

#[tokio::test]
async fn unblock_with_dropped_reader_async() {
    for n in SIZE_RANGE {
        unblock_with_dropped_reader_async_for(n).await;
    }
}

fn receive_after_sender_dropped_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }
    drop(tx);

    for i in 0..n {
        assert!(matches!(rx.try_recv(), Ok(g) if *g == i));
    }
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Closed)));
}

#[test]
fn receive_after_sender_dropped() {
    for n in SIZE_RANGE {
        receive_after_sender_dropped_for(n);
    }
}

async fn receive_after_sender_dropped_async_for(n: usize) {
    let (mut tx, mut rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.send(i).await.is_ok());
    }
    drop(tx);

    for i in 0..n {
        assert!(matches!(rx.recv().await, Some(g) if *g == i));
    }
    assert!(matches!(rx.recv().await, None));
}

#[tokio::test]
async fn receive_after_sender_dropped_async() {
    for n in SIZE_RANGE {
        receive_after_sender_dropped_async_for(n).await;
    }
}

fn send_after_all_receivers_dropped_for(n: usize) {
    let (mut tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    drop(rx);

    assert!(matches!(tx.try_send(0), Err(TrySendError::NoReceivers(0))));
}

#[test]
fn send_after_all_receivers_dropped() {
    for n in SIZE_RANGE {
        send_after_all_receivers_dropped_for(n);
    }
}

async fn send_after_all_receivers_dropped_async_for(n: usize) {
    let (mut tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    drop(rx);

    assert!(matches!(tx.send(0).await, Err(SendError(0))));
}

#[tokio::test]
async fn send_after_all_receivers_dropped_async() {
    for n in SIZE_RANGE {
        send_after_all_receivers_dropped_async_for(n).await;
    }
}

async fn receive_as_stream_for(n: usize) {
    let (mut tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }

    let mut stream = rx.into_stream();

    for i in 0..n {
        assert_eq!(stream.next().await, Some(i));
    }
}

#[tokio::test]
async fn receive_as_stream() {
    for n in SIZE_RANGE {
        receive_as_stream_for(n).await;
    }
}

async fn receive_all_as_stream_for(n: usize) {
    let (mut tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());

    for i in 0..n {
        assert!(tx.try_send(i).is_ok());
    }
    drop(tx);

    let stream = rx.into_stream();
    let results = stream.collect::<Vec<_>>().await;
    assert_eq!(results, (0..n).into_iter().collect::<Vec<_>>());
}

#[tokio::test]
async fn receive_all_as_stream() {
    for n in SIZE_RANGE {
        receive_all_as_stream_for(n).await;
    }
}

const TIMEOUT: Duration = Duration::from_secs(5);

async fn run_as_tasks<F1, F2, Fut1, Fut2>(
    tx: Sender<usize>,
    rx: Receiver<usize>,
    write_task: F1,
    read_task: F2,
) -> ()
where
    Fut1: Future<Output = Option<Sender<usize>>> + Send + 'static,
    Fut2: Future<Output = ()> + Send + 'static,
    F1: FnOnce(Sender<usize>) -> Fut1 + Send + 'static,
    F2: FnOnce(Receiver<usize>) -> Fut2 + Send + 'static,
{
    let (coord_tx, coord_rx) = trigger::trigger();
    let task1 = tokio::task::spawn(async move {
        let _tx = write_task(tx).await;
        assert!(coord_rx.await.is_ok());
    });
    let task2 = tokio::task::spawn(async move {
        read_task(rx).await;
        coord_tx.trigger();
    });
    let result = join(
        tokio::time::timeout(TIMEOUT, task1),
        tokio::time::timeout(TIMEOUT, task2),
    )
    .await;
    assert!(matches!(result, (Ok(Ok(_)), Ok(Ok(_)))));
}

async fn run_as_tasks_two_readers<F1, F2, F3, Fut1, Fut2, Fut3>(
    tx: Sender<usize>,
    rx: Receiver<usize>,
    write_task: F1,
    read_task1: F2,
    read_task2: F3,
) -> ()
where
    Fut1: Future<Output = Option<Sender<usize>>> + Send + 'static,
    Fut2: Future<Output = ()> + Send + 'static,
    Fut3: Future<Output = ()> + Send + 'static,
    F1: FnOnce(Sender<usize>) -> Fut1 + Send + 'static,
    F2: FnOnce(Receiver<usize>) -> Fut2 + Send + 'static,
    F3: FnOnce(Receiver<usize>) -> Fut3 + Send + 'static,
{
    let (coord_tx1, coord_rx1) = trigger::trigger();
    let (coord_tx2, coord_rx2) = trigger::trigger();
    let rx_cpy = rx.clone();
    let task1 = tokio::task::spawn(async move {
        let _tx = write_task(tx).await;
        let result = join(coord_rx1, coord_rx2).await;
        assert!(matches!(result, (Ok(_), Ok(_))));
    });
    let task2 = tokio::task::spawn(async move {
        read_task1(rx).await;
        coord_tx1.trigger();
    });
    let task3 = tokio::task::spawn(async move {
        read_task2(rx_cpy).await;
        coord_tx2.trigger();
    });
    let result = join3(
        tokio::time::timeout(TIMEOUT, task1),
        tokio::time::timeout(TIMEOUT, task2),
        tokio::time::timeout(TIMEOUT, task3),
    )
    .await;
    assert!(matches!(result, (Ok(Ok(_)), Ok(Ok(_)), Ok(Ok(_)))));
}

async fn send_receive_different_tasks_within_limit_for(n: usize) {
    let (tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let sender = move |mut tx: Sender<usize>| async move {
        for i in 0..n {
            assert!(tx.send(i).await.is_ok());
        }
        Some(tx)
    };

    let receiver = move |mut rx: Receiver<usize>| async move {
        for i in 0..n {
            assert!(matches!(rx.recv().await, Some(g) if *g == i));
        }
    };
    run_as_tasks(tx, rx, sender, receiver).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn send_receive_different_tasks_within_limit() {
    for n in SIZE_RANGE {
        send_receive_different_tasks_within_limit_for(n).await;
    }
}

async fn send_receive_different_tasks_many_for(n: usize) {
    let (tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let sender = |mut tx: Sender<usize>| async move {
        for i in 0..20 {
            assert!(tx.send(i).await.is_ok());
        }
        Some(tx)
    };

    let receiver = |mut rx: Receiver<usize>| async move {
        for i in 0..20 {
            assert!(matches!(rx.recv().await, Some(g) if *g == i));
        }
    };
    run_as_tasks(tx, rx, sender, receiver).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn send_receive_different_tasks_many() {
    for n in SIZE_RANGE {
        send_receive_different_tasks_many_for(n).await;
    }
}

async fn receiver_wakes_sender_for(n: usize) {
    let (tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let notify = Arc::new(Notify::new());
    let notify_cpy = notify.clone();

    let sender = move |mut tx: Sender<usize>| async move {
        let tx = async move {
            for i in 0..(n + 1) {
                assert!(tx.send(i).await.is_ok());
            }
            tx
        }
        .notify_on_blocked(notify)
        .await;
        Some(tx)
    };

    let receiver = move |mut rx: Receiver<usize>| async move {
        notify_cpy.notified().await;
        for i in 0..(n + 1) {
            assert!(matches!(rx.recv().await, Some(g) if *g == i));
        }
    };
    run_as_tasks(tx, rx, sender, receiver).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn receiver_wakes_sender() {
    for n in SIZE_RANGE {
        receiver_wakes_sender_for(n).await;
    }
}

async fn sender_wakes_receiver_for(n: usize) {
    let (tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let notify = Arc::new(Notify::new());
    let notify_cpy = notify.clone();

    let sender = move |mut tx: Sender<usize>| async move {
        notify.notified().await;
        for i in 0..(n + 1) {
            assert!(tx.send(i).await.is_ok());
        }
        Some(tx)
    };

    let receiver = move |mut rx: Receiver<usize>| async move {
        async move {
            for i in 0..(n + 1) {
                assert!(matches!(rx.recv().await, Some(g) if *g == i));
            }
        }
        .notify_on_blocked(notify_cpy)
        .await;
    };
    run_as_tasks(tx, rx, sender, receiver).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sender_wakes_receiver() {
    for n in SIZE_RANGE {
        sender_wakes_receiver_for(n).await;
    }
}

async fn sender_wakes_two_receivers_for(n: usize) {
    let (tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let notify1 = Arc::new(Notify::new());
    let notify1_cpy = notify1.clone();
    let notify2 = Arc::new(Notify::new());
    let notify2_cpy = notify2.clone();

    let sender = move |mut tx: Sender<usize>| async move {
        join(notify1.notified(), notify2.notified()).await;
        for i in 0..(n + 1) {
            assert!(tx.send(i).await.is_ok());
        }
        Some(tx)
    };

    let receiver1 = move |mut rx: Receiver<usize>| async move {
        async move {
            for i in 0..(n + 1) {
                assert!(matches!(rx.recv().await, Some(g) if *g == i));
            }
        }
        .notify_on_blocked(notify1_cpy)
        .await;
    };

    let receiver2 = move |mut rx: Receiver<usize>| async move {
        async move {
            for i in 0..(n + 1) {
                assert!(matches!(rx.recv().await, Some(g) if *g == i));
            }
        }
        .notify_on_blocked(notify2_cpy)
        .await;
    };
    run_as_tasks_two_readers(tx, rx, sender, receiver1, receiver2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sender_wakes_two_receivers() {
    for n in SIZE_RANGE {
        sender_wakes_two_receivers_for(n).await;
    }
}

async fn dropped_receiver_wakes_sender_for(n: usize) {
    let (tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let notify = Arc::new(Notify::new());
    let notify_cpy = notify.clone();

    let sender = move |mut tx: Sender<usize>| async move {
        let tx = async move {
            for i in 0..n {
                assert!(tx.send(i).await.is_ok());
            }
            assert!(matches!(tx.send(n).await, Err(SendError(x)) if x == n));
            tx
        }
        .notify_on_blocked(notify)
        .await;
        Some(tx)
    };

    let receiver = |rx: Receiver<usize>| async move {
        notify_cpy.notified().await;
        drop(rx);
    };
    run_as_tasks(tx, rx, sender, receiver).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_receiver_wakes_sender() {
    for n in SIZE_RANGE {
        dropped_receiver_wakes_sender_for(n).await;
    }
}

async fn dropped_sender_wakes_receiver_for(n: usize) {
    let (tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let notify = Arc::new(Notify::new());
    let notify_cpy = notify.clone();

    let sender = |tx: Sender<usize>| async move {
        notify.notified().await;
        drop(tx);
        None
    };

    let receiver = |mut rx: Receiver<usize>| async move {
        async move {
            assert!(rx.recv().await.is_none());
        }
        .notify_on_blocked(notify_cpy)
        .await;
    };
    run_as_tasks(tx, rx, sender, receiver).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_sender_wakes_receiver() {
    for n in SIZE_RANGE {
        dropped_sender_wakes_receiver_for(n).await;
    }
}

#[tokio::test]
async fn send_many_records_two_receivers() {
    let (tx, rx) = super::channel::<usize>(NonZeroUsize::new(5).unwrap());

    let sender = |mut tx: Sender<usize>| async move {
        for i in 0..10000 {
            assert!(tx.send(i).await.is_ok());
        }
        Some(tx)
    };

    let receiver1 = |mut rx: Receiver<usize>| async move {
        for i in 0..10000 {
            assert!(matches!(rx.recv().await, Some(g) if *g == i));
        }
    };

    let receiver2 = |mut rx: Receiver<usize>| async move {
        for i in 0..10000 {
            assert!(matches!(rx.recv().await, Some(g) if *g == i));
        }
    };
    run_as_tasks_two_readers(tx, rx, sender, receiver1, receiver2).await;
}

async fn send_with_subscriber_for(n: usize) {
    let (mut tx, rx) = super::channel::<usize>(NonZeroUsize::new(n).unwrap());
    let sub = rx.subscriber();
    drop(rx);
    assert!(matches!(tx.send(1).await, Err(SendError(1))));
    assert!(tx.discarding_send(1).await.is_ok());

    let rx2_res = sub.subscribe();
    assert!(rx2_res.is_ok());
    let mut rx2 = rx2_res.unwrap();
    assert!(tx.send(2).await.is_ok());
    assert!(matches!(rx2.recv().await, Some(g) if *g == 2));

    drop(rx2);
    drop(sub);

    assert!(matches!(tx.send(3).await, Err(SendError(3))));
    assert!(matches!(tx.discarding_send(3).await, Err(SendError(3))));

}

#[tokio::test]
async fn send_with_subscriber() {
    for n in SIZE_RANGE {
        send_with_subscriber_for(n).await;
    }
}
