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


use pin_utils::core_reexport::num::NonZeroUsize;
use super::{TrySendError, TryRecvError};
use std::future::Future;
use crate::sync::topic::{Sender, Receiver};
use futures::StreamExt;

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

#[test]
fn send_until_full() {
    let (mut tx, _rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    assert!(tx.try_send(1).is_ok());
    assert!(tx.try_send(2).is_ok());
    assert!(tx.try_send(3).is_ok());
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)))
}

#[tokio::test]
async fn send_until_full_async() {
    let (mut tx, _rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    assert!(tx.send(1).await.is_ok());
    assert!(tx.send(2).await.is_ok());
    assert!(tx.send(3).await.is_ok());
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)))
}

#[test]
fn send_and_receive_single() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    assert!(tx.try_send(1).is_ok());

    let guard = rx.try_recv();
    assert!(matches!(guard, Ok(g) if *g == 1));

    assert!(matches!(rx.try_recv(), Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn send_and_receive_single_async() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    assert!(tx.send(1).await.is_ok());

    let guard = rx.recv().await;
    assert!(matches!(guard, Some(g) if *g == 1));

    assert!(matches!(rx.try_recv(), Err(TryRecvError::NoValue)));
}

#[test]
fn send_and_receive_single_two_consumers() {
    let (mut tx, mut rx1) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let mut rx2 = rx1.clone();

    assert!(tx.try_send(1).is_ok());

    let guard = rx1.try_recv();
    assert!(matches!(guard, Ok(g) if *g == 1));
    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));

    let guard = rx2.try_recv();
    assert!(matches!(guard, Ok(g) if *g == 1));
    assert!(matches!(rx2.try_recv(), Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn send_and_receive_single_two_consumers_async() {
    let (mut tx, mut rx1) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let mut rx2 = rx1.clone();

    assert!(tx.send(1).await.is_ok());

    let guard = rx1.recv().await;
    assert!(matches!(guard, Some(g) if *g == 1));
    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));

    let guard = rx2.recv().await;
    assert!(matches!(guard, Some(g) if *g == 1));
    assert!(matches!(rx2.try_recv(), Err(TryRecvError::NoValue)));
}

#[test]
fn send_and_receive_multiple() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());

    for i in 1..4 {
        assert!(tx.try_send(i).is_ok());
    }

    for i in 1..4 {
        let guard = rx.try_recv();
        assert!(matches!(guard, Ok(g) if *g == i));
    }

    assert!(matches!(rx.try_recv(), Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn send_and_receive_multiple_async() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());

    for i in 1..4 {
        assert!(tx.send(i).await.is_ok());
    }

    for i in 1..4 {
        let guard = rx.recv().await;
        assert!(matches!(guard, Some(g) if *g == i));
    }

    assert!(matches!(rx.try_recv(), Err(TryRecvError::NoValue)));
}

#[test]
fn send_and_receive_multiple_interleaved() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());

    for i in 1..4 {
        assert!(tx.try_send(i).is_ok());
        let guard = rx.try_recv();
        assert!(matches!(guard, Ok(g) if *g == i));
    }

}

#[tokio::test]
async fn send_and_receive_multiple_interleaved_async() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());

    for i in 1..4 {
        assert!(tx.send(i).await.is_ok());
        let guard = rx.recv().await;
        assert!(matches!(guard, Some(g) if *g == i));
    }

}

#[test]
fn send_and_receive_multiple_two_consumers() {
    let (mut tx, mut rx1) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let mut rx2 = rx1.clone();

    for i in 1..4 {
        assert!(tx.try_send(i).is_ok());
    }

    for i in 1..4 {
        let guard = rx1.try_recv();
        assert!(matches!(guard, Ok(g) if *g == i));
    }

    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));

    for i in 1..4 {
        let guard = rx2.try_recv();
        assert!(matches!(guard, Ok(g) if *g == i));
    }

    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));
    assert!(matches!(rx2.try_recv(), Err(TryRecvError::NoValue)));
}

#[tokio::test]
async fn send_and_receive_multiple_two_consumers_async() {
    let (mut tx, mut rx1) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let mut rx2 = rx1.clone();

    for i in 1..4 {
        assert!(tx.send(i).await.is_ok());
    }

    for i in 1..4 {
        let guard = rx1.recv().await;
        assert!(matches!(guard, Some(g) if *g == i));
    }

    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));

    for i in 1..4 {
        let guard = rx2.recv().await;
        assert!(matches!(guard, Some(g) if *g == i));
    }

    assert!(matches!(rx1.try_recv(), Err(TryRecvError::NoValue)));
    assert!(matches!(rx2.try_recv(), Err(TryRecvError::NoValue)));
}

#[test]
fn unblock_with_read() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    assert!(tx.try_send(1).is_ok());
    assert!(tx.try_send(2).is_ok());
    assert!(tx.try_send(3).is_ok());
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    assert!(matches!(rx.try_recv(), Ok(g) if *g == 1));

    assert!(tx.try_send(4).is_ok());
    assert_eq!(tx.try_send(5), Err(TrySendError::NoCapacity(5)));
}

#[tokio::test]
async fn unblock_with_read_async() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    assert!(tx.send(1).await.is_ok());
    assert!(tx.send(2).await.is_ok());
    assert!(tx.send(3).await.is_ok());
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    assert!(matches!(rx.recv().await, Some(g) if *g == 1));

    assert!(tx.send(4).await.is_ok());
    assert_eq!(tx.try_send(5), Err(TrySendError::NoCapacity(5)));
}

#[test]
fn unblock_with_two_readers() {
    let (mut tx, mut rx1) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let mut rx2 = rx1.clone();
    assert!(tx.try_send(1).is_ok());
    assert!(tx.try_send(2).is_ok());
    assert!(tx.try_send(3).is_ok());
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    assert!(matches!(rx1.try_recv(), Ok(g) if *g == 1));
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    assert!(matches!(rx2.try_recv(), Ok(g) if *g == 1));
    assert!(tx.try_send(4).is_ok());
    assert_eq!(tx.try_send(5), Err(TrySendError::NoCapacity(5)));
}

#[tokio::test]
async fn unblock_with_two_readers_async() {
    let (mut tx, mut rx1) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let mut rx2 = rx1.clone();
    assert!(tx.send(1).await.is_ok());
    assert!(tx.send(2).await.is_ok());
    assert!(tx.send(3).await.is_ok());
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    assert!(matches!(rx1.recv().await, Some(g) if *g == 1));
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    assert!(matches!(rx2.recv().await, Some(g) if *g == 1));
    assert!(tx.send(4).await.is_ok());
    assert_eq!(tx.try_send(5), Err(TrySendError::NoCapacity(5)));
}

#[test]
fn unblock_with_dropped_reader() {
    let (mut tx, mut rx1) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let rx2 = rx1.clone();
    assert!(tx.try_send(1).is_ok());
    assert!(tx.try_send(2).is_ok());
    assert!(tx.try_send(3).is_ok());
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    assert!(matches!(rx1.try_recv(), Ok(g) if *g == 1));
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    drop(rx2);
    assert!(tx.try_send(4).is_ok());
    assert_eq!(tx.try_send(5), Err(TrySendError::NoCapacity(5)));
}

#[tokio::test]
async fn unblock_with_dropped_reader_async() {
    let (mut tx, mut rx1) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let rx2 = rx1.clone();
    assert!(tx.send(1).await.is_ok());
    assert!(tx.send(2).await.is_ok());
    assert!(tx.send(3).await.is_ok());
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    assert!(matches!(rx1.recv().await, Some(g) if *g == 1));
    assert_eq!(tx.try_send(4), Err(TrySendError::NoCapacity(4)));

    drop(rx2);
    assert!(tx.send(4).await.is_ok());
    assert_eq!(tx.try_send(5), Err(TrySendError::NoCapacity(5)));
}

#[test]
fn receive_after_sender_dropped() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());

    assert!(tx.try_send(1).is_ok());
    assert!(tx.try_send(2).is_ok());
    assert!(tx.try_send(3).is_ok());
    drop(tx);

    assert!(matches!(rx.try_recv(), Ok(g) if *g == 1));
    assert!(matches!(rx.try_recv(), Ok(g) if *g == 2));
    assert!(matches!(rx.try_recv(), Ok(g) if *g == 3));
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Closed)));

}

#[tokio::test]
async fn receive_after_sender_dropped_async() {
    let (mut tx, mut rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());

    assert!(tx.send(1).await.is_ok());
    assert!(tx.send(2).await.is_ok());
    assert!(tx.send(3).await.is_ok());
    drop(tx);

    assert!(matches!(rx.recv().await, Some(g) if *g == 1));
    assert!(matches!(rx.recv().await, Some(g) if *g == 2));
    assert!(matches!(rx.recv().await, Some(g) if *g == 3));
    assert!(matches!(rx.recv().await, None));

}

#[tokio::test]
async fn receive_as_stream() {
    let (mut tx, rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());

    assert!(tx.try_send(1).is_ok());
    assert!(tx.try_send(2).is_ok());
    assert!(tx.try_send(3).is_ok());

    let mut stream = rx.into_stream();
    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, Some(2));
    assert_eq!(stream.next().await, Some(3));

}

#[tokio::test]
async fn receive_all_as_stream() {
    let (mut tx, rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());

    assert!(tx.try_send(1).is_ok());
    assert!(tx.try_send(2).is_ok());
    assert!(tx.try_send(3).is_ok());
    drop(tx);

    let stream = rx.into_stream();
    let results = stream.collect::<Vec<_>>().await;
    assert_eq!(results, vec![1, 2, 3]);

}

async fn run_as_tasks<F1, F2, Fut1, Fut2>(
    tx: Sender<i32>, rx: Receiver<i32>,
    write_task: F1,
    read_task: F2) -> ()
where
    Fut1: Future<Output = ()> + Send + 'static,
    Fut2: Future<Output = ()> + Send + 'static,
    F1: FnOnce(Sender<i32>) -> Fut1,
    F2: FnOnce(Receiver<i32>) -> Fut2,
{
    let task1 = tokio::task::spawn(write_task(tx));
    let task2 = tokio::task::spawn(read_task(rx));
    let result = futures::future::join(task1, task2).await;
    assert!(matches!(result, (Ok(_), Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn send_receiver_different_tasks() {
    let (tx, rx) = super::channel::<i32>(NonZeroUsize::new(3).unwrap());
    let sender = |mut tx: Sender<i32>| async move {
        for i in 1..6 {
            assert!(tx.send(i).await.is_ok());
        }
    };

    let receiver = |mut rx: Receiver<i32>| async move {
        for i in 1..6 {
            assert!(matches!(rx.recv().await, Some(g) if *g == i));
        }
    };
    run_as_tasks(tx, rx, sender, receiver).await;
}