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

use crate::sync::rwlock::{RwLock, WriterQueue};
use futures::future::{join, join_all};
use futures::task::{self, ArcWake};
use futures::FutureExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Waker;
use tokio::sync::{oneshot, Barrier};

const REPEATS: usize = 1000;

fn validate_empty_state(queue: &WriterQueue) {
    let WriterQueue {
        first,
        last,
        wakers,
    } = queue;
    assert!(first.is_none());
    assert!(last.is_none());
    assert!(wakers.is_empty());
}

fn check_consistency(queue: &WriterQueue) {
    let WriterQueue {
        first,
        last,
        wakers,
    } = queue;
    let mut n: usize = 0;
    let mut prev = None;
    let mut current = *first;
    while let Some(i) = current {
        n += 1;
        assert!(wakers.contains(i));
        let waker = &wakers[i];
        assert_eq!(waker.prev, prev);
        prev = current;
        if waker.next.is_none() {
            assert_eq!(current, *last);
        }
        current = waker.next;
    }
    assert_eq!(wakers.len(), n);
}

#[derive(Default, Debug)]
struct TestWaker(AtomicBool);

impl TestWaker {
    fn woken(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

impl ArcWake for TestWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::SeqCst);
    }
}

fn make_waker() -> (Arc<TestWaker>, Waker) {
    let test_waker = Arc::new(TestWaker::default());
    let waker = task::waker(test_waker.clone());
    (test_waker, waker)
}

#[test]
fn empty_writer_queue() {
    let mut queue = WriterQueue::default();
    queue.remove(0); //Removing a non-existent entry shouldn't panic.
    check_consistency(&queue);
    assert!(queue.poll().is_none());
    validate_empty_state(&queue);
}

#[test]
fn insert_single() {
    let (rx, waker) = make_waker();
    let mut queue = WriterQueue::default();
    queue.add_waker(waker, None);
    check_consistency(&queue);

    let head = queue.poll();
    check_consistency(&queue);
    assert!(head.is_some());
    head.unwrap().wake();
    assert!(rx.woken());

    assert!(queue.poll().is_none());
    validate_empty_state(&queue);
}

#[test]
fn remove_inserted() {
    let (_, waker) = make_waker();
    let mut queue = WriterQueue::default();
    let slot = queue.add_waker(waker, None);
    check_consistency(&queue);
    queue.remove(slot);
    check_consistency(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
    validate_empty_state(&queue);
}

#[test]
fn insert_two() {
    let (rx1, waker1) = make_waker();
    let (rx2, waker2) = make_waker();
    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    let j = queue.add_waker(waker2, None);
    check_consistency(&queue);

    assert_ne!(i, j);

    let first = queue.poll();
    check_consistency(&queue);
    assert!(first.is_some());
    first.unwrap().wake();

    assert!(rx1.woken());
    assert!(!rx2.woken());

    let second = queue.poll();
    check_consistency(&queue);
    assert!(second.is_some());
    second.unwrap().wake();

    assert!(rx2.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn insert_three() {
    let (rx1, waker1) = make_waker();
    let (rx2, waker2) = make_waker();
    let (rx3, waker3) = make_waker();
    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    let j = queue.add_waker(waker2, None);
    check_consistency(&queue);
    let k = queue.add_waker(waker3, None);
    check_consistency(&queue);

    assert_ne!(i, j);
    assert_ne!(i, k);
    assert_ne!(j, k);

    let first = queue.poll();
    check_consistency(&queue);
    assert!(first.is_some());
    first.unwrap().wake();

    assert!(rx1.woken());
    assert!(!rx2.woken());
    assert!(!rx3.woken());

    let second = queue.poll();
    check_consistency(&queue);
    assert!(second.is_some());
    second.unwrap().wake();

    assert!(rx2.woken());
    assert!(!rx3.woken());

    let third = queue.poll();
    check_consistency(&queue);
    assert!(third.is_some());
    third.unwrap().wake();

    assert!(rx3.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn update_inserted() {
    let (_, waker1) = make_waker();
    let (rx, waker2) = make_waker();
    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    let j = queue.add_waker(waker2, Some(i));
    check_consistency(&queue);

    assert_eq!(i, j);

    let head = queue.poll();
    check_consistency(&queue);
    assert!(head.is_some());
    head.unwrap().wake();
    assert!(rx.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn update_spurious() {
    let (rx1, waker1) = make_waker();
    let (rx2, waker2) = make_waker();
    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    let j = queue.add_waker(waker2, Some(i + 1));
    check_consistency(&queue);

    assert_ne!(i, j);

    let first = queue.poll();
    check_consistency(&queue);
    assert!(first.is_some());
    first.unwrap().wake();

    assert!(rx1.woken());
    assert!(!rx2.woken());

    let second = queue.poll();
    check_consistency(&queue);
    assert!(second.is_some());
    second.unwrap().wake();

    assert!(rx2.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn remove_first_of_two() {
    let (_, waker1) = make_waker();
    let (rx2, waker2) = make_waker();
    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    queue.add_waker(waker2, None);
    check_consistency(&queue);

    queue.remove(i);
    check_consistency(&queue);

    assert!(!rx2.woken());
    let entry = queue.poll();
    check_consistency(&queue);
    assert!(entry.is_some());
    entry.unwrap().wake();

    assert!(rx2.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn remove_second_of_two() {
    let (rx1, waker1) = make_waker();
    let (_, waker2) = make_waker();
    let mut queue = WriterQueue::default();
    queue.add_waker(waker1, None);
    check_consistency(&queue);
    let i = queue.add_waker(waker2, None);
    check_consistency(&queue);

    queue.remove(i);
    check_consistency(&queue);

    assert!(!rx1.woken());
    let entry = queue.poll();
    check_consistency(&queue);
    assert!(entry.is_some());
    entry.unwrap().wake();

    assert!(rx1.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn remove_first_of_three() {
    let (_, waker1) = make_waker();
    let (rx2, waker2) = make_waker();
    let (rx3, waker3) = make_waker();
    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);

    queue.add_waker(waker2, None);
    check_consistency(&queue);
    queue.add_waker(waker3, None);
    check_consistency(&queue);

    queue.remove(i);
    check_consistency(&queue);

    assert!(!rx2.woken());
    let entry = queue.poll();
    check_consistency(&queue);
    assert!(entry.is_some());
    entry.unwrap().wake();
    assert!(rx2.woken());

    assert!(!rx3.woken());
    let entry = queue.poll();
    check_consistency(&queue);
    assert!(entry.is_some());
    entry.unwrap().wake();
    assert!(rx3.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn remove_second_of_three() {
    let (rx1, waker1) = make_waker();
    let (_, waker2) = make_waker();
    let (rx3, waker3) = make_waker();
    let mut queue = WriterQueue::default();
    queue.add_waker(waker1, None);
    check_consistency(&queue);
    let i = queue.add_waker(waker2, None);
    check_consistency(&queue);
    queue.add_waker(waker3, None);
    check_consistency(&queue);

    queue.remove(i);
    check_consistency(&queue);

    assert!(!rx1.woken());
    let entry = queue.poll();
    check_consistency(&queue);
    assert!(entry.is_some());
    entry.unwrap().wake();
    assert!(rx1.woken());

    assert!(!rx3.woken());
    let entry = queue.poll();
    check_consistency(&queue);
    assert!(entry.is_some());
    entry.unwrap().wake();
    assert!(rx3.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn remove_third_of_three() {
    let (rx1, waker1) = make_waker();
    let (rx2, waker2) = make_waker();
    let (_, waker3) = make_waker();
    let mut queue = WriterQueue::default();
    queue.add_waker(waker1, None);
    check_consistency(&queue);
    queue.add_waker(waker2, None);
    check_consistency(&queue);
    let i = queue.add_waker(waker3, None);
    check_consistency(&queue);

    queue.remove(i);
    check_consistency(&queue);

    assert!(!rx1.woken());
    let entry = queue.poll();
    check_consistency(&queue);
    assert!(entry.is_some());
    entry.unwrap().wake();
    assert!(rx1.woken());

    assert!(!rx2.woken());
    let entry = queue.poll();
    check_consistency(&queue);
    assert!(entry.is_some());
    entry.unwrap().wake();
    assert!(rx2.woken());

    validate_empty_state(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn insert_and_poll_multiple() {
    let (_rx1, waker1) = make_waker();
    let (_rx2, waker2) = make_waker();
    let (_rx3, waker3) = make_waker();

    let mut queue = WriterQueue::default();
    queue.add_waker(waker1, None);
    check_consistency(&queue);
    queue.add_waker(waker2, None);
    check_consistency(&queue);
    queue.add_waker(waker3, None);
    check_consistency(&queue);

    assert!(queue.poll().is_some());
    check_consistency(&queue);
    assert!(queue.poll().is_some());
    check_consistency(&queue);
    assert!(queue.poll().is_some());
    check_consistency(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn insert_and_remove_multiple_forward() {
    let (_rx1, waker1) = make_waker();
    let (_rx2, waker2) = make_waker();
    let (_rx3, waker3) = make_waker();

    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    let j = queue.add_waker(waker2, None);
    check_consistency(&queue);
    let k = queue.add_waker(waker3, None);
    check_consistency(&queue);

    queue.remove(k);
    check_consistency(&queue);
    queue.remove(j);
    check_consistency(&queue);
    queue.remove(i);
    check_consistency(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn insert_and_remove_multiple_back() {
    let (_rx1, waker1) = make_waker();
    let (_rx2, waker2) = make_waker();
    let (_rx3, waker3) = make_waker();

    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    let j = queue.add_waker(waker2, None);
    check_consistency(&queue);
    let k = queue.add_waker(waker3, None);
    check_consistency(&queue);

    queue.remove(i);
    check_consistency(&queue);
    queue.remove(j);
    check_consistency(&queue);
    queue.remove(k);
    check_consistency(&queue);
    assert!(queue.poll().is_none());
}

#[test]
fn insert_and_remove_multiple_mixed() {
    let (_rx1, waker1) = make_waker();
    let (_rx2, waker2) = make_waker();
    let (_rx3, waker3) = make_waker();

    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    let j = queue.add_waker(waker2, None);
    check_consistency(&queue);
    let k = queue.add_waker(waker3, None);
    check_consistency(&queue);

    queue.remove(j);
    check_consistency(&queue);
    queue.remove(i);
    check_consistency(&queue);
    queue.remove(k);
    check_consistency(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[test]
fn remove_first_and_poll() {
    let (_rx1, waker1) = make_waker();
    let (_rx2, waker2) = make_waker();
    let (_rx3, waker3) = make_waker();

    let mut queue = WriterQueue::default();
    let i = queue.add_waker(waker1, None);
    check_consistency(&queue);
    queue.add_waker(waker2, None);
    check_consistency(&queue);
    queue.add_waker(waker3, None);
    check_consistency(&queue);

    queue.remove(i);
    check_consistency(&queue);
    assert!(queue.poll().is_some());
    check_consistency(&queue);
    assert!(queue.poll().is_some());
    check_consistency(&queue);
    assert!(queue.poll().is_none());
    check_consistency(&queue);
}

#[tokio::test]
async fn uncontended_read() {
    let rw_lock = RwLock::new(2);

    let lock = rw_lock.read().await;

    let i = *lock;

    assert_eq!(i, 2);
}

#[tokio::test]
async fn uncontended_write() {
    let rw_lock = RwLock::new(2);

    let mut lock = rw_lock.write().await;

    *lock = 7;

    drop(lock);

    let lock = rw_lock.read().await;
    let i = *lock;

    assert_eq!(i, 7);
}

#[tokio::test]
async fn read_waiting_on_write() {
    let (tx, rx) = oneshot::channel();

    let rw_lock1 = RwLock::new(0);
    let rw_lock2 = rw_lock1.clone();

    let read = async move {
        rx.await.expect("Channel dropped.");
        let lock = rw_lock1.read().await;
        let i = *lock;

        assert_eq!(i, 7);
    };

    let write = async move {
        let mut lock = rw_lock2.write().await;
        tx.send(()).expect("Channel dropped.");
        tokio::task::yield_now().await;
        *lock = 7;
        tokio::task::yield_now().await;
        drop(lock);
    };

    join(read, write).await;
}

#[tokio::test(threaded_scheduler)]
async fn read_waiting_on_write_threaded() {
    for _ in 0..REPEATS {
        let (tx, rx) = oneshot::channel();

        let rw_lock1 = RwLock::new(0);
        let rw_lock2 = rw_lock1.clone();

        let read = tokio::task::spawn(async move {
            rx.await.expect("Channel dropped.");
            let lock = rw_lock1.read().await;
            let i = *lock;

            assert_eq!(i, 7);
        });

        let write = tokio::task::spawn(async move {
            let mut lock = rw_lock2.write().await;
            tx.send(()).expect("Channel dropped.");
            tokio::task::yield_now().await;
            *lock = 7;
            tokio::task::yield_now().await;
            drop(lock);
        });

        let (result1, result2) = join(read, write).await;
        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }
}

#[tokio::test]
async fn write_waiting_on_write() {
    let (tx, rx) = oneshot::channel();

    let rw_lock = RwLock::new(0);
    let rw_lock1 = rw_lock.clone();
    let rw_lock2 = rw_lock.clone();

    let blocked = async move {
        rx.await.expect("Channel dropped.");
        let mut lock = rw_lock1.write().await;

        assert_eq!(*lock, 1);

        *lock = 2;
    };

    let write = async move {
        let mut lock = rw_lock2.write().await;
        assert_eq!(*lock, 0);
        tx.send(()).expect("Channel dropped.");
        tokio::task::yield_now().await;
        *lock = 1;
        tokio::task::yield_now().await;
        drop(lock);
    };

    join(blocked, write).await;

    let lock = rw_lock.read().await;
    let i = *lock;

    assert_eq!(i, 2);
}

#[tokio::test(threaded_scheduler)]
async fn write_waiting_on_write_threaded() {
    for _ in 0..REPEATS {
        let (tx, rx) = oneshot::channel();

        let rw_lock = RwLock::new(0);
        let rw_lock1 = rw_lock.clone();
        let rw_lock2 = rw_lock.clone();

        let blocked = tokio::task::spawn(async move {
            rx.await.expect("Channel dropped.");
            let mut lock = rw_lock1.write().await;

            assert_eq!(*lock, 1);

            *lock = 2;
        });

        let write = tokio::task::spawn(async move {
            let mut lock = rw_lock2.write().await;
            assert_eq!(*lock, 0);
            tx.send(()).expect("Channel dropped.");
            tokio::task::yield_now().await;
            *lock = 1;
            tokio::task::yield_now().await;
            drop(lock);
        });

        let (result1, result2) = join(blocked, write).await;
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        let lock = rw_lock.read().await;
        let i = *lock;

        assert_eq!(i, 2);
    }
}

#[tokio::test]
async fn write_waiting_on_single_read() {
    let (tx, rx) = oneshot::channel();

    let rw_lock = RwLock::new(1);
    let rw_lock1 = rw_lock.clone();
    let rw_lock2 = rw_lock.clone();

    let blocked = async move {
        rx.await.expect("Channel dropped.");
        let mut lock = rw_lock1.write().await;

        assert_eq!(*lock, 1);

        *lock = 2;
    };

    let read = async move {
        let lock = rw_lock2.read().await;
        assert_eq!(*lock, 1);
        tx.send(()).expect("Channel dropped.");
        tokio::task::yield_now().await;
        drop(lock);
    };

    join(blocked, read).await;

    let lock = rw_lock.read().await;
    let i = *lock;

    assert_eq!(i, 2);
}

#[tokio::test(threaded_scheduler)]
async fn write_waiting_on_single_read_threaded() {
    for _ in 0..REPEATS {
        let (tx, rx) = oneshot::channel();

        let rw_lock = RwLock::new(1);
        let rw_lock1 = rw_lock.clone();
        let rw_lock2 = rw_lock.clone();

        let blocked = tokio::task::spawn(async move {
            rx.await.expect("Channel dropped.");
            let mut lock = rw_lock1.write().await;

            assert_eq!(*lock, 1);

            *lock = 2;
        });

        let read = tokio::task::spawn(async move {
            let lock = rw_lock2.read().await;
            assert_eq!(*lock, 1);
            tx.send(()).expect("Channel dropped.");
            tokio::task::yield_now().await;
            drop(lock);
        });

        let (result1, result2) = join(blocked, read).await;
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        let lock = rw_lock.read().await;
        let i = *lock;

        assert_eq!(i, 2);
    }
}

#[tokio::test]
async fn write_waiting_on_multiple_reads() {
    let num_readers = 10;

    let mut tasks = vec![];
    let barrier = Arc::new(Barrier::new(num_readers + 1));
    let writer_barrier = barrier.clone();

    let rw_lock = RwLock::new(1);
    let rw_writer = rw_lock.clone();

    let blocked = async move {
        writer_barrier.wait().await;
        let mut lock = rw_writer.write().await;

        assert_eq!(*lock, 1);

        *lock = 2;
    };
    tasks.push(blocked.boxed());

    for _ in 0..num_readers {
        let rw_lock_cpy = rw_lock.clone();
        let reader_barrier = barrier.clone();
        let read_task = async move {
            let lock = rw_lock_cpy.read().await;
            assert_eq!(*lock, 1);
            reader_barrier.wait().await;
            tokio::task::yield_now().await;
            drop(lock);
        };
        tasks.push(read_task.boxed());
    }

    join_all(tasks.into_iter()).await;

    let lock = rw_lock.read().await;
    let i = *lock;

    assert_eq!(i, 2);
}

#[tokio::test(threaded_scheduler)]
async fn write_waiting_on_multiple_reads_threaded() {
    let num_readers = 10;

    let mut tasks = vec![];
    let barrier = Arc::new(Barrier::new(num_readers + 1));
    let writer_barrier = barrier.clone();

    let rw_lock = RwLock::new(1);
    let rw_writer = rw_lock.clone();

    let blocked = async move {
        writer_barrier.wait().await;
        let mut lock = rw_writer.write().await;

        assert_eq!(*lock, 1);

        *lock = 2;
    };
    tasks.push(tokio::task::spawn(blocked));

    for _ in 0..num_readers {
        let rw_lock_cpy = rw_lock.clone();
        let reader_barrier = barrier.clone();
        let read_task = async move {
            let lock = rw_lock_cpy.read().await;
            assert_eq!(*lock, 1);
            reader_barrier.wait().await;
            tokio::task::yield_now().await;
            drop(lock);
        };
        tasks.push(tokio::task::spawn(read_task));
    }

    let results = join_all(tasks.into_iter()).await;
    for result in results.into_iter() {
        assert!(result.is_ok());
    }

    let lock = rw_lock.read().await;
    let i = *lock;

    assert_eq!(i, 2);
}

#[tokio::test]
async fn multiple_writes_waiting_on_multiple_reads() {
    let num_readers = 10;

    let mut tasks = vec![];
    let barrier = Arc::new(Barrier::new(num_readers + 2));

    let rw_lock = RwLock::new(1);

    let (tx, rx) = oneshot::channel();

    let rw_writer1 = rw_lock.clone();
    let writer_barrier1 = barrier.clone();
    let blocked1 = async move {
        writer_barrier1.wait().await;
        rx.await.expect("Channel dropped.");
        let mut lock = rw_writer1.write().await;

        assert_eq!(*lock, 2);

        *lock = 3;
    };
    tasks.push(blocked1.boxed());

    let rw_writer2 = rw_lock.clone();
    let writer_barrier2 = barrier.clone();
    let blocked2 = async move {
        writer_barrier2.wait().await;
        let mut lock = rw_writer2.write().await;
        tx.send(()).expect("Channel dropped.");
        tokio::task::yield_now().await;

        assert_eq!(*lock, 1);

        *lock = 2;
        drop(lock);
    };
    tasks.push(blocked2.boxed());

    for _ in 0..num_readers {
        let rw_lock_cpy = rw_lock.clone();
        let reader_barrier = barrier.clone();
        let read_task = async move {
            let lock = rw_lock_cpy.read().await;
            assert_eq!(*lock, 1);
            reader_barrier.wait().await;
            tokio::task::yield_now().await;
            drop(lock);
        };
        tasks.push(read_task.boxed());
    }

    join_all(tasks.into_iter()).await;

    let lock = rw_lock.read().await;
    let i = *lock;

    assert_eq!(i, 3);
}

#[tokio::test(threaded_scheduler)]
async fn multiple_writes_waiting_on_multiple_reads_threaded() {
    for _ in 0..REPEATS {
        let num_readers = 10;

        let mut tasks = vec![];
        let barrier = Arc::new(Barrier::new(num_readers + 2));

        let rw_lock: RwLock<i32> = RwLock::new(1);

        let (tx, rx) = oneshot::channel();

        let rw_writer1 = rw_lock.clone();
        let writer_barrier1 = barrier.clone();
        let blocked1 = async move {
            writer_barrier1.wait().await;
            rx.await.expect("Channel dropped.");
            let mut lock = rw_writer1.write().await;

            assert_eq!(*lock, 2);

            *lock = 3;
        };
        tasks.push(tokio::task::spawn(blocked1));

        let rw_writer2 = rw_lock.clone();
        let writer_barrier2 = barrier.clone();
        let blocked2 = async move {
            writer_barrier2.wait().await;
            let mut lock = rw_writer2.write().await;
            tx.send(()).expect("Channel dropped.");
            tokio::task::yield_now().await;

            assert_eq!(*lock, 1);

            *lock = 2;
            drop(lock);
        };
        tasks.push(tokio::task::spawn(blocked2));

        for _ in 0..num_readers {
            let rw_lock_cpy = rw_lock.clone();
            let reader_barrier = barrier.clone();
            let read_task = async move {
                let lock = rw_lock_cpy.read().await;
                assert_eq!(*lock, 1);
                reader_barrier.wait().await;
                tokio::task::yield_now().await;
                drop(lock);
            };
            tasks.push(tokio::task::spawn(read_task));
        }

        let results = join_all(tasks.into_iter()).await;
        for result in results.into_iter() {
            assert!(result.is_ok());
        }

        let lock = rw_lock.read().await;
        let i = *lock;

        assert_eq!(i, 3);
    }
}

#[tokio::test]
async fn try_take_read_lock() {
    let rw_lock = RwLock::new(0);

    let read_lock1 = rw_lock.try_read();
    assert!(read_lock1.is_some());

    let read_lock2 = rw_lock.try_read();
    assert!(read_lock2.is_some());

    drop(read_lock1);
    drop(read_lock2);

    let _write_lock = rw_lock.write().await;

    let read_lock3 = rw_lock.try_read();
    assert!(read_lock3.is_none());
}

#[tokio::test]
async fn try_take_write_lock() {
    let rw_lock = RwLock::new(0);

    let write_lock1 = rw_lock.try_write();
    assert!(write_lock1.is_some());

    let write_lock2 = rw_lock.try_write();
    assert!(write_lock2.is_none());

    drop(write_lock1);
    drop(write_lock2);

    let _read_lock = rw_lock.read().await;

    let write_lock3 = rw_lock.try_write();
    assert!(write_lock3.is_none());
}
