use crate::mailbox::core::{queue, Node, QueueResult};
use crate::mailbox::fan_in::mailbox_channel;
use bytes::BytesMut;
use concurrent_queue::ConcurrentQueue;
use futures::future::join;
use futures::StreamExt;
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::collections::{LinkedList, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicPtr, AtomicU8};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio_util::codec::{Decoder, FramedRead};

struct StubDecoder;
impl Decoder for StubDecoder {
    type Item = Vec<u8>;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        //println!("Decode");

        if src.is_empty() {
            Ok(None)
        } else {
            let r = src.to_vec();
            src.clear();
            Ok(Some(r))
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn t() {
    let (registrar, mut rx) = mailbox_channel(NonZeroUsize::new(64).unwrap());

    let count: u8 = 2;
    let idx = Arc::new(AtomicU8::new(count));

    for i in 0..count {
        let tx = registrar.register().unwrap();
        let mine = idx.clone();
        tokio::spawn(async move {
            tx.send(vec![i + 1, i + 2, i + 3]).await.unwrap();
            let prev = mine.fetch_sub(1, Ordering::Relaxed);
            if prev - 1 == 0 {
                println!("Writers complete");
            }
        });
    }

    let b = async move {
        let mut decoder = FramedRead::new(rx, StubDecoder);

        loop {
            let r = decoder.next().await.unwrap().unwrap();
            if r.is_empty() {
                break;
            } else {
                println!("{:?}", r);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };

    b.await
}

#[test]
fn queue_has_next() {
    let (queue_tx, queue_rx) = queue();

    assert!(!queue_rx.has_next());

    queue_tx.push(());
    assert!(queue_rx.has_next());

    queue_tx.push(());
    assert!(queue_rx.has_next());

    assert!(queue_rx.pop().is_some());
    assert!(queue_rx.has_next());

    assert!(queue_rx.pop().is_some());
    assert!(!queue_rx.has_next());

    assert!(queue_rx.pop().is_none());
    assert!(!queue_rx.has_next());
}

#[test]
fn queue_head_ref() {
    let (queue_tx, queue_rx) = queue::<i32>();

    assert!(queue_rx.head_ref().is_none());

    queue_tx.push(1);
    assert_eq!(queue_rx.head_ref(), QueueResult::Data(&1));

    queue_tx.push(2);
    assert_eq!(queue_rx.head_ref(), QueueResult::Data(&1));

    assert_eq!(queue_rx.pop(), QueueResult::Data(1));
    assert_eq!(queue_rx.head_ref(), QueueResult::Data(&2));

    assert_eq!(queue_rx.pop(), QueueResult::Data(2));
    assert_eq!(queue_rx.head_ref(), QueueResult::Empty);

    assert_eq!(queue_rx.pop(), QueueResult::Empty);
    assert_eq!(queue_rx.head_ref(), QueueResult::Empty);

    queue_tx.push(3);
    assert_eq!(queue_rx.head_ref(), QueueResult::Data(&3));
}

#[test]
fn bm() {
    let count = 100_000;

    {
        let now = std::time::Instant::now();
        let (queue_tx, queue_rx) = queue::<i32>();

        for i in 0..count {
            queue_tx.push(i);
        }

        for i in 0..count {
            match queue_rx.pop() {
                QueueResult::Data(a) => {
                    assert_eq!(a, i)
                }
                r => panic!("{:?}", r),
            }
        }

        assert!(queue_rx.pop().is_none());
        println!("1: {}", (std::time::Instant::now() - now).as_nanos());
    }
    {
        let now = std::time::Instant::now();
        let mut deque = VecDeque::new();

        for i in 0..count {
            deque.push_back(i);
        }

        for i in 0..count {
            assert_eq!(deque.pop_front(), Some(i));
        }

        assert!(deque.pop_front().is_none());
        println!("2: {}", (std::time::Instant::now() - now).as_nanos());
    }
    {
        let now = std::time::Instant::now();
        let mut ll = LinkedList::new();

        for i in 0..count {
            ll.push_back(i);
        }

        for i in 0..count {
            assert_eq!(ll.pop_front(), Some(i));
        }

        assert!(ll.pop_front().is_none());
        println!("3: {}", (std::time::Instant::now() - now).as_nanos());
    }
    {
        let now = std::time::Instant::now();
        let mut ll = ConcurrentQueue::unbounded();

        for i in 0..count {
            ll.push(i).unwrap();
        }

        for i in 0..count {
            assert_eq!(ll.pop(), Ok(i));
        }

        assert!(ll.pop().is_err());
        println!("4: {}", (std::time::Instant::now() - now).as_nanos());
    }
    {
        let now = std::time::Instant::now();
        let mut deque = Arc::new(Mutex::new(VecDeque::new()));

        for i in 0..count {
            let deque = &mut *deque.lock();
            deque.push_back(i);
        }

        for i in 0..count {
            let deque = &mut *deque.lock();
            assert_eq!(deque.pop_front(), Some(i));
        }

        let deque = &mut *deque.lock();
        assert!(deque.pop_front().is_none());
        println!("5: {}", (std::time::Instant::now() - now).as_nanos());
    }
}
