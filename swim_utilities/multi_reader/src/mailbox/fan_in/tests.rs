// Copyright 2015-2021 Swim Inc.
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
