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

use crate::{byte_channel, ByteReader};
use bytes::BytesMut;
use futures::future::join;
use futures::FutureExt;
use futures_util::stream::FuturesUnordered;
use rand::Rng;
use slab::Slab;
use std::future::Future;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::time::{sleep, timeout};
use waker_fn::waker_fn;

const BYTE_CHANNEL_LEN: usize = 4096;
const DATA_LEN: usize = 1048576;
const CHUNK_LEN: usize = 1024;

struct MultiReader {
    readers: Slab<ByteReader>,
    ready_remote: Arc<AtomicUsize>,
    ready_local: usize,
}

impl MultiReader {
    fn new() -> Self {
        MultiReader {
            readers: Slab::new(),
            ready_remote: Arc::new(AtomicUsize::new(0)),
            ready_local: 0,
        }
    }

    fn add_reader(&mut self, reader: ByteReader) {
        let key = self.readers.insert(reader);
        self.ready_local |= 1 << key;
    }

    fn next_ready(&mut self) -> Option<usize> {
        if self.ready_local == 0 {
            self.ready_local = self.ready_remote.fetch_and(0, Ordering::SeqCst);

            if self.ready_local == 0 {
                return None;
            }
        }

        let index = self.ready_local.trailing_zeros() as usize;
        Some(index)
    }
}

impl AsyncRead for MultiReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        while let Some(index) = self.next_ready() {
            let waker = cx.waker().clone();
            let ready = self.ready_remote.clone();

            if let Some(reader) = self.readers.get_mut(index) {
                let buff_len = buf.filled().len();

                let result = Pin::new(reader).poll_read(
                    &mut Context::from_waker(&waker_fn(move || {
                        ready.fetch_or(1 << index, Ordering::SeqCst);
                        // Wake up the parent task
                        waker.wake_by_ref();
                    })),
                    buf,
                );

                if let Poll::Ready(result) = result {
                    match result {
                        Ok(_) if buff_len < buf.filled().len() => {
                            return Poll::Ready(Ok(()));
                        }
                        _ => {
                            self.readers.remove(index);
                        }
                    }
                }

                self.ready_local ^= 1 << index;
            }
        }

        if self.readers.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

#[tokio::test]
async fn bar() {
    let (mut writer, mut reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let write = async move {
        writer.write("foo".as_bytes()).await;
        sleep(Duration::from_secs(1)).await;
        drop(writer);
        sleep(Duration::from_secs(1)).await;
    };

    let read = async move {
        let mut buf = BytesMut::new();
        let val = reader.read_buf(&mut buf).await;
        eprintln!("val = {:?}", val);
        let val = reader.read_buf(&mut buf).await;
    };

    join(timeout(Duration::from_secs(10), read), write).await;
}

#[tokio::test]
async fn foo() {
    let (mut first_writer, mut first_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (mut second_writer, mut second_reader) = byte_channel(NonZeroUsize::new(16).unwrap());
    let (mut third_writer, mut third_reader) = byte_channel(NonZeroUsize::new(16).unwrap());

    let mut multi_reader = MultiReader::new();

    multi_reader.add_reader(first_reader);
    multi_reader.add_reader(second_reader);
    multi_reader.add_reader(third_reader);

    let write = async move {
        sleep(Duration::from_secs(1)).await;
        let a = first_writer.write("foo".as_bytes()).await;
        println!("sent foo 1");
        sleep(Duration::from_secs(1)).await;
        let a = second_writer.write("bar".as_bytes()).await;
        println!("sent bar 1");
        sleep(Duration::from_secs(1)).await;
        let a = first_writer.write("foo2".as_bytes()).await;
        println!("sent foo 2");
        sleep(Duration::from_secs(1)).await;
        let a = second_writer.write("bar2".as_bytes()).await;
        println!("sent bar 2");
        sleep(Duration::from_secs(1)).await;
        drop(first_writer);
        drop(second_writer);
        drop(third_writer);
        sleep(Duration::from_secs(1)).await;
    };

    let read = async move {
        let mut buf = BytesMut::new();
        // sleep(Duration::from_secs(2)).await;
        let c = multi_reader.read_buf(&mut buf).await;
        eprintln!("c = {:?}", c);
        eprintln!("buf = {:?}", buf);
        let c = multi_reader.read_buf(&mut buf).await;
        eprintln!("c = {:?}", c);
        eprintln!("buf = {:?}", buf);
        let c = multi_reader.read_buf(&mut buf).await;
        eprintln!("c = {:?}", c);
        eprintln!("buf = {:?}", buf);
        let c = multi_reader.read_buf(&mut buf).await;
        eprintln!("c = {:?}", c);
        eprintln!("buf = {:?}", buf);
        let c = multi_reader.read_buf(&mut buf).await;
        eprintln!(
            "multi_reader.readers.len() = {:?}",
            multi_reader.readers.len()
        );
    };

    // read.await;
    // timeout(Duration::from_secs(10), read).await;

    join(timeout(Duration::from_secs(10), read), write).await;
}

#[tokio::test]
async fn simple_send_recv() {
    let (mut tx, mut rx) = super::byte_channel(NonZeroUsize::new(16).unwrap());
    let res = tx.write(&[0, 1, 2, 3]).await;
    assert!(matches!(res, Ok(4)));

    let mut buf = BytesMut::new();

    let res = rx.read_buf(&mut buf).await;
    assert!(matches!(res, Ok(4)));

    assert_eq!(buf.as_ref(), &[0, 1, 2, 3]);
}

#[tokio::test]
async fn close_writer_empty() {
    let (tx, mut rx) = super::byte_channel(NonZeroUsize::new(16).unwrap());

    drop(tx);
    let mut buf = BytesMut::new();
    let res = rx.read_buf(&mut buf).await;

    assert!(matches!(res, Ok(0)));
}

#[tokio::test]
async fn close_reader_empty() {
    let (mut tx, rx) = super::byte_channel(NonZeroUsize::new(16).unwrap());
    drop(rx);

    let res = tx.write(&[0, 1, 2, 3]).await;

    if let Err(e) = res {
        assert_eq!(e.kind(), ErrorKind::BrokenPipe);
    } else {
        panic!("Should fail.");
    }
}

#[tokio::test]
async fn read_unblocks_write() {
    let (mut tx, mut rx) = super::byte_channel(NonZeroUsize::new(7).unwrap());
    let res = tx.write(&[0, 1, 2, 3, 4, 5, 6]).await;
    assert!(res.is_ok());

    let mut buf: [u8; 4] = [0, 0, 0, 0];
    let mut buf_ref: &mut [u8] = &mut buf;

    let mut write = WasPending::new(Box::pin(tx.write(&[7, 8])));

    let (r1, r2) = join(&mut write, rx.read_buf(&mut buf_ref)).await;

    write.assert_was_pending();

    assert!(matches!(r1, Ok(2)));
    assert!(matches!(r2, Ok(4)));

    assert_eq!(&buf, &[0, 1, 2, 3]);

    let mut buf = BytesMut::new();
    let res = rx.read_buf(&mut buf).await;
    assert!(matches!(res, Ok(5)));
    assert_eq!(buf.as_ref(), &[4, 5, 6, 7, 8]);
}

#[tokio::test]
async fn write_unblocks_read() {
    let (mut tx, mut rx) = super::byte_channel(NonZeroUsize::new(8).unwrap());

    let mut buf = BytesMut::new();

    let mut read = WasPending::new(Box::pin(rx.read_buf(&mut buf)));

    let (r1, r2) = join(&mut read, tx.write(&[0, 1, 2, 3])).await;

    read.assert_was_pending();

    assert!(matches!(r1, Ok(4)));
    assert!(matches!(r2, Ok(4)));

    assert_eq!(buf.as_ref(), &[0, 1, 2, 3]);
}

#[tokio::test]
async fn reader_sees_data_written_before_writer_cloed() {
    let (mut tx, mut rx) = super::byte_channel(NonZeroUsize::new(8).unwrap());

    assert!(tx.write(&[0, 1, 2, 3]).await.is_ok());
    drop(tx);

    let mut buf = BytesMut::new();

    let r1 = rx.read_buf(&mut buf).await;
    assert!(matches!(r1, Ok(4)));
    assert_eq!(buf.as_ref(), &[0, 1, 2, 3]);

    let r2 = rx.read_buf(&mut buf).await;

    assert!(matches!(r2, Ok(0)));
}

#[tokio::test]
async fn read_into_empty_target() {
    let (mut tx, mut rx) = super::byte_channel(NonZeroUsize::new(8).unwrap());

    assert!(tx.write(&[0, 1, 2, 3]).await.is_ok());

    let mut empty = [];
    let mut buf: &mut [u8] = &mut empty;

    let res = rx.read_buf(&mut buf).await;
    assert!(matches!(res, Ok(0)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_bulk() {
    let mut rnd = rand::thread_rng();
    let data: Box<[u8]> = std::iter::from_fn(|| Some(rnd.gen::<u8>()))
        .take(DATA_LEN)
        .collect();

    let data_ref: &[u8] = data.as_ref();

    let (mut tx, mut rx) = super::byte_channel(NonZeroUsize::new(BYTE_CHANNEL_LEN).unwrap());

    let send = async move {
        for chunk in data_ref.chunks(CHUNK_LEN) {
            assert!(tx.write_all(chunk).await.is_ok());
        }
    };

    let recv = async move {
        let mut buf = BytesMut::new();
        buf.reserve(DATA_LEN);
        loop {
            match rx.read_buf(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(e) => panic!("Read error: {:?}", e),
            }
        }
        buf.to_vec()
    };

    let (_, received_data) = timeout(Duration::from_secs(10), join(send, recv))
        .await
        .expect("Transfer likely deadlocked.");

    assert_eq!(received_data.as_slice(), data.as_ref());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_bulk_multi_threaded() {
    let mut rnd = rand::thread_rng();
    let data: Arc<[u8]> = std::iter::from_fn(|| Some(rnd.gen::<u8>()))
        .take(DATA_LEN)
        .collect();

    let data_cpy = data.clone();

    let (mut tx, mut rx) = super::byte_channel(NonZeroUsize::new(BYTE_CHANNEL_LEN).unwrap());

    let send = async move {
        for chunk in data_cpy.chunks(CHUNK_LEN) {
            assert!(tx.write_all(chunk).await.is_ok());
        }
    };

    let recv = async move {
        let mut buf = BytesMut::new();
        buf.reserve(DATA_LEN);
        loop {
            match rx.read_buf(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(e) => panic!("Read error: {:?}", e),
            }
        }
        buf.to_vec()
    };

    let (send_res, recv_res) = timeout(
        Duration::from_secs(10),
        join(tokio::spawn(send), tokio::spawn(recv)),
    )
    .await
    .expect("Transfer likely deadlocked.");

    assert!(send_res.is_ok());
    let received_data = recv_res.expect("Receive task paniced.");
    assert_eq!(received_data.as_slice(), data.as_ref());
}

struct WasPending<F>(F, bool);

impl<F> WasPending<F> {
    fn new(f: F) -> Self {
        WasPending(f, false)
    }

    fn assert_was_pending(&self) {
        assert!(self.1);
    }
}

impl<F: Future + Unpin> Future for WasPending<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.as_mut().get_mut().0.poll_unpin(cx);
        if result.is_pending() {
            self.1 = true;
        }
        result
    }
}
