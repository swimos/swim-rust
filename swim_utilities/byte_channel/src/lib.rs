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

#[cfg(test)]
mod tests;

use bytes::{Buf, BytesMut};
use parking_lot::Mutex;
use slab::Slab;
use std::fmt::{Debug, Formatter};
use std::io::{Error, ErrorKind, Result as IoResult};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use waker_fn::waker_fn;

pub struct MultiReader {
    readers: Slab<ByteReader>,
    tx: mpsc::UnboundedSender<usize>,
    rx: mpsc::UnboundedReceiver<usize>,
}

impl MultiReader {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        MultiReader {
            readers: Slab::new(),
            tx,
            rx,
        }
    }

    pub fn add_reader(&mut self, reader: ByteReader) {
        let key = self.readers.insert(reader);
        self.tx.send(key).expect("Channel closed unexpectedly!");
    }
}

impl AsyncRead for MultiReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let waker = cx.waker().clone();
        let tx = self.tx.clone();

        if let Poll::Ready(Some(index)) = self.rx.poll_recv(cx) {
            if let Some(reader) = self.readers.get_mut(index) {
                let buff_len = buf.filled().len();

                let result = Pin::new(reader).poll_read(
                    &mut Context::from_waker(&waker_fn(move || {
                        tx.send(index).expect("Channel closed unexpectedly!");
                        // Wake up the parent task
                        waker.wake_by_ref();
                    })),
                    buf,
                );

                if let Poll::Ready(result) = result {
                    match result {
                        Ok(_) if buff_len < buf.filled().len() => {
                            // Add this reader to the back of the queue
                            self.tx.send(index).expect("Channel closed unexpectedly!");
                            return Poll::Ready(Ok(()));
                        }
                        _ => {
                            // If the buffer has not been changed, it implies that EOF has been reached.
                            self.readers.remove(index);
                        }
                    }
                }
            }
        }

        if self.readers.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

/// A single producer, single consumer (SPSC) byte channel operated on using Tokio `AsyncRead` and
/// `AsyncWrite` implementations.
///
/// Internally, the shared data structure is initialised with a capacity of `buffer_size`.
///
/// # Note
/// Dropping either half will close the channel but any remaining data is available to be read by
/// the read half before an IO error is returned.
pub fn byte_channel(buffer_size: NonZeroUsize) -> (ByteWriter, ByteReader) {
    let closed = Arc::new(AtomicBool::new(false));
    let inner = Arc::new(Mutex::new(Conduit::new(buffer_size, closed.clone())));
    (
        ByteWriter {
            inner: inner.clone(),
            closed: closed.clone(),
        },
        ByteReader { inner, closed },
    )
}

struct Conduit {
    data: BytesMut,
    capacity: usize,
    waker: Option<Waker>,
    closed: Arc<AtomicBool>,
}

impl Debug for Conduit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Conduit {
            data,
            capacity,
            closed,
            ..
        } = self;
        f.debug_struct("Conduit")
            .field("data", data)
            .field("capacity", capacity)
            .field("closed", closed)
            .finish()
    }
}

impl Conduit {
    fn new(buffer_size: NonZeroUsize, closed: Arc<AtomicBool>) -> Conduit {
        let buffer_size = buffer_size.get();
        Conduit {
            data: BytesMut::with_capacity(buffer_size),
            capacity: buffer_size,
            waker: None,
            closed,
        }
    }

    #[inline]
    fn close_channel(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.wake();
    }

    #[inline]
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    #[inline]
    fn read(&mut self, buf: &mut ReadBuf<'_>, count: usize) {
        debug_assert!(buf.remaining() > 0);
        debug_assert!(count > 0);

        buf.put_slice(&self.data[..count]);
        self.data.advance(count);
        self.wake();
    }

    #[inline]
    fn write(&mut self, buf: &[u8], avail: usize) -> usize {
        debug_assert!(avail > 0);
        let len = buf.len().min(avail);
        self.data.extend_from_slice(&buf[..len]);
        self.wake();

        len
    }
}

impl AsyncRead for Conduit {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        if self.data.has_remaining() {
            let count = self.data.remaining().min(buf.remaining());
            if count > 0 {
                self.read(buf, count);
            }
            Poll::Ready(Ok(()))
        } else if self.closed.load(Ordering::Relaxed) {
            Poll::Ready(Ok(()))
        } else {
            self.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl AsyncWrite for Conduit {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        if self.closed.load(Ordering::SeqCst) {
            Poll::Ready(Err(ErrorKind::BrokenPipe.into()))
        } else if buf.is_empty() {
            Poll::Ready(Ok(0))
        } else {
            let available = self.capacity - self.data.len();
            if available == 0 {
                self.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }

            let len = self.write(buf, available);
            Poll::Ready(Ok(len))
        }
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<IoResult<()>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.close_channel();
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub struct ByteReader {
    inner: Arc<Mutex<Conduit>>,
    closed: Arc<AtomicBool>,
}

impl ByteReader {
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Drop for ByteReader {
    fn drop(&mut self) {
        let guard = &mut *(self.inner.lock());
        guard.close_channel();
    }
}

impl AsyncRead for ByteReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let inner = &mut *(self.inner.lock());
        Pin::new(inner).poll_read(cx, buf)
    }
}

pub struct ByteWriter {
    inner: Arc<Mutex<Conduit>>,
    closed: Arc<AtomicBool>,
}

impl ByteWriter {
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Debug for ByteWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ByteWriter { inner, closed } = self;
        match inner.try_lock() {
            Some(guard) => f
                .debug_struct("ByteWriter")
                .field("value", &*guard)
                .field("closed", closed)
                .finish(),
            None => {
                write!(f, "ByteWriter")
            }
        }
    }
}

impl Drop for ByteWriter {
    fn drop(&mut self) {
        let inner = &mut *(self.inner.lock());
        inner.close_channel();
    }
}

impl AsyncWrite for ByteWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let inner = &mut *(self.inner.lock());
        Pin::new(inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let inner = &mut *(self.inner.lock());
        Pin::new(inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let inner = &mut *(self.inner.lock());
        Pin::new(inner).poll_shutdown(cx)
    }
}
