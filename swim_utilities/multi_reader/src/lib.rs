use byte_channel::ByteReader;
use futures_util::Stream;
use slab::Slab;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_util::codec::{Decoder, FramedRead};
use waker_fn::waker_fn;

#[cfg(test)]
mod tests;

pub struct MultiReader<D> {
    pub readers: Slab<FramedRead<ByteReader, D>>,
    ready_remote: Arc<AtomicUsize>,
    ready_local: usize,
}

impl<D: Decoder> MultiReader<D> {
    pub fn new() -> Self {
        MultiReader {
            readers: Slab::new(),
            ready_remote: Arc::new(AtomicUsize::new(0)),
            ready_local: 0,
        }
    }

    pub fn add_reader(&mut self, reader: FramedRead<ByteReader, D>) {
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

impl<D: Decoder + Unpin> Stream for MultiReader<D> {
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some(index) = self.next_ready() {
            let waker = cx.waker().clone();
            let ready = self.ready_remote.clone();

            if let Some(reader) = self.readers.get_mut(index) {
                let result =
                    Pin::new(reader).poll_next(&mut Context::from_waker(&waker_fn(move || {
                        ready.fetch_or(1 << index, Ordering::SeqCst);
                        // Wake up the parent task
                        waker.wake_by_ref();
                    })));

                if let Poll::Ready(result) = result {
                    match result {
                        Some(item) => {
                            return Poll::Ready(Some(item));
                        }
                        None => {
                            // The reader is closed and so we remove it.
                            self.readers.remove(index);
                        }
                    }
                }

                self.ready_local ^= 1 << index;
            }
        }

        if self.readers.is_empty() {
            // All readers are done so the multi reader is also done.
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
