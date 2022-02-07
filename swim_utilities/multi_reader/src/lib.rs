use byte_channel::ByteReader;
use futures_util::Stream;
use parking_lot::RwLock;
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
    ready_remote: Arc<RwLock<Vec<AtomicUsize>>>,
    ready_local: Vec<usize>,
}

impl<D: Decoder> MultiReader<D> {
    pub fn new() -> Self {
        MultiReader {
            readers: Slab::new(),
            ready_remote: Arc::new(RwLock::new(vec![AtomicUsize::new(0)])),
            ready_local: vec![0],
        }
    }

    pub fn add_reader(&mut self, reader: FramedRead<ByteReader, D>) {
        let key = self.readers.insert(reader);
        let bucket = key / usize::BITS as usize;

        match self.ready_local.get_mut(bucket) {
            Some(flags) => {
                *flags |= 1 << key;
            }
            None => {
                self.ready_local.insert(bucket, 1 << key);
                self.ready_remote
                    .write()
                    .insert(bucket, AtomicUsize::new(0));
            }
        }
    }

    fn next_ready(&mut self) -> Option<(usize, usize)> {
        for (bucket, flags) in self.ready_local.iter().enumerate() {
            if *flags != 0 {
                let index = flags.trailing_zeros() as usize;
                return Some((bucket, index));
            }
        }

        for (bucket, flags) in self.ready_remote.read().iter().enumerate() {
            *self.ready_local.get_mut(bucket).unwrap() = flags.fetch_and(0, Ordering::SeqCst);
        }

        for (bucket, flags) in self.ready_local.iter().enumerate() {
            if *flags != 0 {
                let index = flags.trailing_zeros() as usize;
                return Some((bucket, index));
            }
        }

        None
    }
}

impl<D: Decoder + Unpin> Stream for MultiReader<D> {
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some((bucket, index)) = self.next_ready() {
            let waker = cx.waker().clone();
            let ready = self.ready_remote.clone();

            if let Some(reader) = self.readers.get_mut(index + bucket * usize::BITS as usize) {
                let result =
                    Pin::new(reader).poll_next(&mut Context::from_waker(&waker_fn(move || {
                        ready
                            .read()
                            .get(bucket)
                            .unwrap()
                            .fetch_or(1 << index, Ordering::SeqCst);
                        // Wake up the parent task
                        waker.wake_by_ref();
                    })));

                if let Poll::Ready(result) = result {
                    match result {
                        Some(item) => {
                            // Add this reader to the back of the queue
                            return Poll::Ready(Some(item));
                        }
                        None => {
                            // The reader is closed and so we remove it.
                            self.readers.remove(index + bucket * usize::BITS as usize);
                        }
                    }
                }

                *self.ready_local.get_mut(bucket).unwrap() ^= 1 << index;
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
