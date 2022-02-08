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
    readers: Slab<FramedRead<ByteReader, D>>,
    ready_remote_buckets: Arc<RwLock<Vec<AtomicUsize>>>,
    ready_local_flags: usize,
    ready_local_bucket: usize,
    buckets_count: usize,
}

impl<D: Decoder> MultiReader<D> {
    pub fn new() -> Self {
        MultiReader {
            readers: Slab::new(),
            ready_remote_buckets: Arc::new(RwLock::new(vec![AtomicUsize::new(0)])),
            ready_local_flags: 0,
            ready_local_bucket: 0,
            buckets_count: 1,
        }
    }

    pub fn add_reader(&mut self, reader: FramedRead<ByteReader, D>) {
        let key = self.readers.insert(reader);
        let bucket = key / usize::BITS as usize;
        let index = key % usize::BITS as usize;

        if bucket == self.ready_local_bucket {
            self.ready_local_flags |= 1 << index;
        } else {
            if let Some(flags) = self.ready_remote_buckets.read().get(bucket) {
                flags.fetch_or(1 << index, Ordering::SeqCst);
                return;
            };
            self.ready_remote_buckets
                .write()
                .insert(bucket, AtomicUsize::new(1 << index));
            self.buckets_count += 1;
        }
    }

    fn next_ready(&mut self) -> Option<(usize, usize)> {
        if self.ready_local_flags == 0 {
            let starting_idx = self.ready_local_bucket;

            let remote_buckets = self.ready_remote_buckets.read();

            loop {
                if self.buckets_count > 1 {
                    // Go to the next bucket.
                    self.ready_local_bucket += 1;

                    // Wrap around if we are past the end.
                    if self.buckets_count <= self.ready_local_bucket {
                        self.ready_local_bucket = 0;
                    }
                }

                // Read the bucket and check if it has any ready flags.
                self.ready_local_flags = remote_buckets
                    .get(self.ready_local_bucket)
                    .expect("Invalid bucket")
                    .fetch_and(0, Ordering::SeqCst);

                if self.ready_local_flags != 0 {
                    break;
                }

                // If we are back at the start and there is nothing, it means that everything is empty.
                if starting_idx == self.ready_local_bucket {
                    return None;
                }
            }
        }

        let index = self.ready_local_flags.trailing_zeros() as usize;
        Some((self.ready_local_bucket, index))
    }
}

impl<D: Decoder + Unpin> Stream for MultiReader<D> {
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some((bucket, index)) = self.next_ready() {
            let waker = cx.waker().clone();
            let ready = self.ready_remote_buckets.clone();

            if let Some(reader) = self.readers.get_mut(index + bucket * usize::BITS as usize) {
                let result =
                    Pin::new(reader).poll_next(&mut Context::from_waker(&waker_fn(move || {
                        ready
                            .read()
                            .get(bucket)
                            .expect("Invalid bucket")
                            .fetch_or(1 << index, Ordering::SeqCst);
                        // Wake up the parent task.
                        waker.wake_by_ref();
                    })));

                if let Poll::Ready(result) = result {
                    match result {
                        Some(item) => {
                            return Poll::Ready(Some(item));
                        }
                        None => {
                            // The reader is closed and so we remove it.
                            self.readers.remove(index + bucket * usize::BITS as usize);
                        }
                    }
                }

                // Unset the ready flag of this reader.
                self.ready_local_flags ^= 1 << index;
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
