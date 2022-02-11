use byte_channel::ByteReader;
use futures_util::Stream;
use slab::Slab;
use smallvec::{smallvec, SmallVec};
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
    ready_remote: SmallVec<[Arc<AtomicUsize>; 4]>,
    ready_local_flags: usize,
    ready_local_bucket: usize,
    next: Option<(usize, usize)>,
}

impl<D: Decoder> MultiReader<D> {
    pub fn new() -> Self {
        MultiReader {
            readers: Slab::new(),
            ready_remote: smallvec![Arc::new(AtomicUsize::new(0))],
            ready_local_flags: 0,
            ready_local_bucket: 0,
            next: None,
        }
    }

    pub fn add_reader(&mut self, reader: FramedRead<ByteReader, D>) {
        let key = self.readers.insert(reader);
        let bucket = key / usize::BITS as usize;
        let index = key % usize::BITS as usize;

        if bucket == self.ready_local_bucket {
            self.ready_local_flags |= 1 << index;
            if self.next.is_none() {
                self.next = Some((bucket, index));
            }
        } else {
            match self.ready_remote.get(bucket) {
                Some(flags) => {
                    flags.fetch_or(1 << index, Ordering::SeqCst);
                }
                None => {
                    self.ready_remote
                        .insert(bucket, Arc::new(AtomicUsize::new(1 << index)));
                }
            }
        }
    }

    fn get_next_ready(&mut self) -> Option<(usize, usize)> {
        self.next.or_else(|| {
            self.update_next();
            self.next
        })
    }

    fn update_next(&mut self) {
        if self.ready_local_flags == 0 {
            let starting_idx = self.ready_local_bucket;

            loop {
                // Go to the next bucket.
                self.ready_local_bucket += 1;

                // Wrap around if we are past the end.
                if self.ready_remote.len() <= self.ready_local_bucket {
                    self.ready_local_bucket = 0;
                }

                // Read the bucket and check if it has any ready flags.
                self.ready_local_flags = self
                    .ready_remote
                    .get(self.ready_local_bucket)
                    .expect("Invalid bucket")
                    .fetch_and(0, Ordering::SeqCst);

                if self.ready_local_flags != 0 {
                    break;
                }

                // If we are back at the start and there is nothing, it means that everything is empty.
                if starting_idx == self.ready_local_bucket {
                    self.next = None;
                    return;
                }
            }
        }

        let index = self.ready_local_flags.trailing_zeros() as usize;
        self.next = Some((self.ready_local_bucket, index));
        self.ready_local_flags ^= 1 << index;
    }
}

impl<D: Decoder + Unpin> Stream for MultiReader<D> {
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some((bucket, index)) = self.get_next_ready() {
            let waker = cx.waker().clone();
            let ready = self
                .ready_remote
                .get(bucket)
                .expect("Invalid bucket")
                .clone();

            if let Some(reader) = self.readers.get_mut(index + bucket * usize::BITS as usize) {
                let result =
                    Pin::new(reader).poll_next(&mut Context::from_waker(&waker_fn(move || {
                        ready.fetch_or(1 << index, Ordering::SeqCst);
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
            }

            // Stop polling the current reader.
            self.next = None;
        }

        if self.readers.is_empty() {
            // All readers are done so the multi reader is also done.
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
