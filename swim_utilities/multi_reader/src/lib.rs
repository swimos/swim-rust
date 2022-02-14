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

const BUCKET_SIZE: usize = usize::BITS as usize;

type Index = usize;

/// Flags indicating when a reader is ready to be polled, stored
/// as an integer. Each bit of the integer shows if the reader
/// with the same index is ready.
struct LocalFlags(usize);

impl LocalFlags {
    /// Return the index of the next reader that is ready to be polled.
    #[inline(always)]
    fn get_next(&mut self) -> Option<usize> {
        if self.is_empty() {
            None
        } else {
            let index = self.0.trailing_zeros() as usize;
            self.unset_flag(index);
            Some(index)
        }
    }

    /// Set all bits of the flags.
    #[inline(always)]
    fn set_all_flags(&mut self, flags: usize) {
        self.0 = flags;
    }

    /// Set the bit at the specified position to 1 (ready).
    #[inline(always)]
    fn set_flag(&mut self, index: usize) {
        self.0 |= 1 << index
    }

    /// Set the bit at the specified position to 0 (pending).
    #[inline(always)]
    fn unset_flag(&mut self, index: usize) {
        self.0 ^= 1 << index
    }

    /// Check if all of the bits are set to 0 (pending).
    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

/// A collection of flags indicating whether a reader is ready or pending.
struct ReaderBuckets(SmallVec<[Arc<AtomicUsize>; 4]>);

impl ReaderBuckets {
    fn new() -> Self {
        ReaderBuckets(smallvec![Arc::new(AtomicUsize::new(0))])
    }

    /// Set the reader with the given index in the given bucket as ready.
    /// The index is the relative index in the bucket and not an absolute index.
    #[inline(always)]
    fn set(&mut self, bucket: usize, index: usize) {
        match self.0.get(bucket) {
            Some(flags) => {
                flags.fetch_or(1 << index, Ordering::SeqCst);
            }
            None => {
                self.0
                    .insert(bucket, Arc::new(AtomicUsize::new(1 << index)));
            }
        }
    }

    /// Get all flags for a given bucket.
    #[inline(always)]
    fn get(&self, bucket: usize) -> Option<&Arc<AtomicUsize>> {
        self.0.get(bucket)
    }

    /// Return the number of buckets.
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// A reader combinator capable of reading from many framed readers concurrently.
pub struct MultiReader<D> {
    /// A collection of all framed readers.
    readers: Slab<FramedRead<ByteReader, D>>,
    /// A bucket of flags set by the readers whenever they are ready to be polled.
    reader_buckets: ReaderBuckets,
    /// A local copy of the reader flags from the current bucket.
    local_flags: LocalFlags,
    /// The index of the current bucket of readers.
    current_bucket: usize,
    /// The next reader that is ready to be polled.
    ready_reader: Option<Index>,
}

impl<D: Decoder> MultiReader<D> {
    pub fn new() -> Self {
        MultiReader {
            readers: Slab::new(),
            reader_buckets: ReaderBuckets::new(),
            local_flags: LocalFlags(0),
            current_bucket: 0,
            ready_reader: None,
        }
    }

    /// Add a reader to be polled when ready.
    pub fn add_reader(&mut self, reader: FramedRead<ByteReader, D>) {
        let key = self.readers.insert(reader);
        let bucket = key / BUCKET_SIZE;
        let index = key % BUCKET_SIZE;

        if bucket == self.current_bucket {
            self.local_flags.set_flag(index);
            if self.ready_reader.is_none() {
                self.ready_reader = Some(index);
            }
        } else {
            self.reader_buckets.set(bucket, index);
        }
    }

    fn get_next_reader(&mut self) -> Option<usize> {
        self.ready_reader.or_else(|| {
            self.update_next_reader();
            self.ready_reader
        })
    }

    fn update_next_reader(&mut self) {
        if self.local_flags.is_empty() {
            let starting_idx = self.current_bucket;

            loop {
                self.current_bucket += 1;

                if self.reader_buckets.len() <= self.current_bucket {
                    self.current_bucket = 0;
                }

                self.local_flags.set_all_flags(
                    self.reader_buckets
                        .get(self.current_bucket)
                        .expect("Invalid bucket")
                        .fetch_and(0, Ordering::SeqCst),
                );

                if !self.local_flags.is_empty() {
                    break;
                }

                // If we are back at the start and there is nothing, it means that everything is empty.
                if starting_idx == self.current_bucket {
                    self.ready_reader = None;
                    return;
                }
            }
        }

        self.ready_reader = self.local_flags.get_next();
    }
}

impl<D: Decoder + Unpin> Stream for MultiReader<D> {
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some(index) = self.get_next_reader() {
            let waker = cx.waker().clone();
            let bucket = self.current_bucket;

            let ready = self
                .reader_buckets
                .get(bucket)
                .expect("Invalid bucket")
                .clone();

            if let Some(reader) = self.readers.get_mut(index + bucket * BUCKET_SIZE) {
                let result =
                    Pin::new(reader).poll_next(&mut Context::from_waker(&waker_fn(move || {
                        ready.fetch_or(1 << index, Ordering::SeqCst);
                        waker.wake_by_ref();
                    })));

                if let Poll::Ready(result) = result {
                    match result {
                        Some(item) => {
                            return Poll::Ready(Some(item));
                        }
                        None => {
                            // The reader is closed and so we remove it.
                            self.readers.remove(index + bucket * BUCKET_SIZE);
                        }
                    }
                }
            }

            // Stop polling the current reader.
            self.ready_reader = None;
        }

        if self.readers.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<D: Decoder> Default for MultiReader<D> {
    fn default() -> Self {
        Self::new()
    }
}
