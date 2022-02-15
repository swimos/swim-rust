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

#![allow(warnings)]

pub mod mailbox;

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

struct LocalFlags(usize);

impl LocalFlags {
    #[inline(always)]
    fn get_next(&mut self) -> usize {
        let index = self.0.trailing_zeros() as usize;
        self.0 ^= 1 << index;
        index
    }

    #[inline(always)]
    fn set_all_flags(&mut self, flags: usize) {
        self.0 = flags;
    }

    #[inline(always)]
    fn set_flag(&mut self, index: usize) {
        self.0 |= 1 << index
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

struct ReaderBuckets(SmallVec<[Arc<AtomicUsize>; 4]>);

impl ReaderBuckets {
    fn new() -> Self {
        ReaderBuckets(smallvec![Arc::new(AtomicUsize::new(0))])
    }

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

    #[inline(always)]
    fn get(&self, bucket: usize) -> Option<&Arc<AtomicUsize>> {
        self.0.get(bucket)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct MultiReader<D> {
    readers: Slab<FramedRead<ByteReader, D>>,
    reader_buckets: ReaderBuckets,
    local_flags: LocalFlags,
    current_bucket: usize,
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
                // Go to the next bucket.
                self.current_bucket += 1;

                // Wrap around if we are past the end.
                if self.reader_buckets.len() <= self.current_bucket {
                    self.current_bucket = 0;
                }

                // Read the bucket and check if it has any ready flags.
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

        self.ready_reader = Some(self.local_flags.get_next());
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
                            self.readers.remove(index + bucket * BUCKET_SIZE);
                        }
                    }
                }
            }

            // Stop polling the current reader.
            self.ready_reader = None;
        }

        if self.readers.is_empty() {
            // All readers are done so the multi reader is also done.
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
