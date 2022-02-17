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
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::codec::{Decoder, FramedRead};
use waker_fn::waker_fn;

#[cfg(test)]
mod tests;


/// A reader combinator capable of reading from many framed readers concurrently.
pub struct MultiReader<D> {
    /// A collection of all framed readers.
    readers: Slab<FramedRead<ByteReader, D>>,
    /// A queue of readers that are ready.
    queue: VecDeque<usize>,
}

impl<D: Decoder> MultiReader<D> {
    pub fn new() -> Self {
        MultiReader {
            readers: Slab::new(),
            queue: VecDeque::new(),
        }
    }

    /// Add a reader to be polled when ready.
    pub fn add_reader(&mut self, reader: FramedRead<ByteReader, D>) {
        let key = self.readers.insert(reader);
        self.queue.push_back(key);
    }
}

impl<D: Decoder + Unpin> Stream for MultiReader<D> {
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some(index) = self.queue.pop_front() {
            let waker = cx.waker().clone();
            let queue_ptr = QueuePtr(&mut self.queue as *mut VecDeque<usize>);

            if let Some(reader) = self.readers.get_mut(index) {
                let result =
                    Pin::new(reader).poll_next(&mut Context::from_waker(&waker_fn(move || {
                        let mut queue = queue_ptr;
                        queue.push_back(index);
                        waker.wake_by_ref();
                    })));

                if let Poll::Ready(result) = result {
                    match result {
                        Some(item) => {
                            self.queue.push_back(index);
                            return Poll::Ready(Some(item));
                        }
                        None => {
                            // The reader is closed and so we remove it.
                            self.readers.remove(index);
                        }
                    }
                }
            }
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

#[derive(Debug, Clone, Copy)]
struct QueuePtr(*mut VecDeque<usize>);

impl Deref for QueuePtr {
    type Target = VecDeque<usize>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}

impl DerefMut for QueuePtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.0 }
    }
}

unsafe impl Send for QueuePtr {}

unsafe impl Sync for QueuePtr {}