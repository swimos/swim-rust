use byte_channel::ByteReader;
use futures_util::Stream;
use parking_lot::Mutex;
use slab::Slab;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_util::codec::{Decoder, FramedRead};
use waker_fn::waker_fn;

#[cfg(test)]
mod tests;

pub struct MultiReader<D> {
    pub readers: Slab<FramedRead<ByteReader, D>>,
    queue: Arc<Mutex<VecDeque<usize>>>,
}

impl<D: Decoder> MultiReader<D> {
    pub fn new() -> Self {
        MultiReader {
            readers: Slab::new(),
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn add_reader(&mut self, reader: FramedRead<ByteReader, D>) {
        let key = self.readers.insert(reader);
        self.queue.lock().push_back(key);
    }
}

impl<D: Decoder + Unpin> Stream for MultiReader<D> {
    type Item = Result<D::Item, D::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut maybe_index = self.queue.lock().pop_front();
        while let Some(index) = maybe_index {
            let waker = cx.waker().clone();
            let queue = self.queue.clone();

            if let Some(reader) = self.readers.get_mut(index) {
                let result =
                    Pin::new(reader).poll_next(&mut Context::from_waker(&waker_fn(move || {
                        queue.lock().push_back(index);
                        // Wake up the parent task
                        waker.wake_by_ref();
                    })));

                if let Poll::Ready(result) = result {
                    match result {
                        Some(item) => {
                            // Add this reader to the back of the queue
                            self.queue.lock().push_back(index);
                            return Poll::Ready(Some(item));
                        }
                        None => {
                            // The reader is closed and so we remove it.
                            self.readers.remove(index);
                        }
                    }
                }
            }
            maybe_index = self.queue.lock().pop_front();
        }

        if self.readers.is_empty() {
            // All readers are done so the multi reader is also done.
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
