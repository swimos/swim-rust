use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use tokio_util::time::delay_queue::Key;
use tokio_util::time::DelayQueue;

#[cfg(test)]
mod tests;

/// Definition of a schedule to yield an item in an interval stream.
#[derive(Debug, Serialize, Deserialize)]
pub enum ScheduleDef {
    /// Yield an item once and only once.
    Once {
        /// The duration which will elapse before the item is yielded.
        after: Duration,
    },
    /// Yield an item forever with a fixed interval between the yields.
    Infinite {
        /// The duration which will elapse before the item is yielded.
        interval: Duration,
    },
    /// Yield an item a fixed number of times with a fixed interval between the yields.
    Interval {
        /// The number of times the item will be yielded.
        count: usize,
        /// The duration which will elapse before the item is yielded.
        interval: Duration,
    },
}

#[derive(Debug, Copy, Clone)]
enum Schedule {
    Infinite {
        interval: Duration,
    },
    Interval {
        remaining: usize,
        interval: Duration,
    },
}

impl Schedule {
    fn interval(&self) -> Duration {
        match self {
            Schedule::Infinite { interval } => *interval,
            Schedule::Interval { interval, .. } => *interval,
        }
    }

    fn next(self) -> Option<Schedule> {
        match self {
            s @ Schedule::Infinite { .. } => Some(s),
            Schedule::Interval {
                remaining,
                interval,
            } => remaining
                .checked_sub(1)
                .map(|remaining| Schedule::Interval {
                    remaining,
                    interval,
                }),
        }
    }
}

impl From<ScheduleDef> for Schedule {
    fn from(value: ScheduleDef) -> Schedule {
        match value {
            ScheduleDef::Once { after } => Schedule::Interval {
                remaining: 0,
                interval: after,
            },
            ScheduleDef::Infinite { interval } => Schedule::Infinite { interval },
            ScheduleDef::Interval { count, interval } => Schedule::Interval {
                // Decremented by 1 for the first run.
                // unwrap_or_default in case 'count' was already 0.
                remaining: count.checked_sub(1).unwrap_or_default(),
                interval,
            },
        }
    }
}

struct Cell<T> {
    item: T,
    strategy: Schedule,
}

impl<T> Cell<T> {
    fn new(item: T, strategy: Schedule) -> Cell<T> {
        Cell { item, strategy }
    }
}

impl<T> Debug for Cell<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cell")
            .field(&"item", &self.item)
            .field(&"strategy", &self.strategy)
            .finish()
    }
}

/// A stream which will yield elements at predefined intervals.
#[pin_project]
pub struct IntervalStream<T> {
    #[pin]
    queue: DelayQueue<Cell<T>>,
}

impl<T> IntervalStream<T> {
    pub fn new() -> IntervalStream<T> {
        IntervalStream {
            queue: DelayQueue::default(),
        }
    }

    /// Pushes a new item into the stream which will be yielded according to the provided schedule.
    ///
    /// # Returns
    /// A key which may be used to remove the item from the stream. This is only valid until the
    /// next call to 'poll_next' has been made if this item was yielded. If this item was yielded,
    /// then this key has been invalidated and a new one may have been returned.
    pub fn push(&mut self, def: ScheduleDef, item: T) -> Key {
        let strategy: Schedule = def.into();
        let interval = strategy.interval();
        self.queue.insert(Cell::new(item, strategy), interval)
    }

    /// Removes the item associated with the provided key and returns its value.
    ///
    /// # Panics
    /// Panics if `key` is not contained in this interval stream.
    pub fn remove(&mut self, key: &Key) -> T {
        let expired = self.queue.remove(key);
        expired.into_inner().item
    }

    /// Attempts to remove the item associated with the provided key.
    pub fn try_remove(&mut self, key: &Key) -> Option<T> {
        let expired = self.queue.try_remove(key);
        expired.map(|exp| exp.into_inner().item)
    }

    /// Returns true if this interval stream is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl<T> Debug for IntervalStream<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntervalStream")
            .field(&"queue", &self.queue)
            .finish()
    }
}

impl<T> Default for IntervalStream<T> {
    fn default() -> Self {
        IntervalStream::new()
    }
}

impl<T> Stream for IntervalStream<T>
where
    T: Clone,
{
    type Item = StreamItem<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.as_mut().project();
        let mut queue: Pin<&mut DelayQueue<Cell<T>>> = projected.queue;

        match ready!(queue.as_mut().poll_expired(cx)) {
            Some(expired) => {
                let Cell { item, strategy } = expired.into_inner();
                let stream_item = match strategy.next() {
                    Some(next) => {
                        let interval = next.interval();
                        let key = queue
                            .as_mut()
                            .insert(Cell::new(item.clone(), next), interval);
                        StreamItem {
                            item,
                            status: ItemStatus::WillYield { key },
                        }
                    }
                    None => StreamItem {
                        item,
                        status: ItemStatus::Complete,
                    },
                };
                Poll::Ready(Some(stream_item))
            }
            None => Poll::Ready(None),
        }
    }
}

/// An item which has been yielded by the stream.
pub struct StreamItem<T> {
    /// The item which was yielded.
    pub item: T,
    /// The status of the item in the stream.
    pub status: ItemStatus,
}

impl<T> StreamItem<T> {
    /// Returns true if this item has completed. I.e, it will not be yielded again.
    pub fn is_complete(&self) -> bool {
        matches!(self.status, ItemStatus::Complete)
    }

    /// Returns true if this item will be yielded again from the stream.
    pub fn is_will_yield(&self) -> bool {
        matches!(self.status, ItemStatus::WillYield { .. })
    }
}

impl<T> Debug for StreamItem<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamItem")
            .field(&"item", &self.item)
            .field(&"status", &self.status)
            .finish()
    }
}

impl<T> Clone for StreamItem<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let StreamItem { item, status } = self;
        StreamItem {
            item: item.clone(),
            status: *status,
        }
    }
}

impl<T> PartialEq for StreamItem<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        let StreamItem { item, status } = self;
        status.eq(&other.status) && item.eq(&other.item)
    }
}

impl<T> Copy for StreamItem<T> where T: Copy {}

impl<T> Eq for StreamItem<T> where T: Eq {}

/// The status of an item in an interval stream.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ItemStatus {
    /// The item will not be yielded again.
    Complete,
    /// The item will be yielded again in the future.
    WillYield {
        /// The item's transient key. This is *only* valid until the stream is polled again.
        key: Key,
    },
}
