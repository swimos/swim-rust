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

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct Node<T> {
    next: AtomicPtr<Self>,
    value: Option<T>,
}

impl<T> Node<T> {
    pub fn new(v: Option<T>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            next: AtomicPtr::new(ptr::null_mut()),
            value: v,
        }))
    }
}

#[derive(Debug, PartialEq)]
pub enum QueueResult<T> {
    Data(T),
    Empty,
    Inconsistent,
}

impl<T> QueueResult<T> {
    pub fn is_some(&self) -> bool {
        matches!(self, QueueResult::Data(_))
    }

    pub fn is_none(&self) -> bool {
        matches!(self, QueueResult::Empty)
    }
}

#[derive(Debug, Clone)]
pub struct QueueProducer<I> {
    inner: Arc<Queue<I>>,
}

impl<I> QueueProducer<I> {
    #[inline]
    pub fn push(&self, item: I) -> Result<(), I> {
        self.inner.push(item)
    }

    #[inline]
    pub fn has_next(&self) -> bool {
        self.inner.has_next()
    }
}

#[derive(Debug)]
pub struct QueueConsumer<I> {
    inner: Arc<Queue<I>>,
}

impl<I> QueueConsumer<I> {
    #[inline]
    pub fn head_ref(&self) -> QueueResult<&I> {
        self.inner.head_ref()
    }

    #[inline]
    pub fn pop(&self) -> QueueResult<I> {
        self.inner.pop()
    }

    #[inline]
    pub fn has_next(&self) -> bool {
        self.inner.has_next()
    }
}

pub fn queue<I>(capacity: usize) -> (QueueProducer<I>, QueueConsumer<I>) {
    let inner = Arc::new(Queue::new(capacity));
    (
        QueueProducer {
            inner: inner.clone(),
        },
        QueueConsumer { inner },
    )
}

// Based on std::sync::mpsc::mpsc_queue::SyncQueue
//
// Something else may be more suitable here
#[derive(Debug)]
struct Queue<I> {
    capacity: usize,
    len: AtomicUsize,
    head: AtomicPtr<Node<I>>,
    tail: UnsafeCell<*mut Node<I>>,
}

impl<I> Queue<I> {
    fn new(capacity: usize) -> Queue<I> {
        let empty = Node::new(None);
        Queue {
            capacity,
            len: AtomicUsize::new(0),
            head: AtomicPtr::new(empty),
            tail: UnsafeCell::new(empty),
        }
    }

    #[inline]
    fn push(&self, item: I) -> Result<(), I> {
        if self.len.load(Ordering::Relaxed) + 1 > self.capacity {
            return Err(item);
        }

        unsafe {
            let n = Node::new(Some(item));
            let prev = self.head.swap(n, Ordering::AcqRel);
            (*prev).next.store(n, Ordering::Release);
        }

        Ok(())
    }

    #[inline]
    fn head_ref(&self) -> QueueResult<&I> {
        unsafe {
            let tail = *self.tail.get();
            let next = (*tail).next.load(Ordering::Acquire);

            if !next.is_null() {
                QueueResult::Data((*next).value.as_ref().unwrap())
            } else {
                if self.head.load(Ordering::Acquire) == tail {
                    QueueResult::Empty
                } else {
                    QueueResult::Inconsistent
                }
            }
        }
    }

    #[inline]
    fn pop(&self) -> QueueResult<I> {
        unsafe {
            let tail = *self.tail.get();
            let next = (*tail).next.load(Ordering::Acquire);

            if !next.is_null() {
                *self.tail.get() = next;

                assert!((*tail).value.is_none());
                assert!((*next).value.is_some());

                let ret = (*next).value.take().unwrap();
                drop(Box::from_raw(tail));

                QueueResult::Data(ret)
            } else {
                if self.head.load(Ordering::Acquire) == tail {
                    QueueResult::Empty
                } else {
                    QueueResult::Inconsistent
                }
            }
        }
    }

    #[inline]
    fn has_next(&self) -> bool {
        unsafe {
            let tail = *self.tail.get();
            let next = (*tail).next.load(Ordering::Acquire);

            !next.is_null()
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let mut cur = *self.tail.get();
            while !cur.is_null() {
                let next = (*cur).next.load(Ordering::Relaxed);
                drop(Box::from_raw(cur));
                cur = next;
            }
        }
    }
}
