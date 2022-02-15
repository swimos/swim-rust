use futures::task::AtomicWaker;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Waker;
use tokio_util::codec::{Decoder, Encoder};

const STATE_COMPLETE: usize = 0;
const STATE_PENDING: usize = 1;

#[derive(Clone, Debug)]
pub struct WriteTask {
    waker: Arc<AtomicWaker>,
    is_pending: Arc<AtomicBool>,
    is_complete: Arc<AtomicBool>,
    state: Arc<AtomicUsize>,
}

#[derive(Copy, Clone, Debug)]
enum WriteTaskState {
    Pending,
    Head,
    Complete,
}

impl WriteTask {
    pub fn new(waker: &Waker) -> WriteTask {
        let shared_waker = AtomicWaker::new();
        shared_waker.register(waker);
        WriteTask {
            waker: Arc::new(shared_waker),
            is_pending: Arc::new(AtomicBool::new(false)),
            is_complete: Arc::new(AtomicBool::new(false)),
            state: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn wake(&self) {
        let WriteTask {
            waker, is_pending, ..
        } = self;

        is_pending.store(false, Ordering::Relaxed);
        waker.wake();
    }

    #[inline]
    pub fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    #[inline]
    pub fn is_pending(&self) -> bool {
        self.is_pending.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.is_complete.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn complete(&self) {
        self.is_complete.store(true, Ordering::Relaxed);
        self.waker.wake();
    }
}

pub struct QueueEntry {
    task: Waker,
    is_parked: bool,
}

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
    pub fn push(&self, item: I) {
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

pub fn queue<I>() -> (QueueProducer<I>, QueueConsumer<I>) {
    let inner = Arc::new(Queue::default());
    (
        QueueProducer {
            inner: inner.clone(),
        },
        QueueConsumer { inner },
    )
}

// Based on std::sync::mpsc::mpsc_queue::SyncQueue
#[derive(Debug)]
struct Queue<I> {
    head: AtomicPtr<Node<I>>,
    tail: UnsafeCell<*mut Node<I>>,
}

impl<I> Default for Queue<I> {
    fn default() -> Self {
        let empty = Node::new(None);
        Queue::new(AtomicPtr::new(empty), UnsafeCell::new(empty))
    }
}

impl<I> Queue<I> {
    fn new(head: AtomicPtr<Node<I>>, tail: UnsafeCell<*mut Node<I>>) -> Queue<I> {
        Queue { head, tail }
    }

    #[inline]
    fn push(&self, item: I) {
        unsafe {
            let n = Node::new(Some(item));
            let prev = self.head.swap(n, Ordering::AcqRel);
            (*prev).next.store(n, Ordering::Release);
        }
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
