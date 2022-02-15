use futures::task::AtomicWaker;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::Waker;
use tokio_util::codec::{Decoder, Encoder};

pub trait AbstractWaker {
    fn wake(self);
}

impl AbstractWaker for Waker {
    fn wake(self) {
        Waker::wake(self)
    }
}

pub trait Codec<I>: Encoder<I> + Decoder<Item = I> {}

#[derive(Clone, Debug)]
pub struct WriteTask {
    waker: Arc<AtomicWaker>,
    is_pending: Arc<AtomicBool>,
    is_complete: Arc<AtomicBool>,
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
        }
    }

    pub fn wake(&mut self) {
        let WriteTask {
            waker,
            is_pending,
            is_complete,
        } = self;

        is_pending.store(false, Ordering::Relaxed);
        waker.wake();
    }

    pub fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    pub fn is_pending(&self) -> bool {
        self.is_pending.load(Ordering::Relaxed)
    }

    pub fn is_complete(&self) -> bool {
        self.is_complete.load(Ordering::Relaxed)
    }

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

pub struct Queue<I> {
    head: AtomicPtr<Node<I>>,
    tail: UnsafeCell<*mut Node<I>>,
}

impl<I> Queue<I> {
    pub fn new(head: AtomicPtr<Node<I>>, tail: UnsafeCell<*mut Node<I>>) -> Queue<I> {
        Queue { head, tail }
    }

    pub fn push(&self, item: I) {
        unsafe {
            let n = Node::new(Some(item));
            let prev = self.head.swap(n, Ordering::AcqRel);
            (*prev).next.store(n, Ordering::Release);
        }
    }

    pub fn pop(&self) -> Option<I> {
        unsafe {
            let tail = *self.tail.get();
            let next = (*tail).next.load(Ordering::Acquire);

            if !next.is_null() {
                *self.tail.get() = next;

                assert!((*tail).value.is_none());
                assert!((*next).value.is_some());

                let ret = (*next).value.take().unwrap();
                drop(Box::from_raw(tail));

                Some(ret)
            } else {
                None
            }
        }
    }

    pub fn has_next(&self) -> bool {
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
