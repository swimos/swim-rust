use crate::mailbox::core::WriteTask;
use crate::mailbox::fan_in::{RawChannelSender, SharedInner};
use bytes::BytesMut;
use futures::task::AtomicWaker;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_util::codec::Encoder;

pin_project! {
    pub struct Send<'r,  I> {
        tx: &'r RawChannelSender,
        item: I,
        state: SendState,
        write_idx: usize
    }
}

enum SendState {
    None,
    Waiting(WriteTask),
    Writing(WriteTask),
}

impl<'r, I> Send<'r, I> {
    pub fn new(tx: &'r RawChannelSender, item: I) -> Send<'r, I> {
        Send {
            tx,
            item,
            state: SendState::None,
            write_idx: 0,
        }
    }
}

impl<'r, I> Future for Send<'r, I>
where
    I: AsRef<[u8]>,
{
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        let item = this.item.as_ref();
        let write_idx = this.write_idx;
        let SharedInner {
            capacity,
            mailbox,
            write_queue,
            receiver,
            closed,
            ..
        } = this.tx.shared.as_ref();

        loop {
            if closed.load(Ordering::Relaxed) {
                return Poll::Ready(Err(()));
            }

            match &this.state {
                SendState::None => {
                    let task = WriteTask::new(cx.waker());
                    let wake = !write_queue.has_next();

                    write_queue.push(task.clone());
                    *this.state = SendState::Waiting(task);

                    if wake {
                        // We only want to wake the receiver if there is not another entry in the
                        // queue as nothing will happen to the task until it has reached the head.
                        receiver.wake();
                    }

                    break Poll::Pending;
                }
                SendState::Waiting(task) => {
                    // guard against spurious wake ups
                    if task.is_pending() {
                        task.register(cx.waker());
                        return Poll::Pending;
                    }

                    *this.state = SendState::Writing(task.clone());
                }
                SendState::Writing(task) => {
                    let data = &mut *mailbox.lock();
                    let available = *capacity - data.len();

                    if available == 0 {
                        task.register(cx.waker());
                        receiver.wake();

                        break Poll::Pending;
                    } else {
                        let len = item.len().min(available);
                        data.extend_from_slice(&item[*write_idx..len]);
                        *write_idx += len;

                        if *write_idx == item.len() {
                            task.complete();
                            break Poll::Ready(Ok(()));
                        } else {
                            receiver.wake();
                            break Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}
