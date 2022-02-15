use crate::io::mailbox::core::WriteTask;
use crate::io::mailbox::fan_in::{RawChannelSender, SharedInner};
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
        write_idx:usize
    }
}

enum SendState {
    None,
    AcquiringPermit(WriteTask),
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
            current,
            write_queue,
            receiver,
            closed,
        } = this.tx.shared.as_ref();

        loop {
            match &this.state {
                SendState::None => {
                    let task = WriteTask::new(cx.waker());

                    write_queue.push(task.clone());
                    *this.state = SendState::AcquiringPermit(task);
                    receiver.wake();

                    //println!("SendState::None: Acquiring permit");

                    break Poll::Pending;
                }
                SendState::AcquiringPermit(state) => {
                    //println!("SendState::AcquiringPermit: enter");

                    if state.is_pending() {
                        //println!("SendState::AcquiringPermit: is_pending");

                        state.register(cx.waker());
                        return Poll::Pending;
                    }

                    *this.state = SendState::Writing(state.clone());
                }
                SendState::Writing(state) => {
                    let data = &mut *mailbox.lock();
                    let available = *capacity - data.len();

                    if available == 0 {
                        state.register(cx.waker());
                        break Poll::Pending;
                    } else {
                        let len = item.len().min(available);
                        data.extend_from_slice(&item[*write_idx..len]);
                        *write_idx += len;

                        if *write_idx == item.len() {
                            state.complete();
                            break Poll::Ready(Ok(()));
                        } else {
                            break Poll::Pending;
                        }
                    }

                    //println!("SendState::Writing: complete");
                }
            }
        }
    }
}
