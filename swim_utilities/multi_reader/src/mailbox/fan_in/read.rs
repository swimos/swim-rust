use crate::io::mailbox::fan_in::{ChannelReceiver, SharedInner};
use bytes::BufMut;
use pin_project_lite::pin_project;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::codec::Decoder;

pin_project! {
    pub struct Read<'r> {
        rx: &'r mut ChannelReceiver,
        into: &'r mut [u8]
    }
}

impl<'r> Read<'r> {
    pub fn new(rx: &'r mut ChannelReceiver, into: &'r mut [u8]) -> Read<'r> {
        Read { rx, into }
    }
}

impl<'r> Future for Read<'r> {
    type Output = Result<I, ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let into = this.into;
        let decoder = &mut this.rx.decoder;
        let SharedInner {
            capacity,
            mailbox,
            current,
            write_queue,
            receiver,
            closed,
        } = &this.rx.shared.as_ref();

        match unsafe { &*current.get() } {
            Some(task) => {
                //println!("Read::some");

                if task.is_complete() {
                    // control of the buffer has been yielded back to us
                    let buf = unsafe { (&mut *mailbox.get()) };

                    let r = decoder.decode(buf).unwrap().unwrap();

                    unsafe {
                        *current.get() = None;
                    }

                    Poll::Ready(Ok(r))
                } else {
                    task.register(cx.waker());
                    Poll::Pending
                }
            }
            None => match write_queue.pop() {
                Some(mut task) => {
                    task.wake();

                    //println!("Read::write queue some");
                    task.register(cx.waker());

                    unsafe {
                        *current.get() = Some(task);
                    }

                    Poll::Pending
                }
                None => {
                    receiver.register(cx.waker());
                    //println!("Read::write queue none");

                    Poll::Pending
                }
            },
        }
    }
}
