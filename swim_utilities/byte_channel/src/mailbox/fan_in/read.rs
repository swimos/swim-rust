use crate::io::mailbox::fan_in::{ChannelReceiver, SharedInner};
use pin_project_lite::pin_project;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::codec::Decoder;

pin_project! {
    pub struct Read<'r, C> {
        rx: &'r ChannelReceiver<C>,
    }
}

impl<'r, C> Read<'r, C> {
    pub fn new(rx: &'r ChannelReceiver<C>) -> Read<'r, C> {
        Read { rx }
    }
}

impl<'r, C, I> Future for Read<'r, C>
where
    C: Decoder<Item = I>,
{
    type Output = Result<I, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let SharedInner {
            capacity,
            mailbox,
            current,
            write_queue,
            receiver,
        } = &this.rx.shared.as_ref();

        match unsafe { &*current.get() } {
            Some(task) => {
                unimplemented!()
            }
            None => match write_queue.pop() {
                Some(task) => {
                    task.wake();
                    unsafe {
                        *current.get() = Some(task);
                    }
                    unimplemented!()
                }
                None => Poll::Pending,
            },
        }
    }
}
