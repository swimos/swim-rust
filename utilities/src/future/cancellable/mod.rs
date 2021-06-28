use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[pin_project]
pub struct Cancellable<F, C> {
    #[pin]
    future: F,
    #[pin]
    cancel: C,
}

impl<F: Future, C: Future> Cancellable<F, C> {
    pub fn new(future: F, cancel: C) -> Cancellable<F, C> {
        Cancellable { future, cancel }
    }
}

impl<F: Future, C: Future> Future for Cancellable<F, C> {
    type Output = CancellableResult<F::Output, C::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.poll(cx) {
            Poll::Ready(result) => Poll::Ready(CancellableResult::Completed(result)),
            Poll::Pending => match this.cancel.poll(cx) {
                Poll::Ready(result) => Poll::Ready(CancellableResult::Cancelled(result)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CancellableResult<T, S> {
    Completed(T),
    Cancelled(S),
}
