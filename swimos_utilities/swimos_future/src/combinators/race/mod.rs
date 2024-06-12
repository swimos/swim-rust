#[cfg(test)]
mod tests;

use either::Either;
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The type returned by the [`race`] function.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct Race2<L, R> {
    left: L,
    right: R,
}

impl<L, R> Unpin for Race2<L, R>
where
    L: Unpin,
    R: Unpin,
{
}

/// Waits for either one of two differently-typed futures to complete and then discards the other
/// future. If both futures are ready then the left future's output is returned.
///
/// This function differs to Future's Select functionality where it discards the remaining future.
///
/// # Cancel safety
/// This function is not cancellation safe. If a future makes progress and another completes first,
/// then the data may be lost in the pending future. If cancellation safety is required, then
/// future's select may be more appropriate.
pub fn race<L, R>(left: L, right: R) -> Race2<L, R>
where
    L: Future + Unpin,
    R: Future + Unpin,
{
    Race2 { left, right }
}

impl<L, R> Future for Race2<L, R>
where
    L: Future + Unpin,
    R: Future + Unpin,
{
    type Output = Either<L::Output, R::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Race2 { left, right } = self.as_mut().get_mut();

        if let Poll::Ready(output) = left.poll_unpin(cx) {
            return Poll::Ready(Either::Left(output));
        }

        if let Poll::Ready(output) = right.poll_unpin(cx) {
            return Poll::Ready(Either::Right(output));
        }

        Poll::Pending
    }
}
