use futures::task::{Context, Poll};
use futures::Future;

// Copyright 2015-2020 SWIM.AI inc.
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
use pin_project::{pin_project, project};

use crate::future::retryable::ResettableFuture;
use std::pin::Pin;

#[pin_project]
pub struct ResetabbleFutureFactory<F, O, E>
where
    F: FutureFactory<O, E>,
{
    factory: F,
    #[pin]
    current: F::Future,
}

impl<F, O, E> ResetabbleFutureFactory<F, O, E>
where
    F: FutureFactory<O, E>,
{
    pub fn wrap(mut factory: F) -> Self {
        let current = factory.future();

        ResetabbleFutureFactory { factory, current }
    }
}

pub trait FutureFactory<Ok, Err> {
    type Future: Future<Output = Result<Ok, Err>> + 'static;

    fn future(&mut self) -> Self::Future;
}

impl<F, O, E> ResettableFuture for ResetabbleFutureFactory<F, O, E>
where
    F: FutureFactory<O, E>,
{
    #[project]
    fn reset(mut self: Pin<&mut Self>) -> bool {
        let mut this = self.as_mut().project();
        let future = this.factory.future();
        this.current.set(future);

        true
    }
}

impl<F, O, E> Future for ResetabbleFutureFactory<F, O, E>
where
    F: FutureFactory<O, E>,
{
    type Output = Result<O, E>;

    #[project]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        this.current.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use futures::task::{Context, Poll};
    use futures::Future;

    use crate::future::retryable::factory::{FutureFactory, ResetabbleFutureFactory};
    use crate::future::retryable::RetryableFuture;
    use std::pin::Pin;

    struct ReadyFactory {}

    impl ReadyFactory {
        fn new() -> Self {
            ReadyFactory {}
        }
    }

    #[derive(Debug)]
    enum FutErr {}

    struct TestFuture {}

    impl Future for TestFuture {
        type Output = Result<(), FutErr>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(()))
        }
    }

    impl FutureFactory<(), FutErr> for ReadyFactory {
        type Future = TestFuture;

        fn future(&mut self) -> Self::Future {
            TestFuture {}
        }
    }

    #[tokio::test]
    async fn factory() {
        let factory = ReadyFactory::new();
        let wrapper = ResetabbleFutureFactory::wrap(factory);

        let retryable = RetryableFuture::new(wrapper, Default::default());
        let r = retryable.await;
        let r = r.unwrap();
        println!("{:?}", r);
    }
}

#[cfg(test)]
mod tokio {
    use crate::future::retryable::factory::{FutureFactory, ResetabbleFutureFactory};

    use crate::future::retryable::RetryableFuture;
    use futures::task::{Context, Poll};
    use futures::Future;
    use std::pin::Pin;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TrySendError;

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    enum SendErr {
        Err,
    }

    struct TestSender<P>
    where
        P: Clone,
    {
        tx: mpsc::Sender<P>,
        payload: P,
    }

    struct SendFuture<P>
    where
        P: Clone,
    {
        tx: mpsc::Sender<P>,
        payload: P,
    }

    impl<P> Future for SendFuture<P>
    where
        P: Clone + Unpin,
    {
        type Output = Result<P, SendErr>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let SendFuture { tx, payload } = self.get_mut();

            match tx.poll_ready(cx) {
                Poll::Ready(Ok(_)) => match tx.try_send(payload.clone()) {
                    Ok(_) => Poll::Ready(Ok(payload.clone())),
                    Err(TrySendError::Closed(_)) => Poll::Ready(Err(SendErr::Err)),
                    Err(TrySendError::Full(_)) => unreachable!(),
                },

                Poll::Ready(Err(_)) => Poll::Ready(Err(SendErr::Err)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<P> FutureFactory<P, SendErr> for TestSender<P>
    where
        P: Clone + Send + Unpin + 'static,
    {
        type Future = SendFuture<P>;

        fn future(&mut self) -> Self::Future {
            SendFuture {
                tx: self.tx.clone(),
                payload: self.payload.clone(),
            }
        }
    }

    #[tokio::test]
    async fn test_send() {
        let payload = 5;
        let (tx, mut rx) = mpsc::channel(1);
        let wrapper = ResetabbleFutureFactory::wrap(TestSender { tx, payload });
        let retry = RetryableFuture::new(wrapper, Default::default()).await;
        let result = rx.recv().await;

        assert_eq!(payload, retry.unwrap());
        assert_eq!(payload, result.unwrap())
    }
}
